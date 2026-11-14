//! harness-daemon: process entry, single-instance lock, transport wiring.
//!
//! The daemon is a thin layer: it loads the config, lets the builder assemble
//! the runtime, hands runs to the scheduler, and serves the three transport
//! channels (UDS control plane, HTTP+SSE, WebSocket) over one shared
//! [`TransportState`]. Scheduling, the loop, tools and compaction all live in
//! their own crates; this process only assembles, starts, supervises and ends.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use harness_builder::{
    AssembledRuntime, BUILTIN_TOOLS, HarnessBuilder, HarnessSpec, ModelSpec, ToolSpec,
};
use harness_config::{Config, SubagentProfile};
use harness_core::model::ModelInfo;
use harness_core::tool::Tool;
use harness_core::{EventSink, Model, RunEvent};
use harness_model::OpenAIModel;
use harness_scheduler::Scheduler;
use harness_session::{RecallStore, SessionStore, turso::TursoSessionStore};
use harness_transport::http;
use harness_transport::ws;
use harness_transport::{EventLog, TransportState, uds::UdsConfig};

use harness_daemon::pidlock::PidLock;
use harness_daemon::subagent::SubagentTool;
use harness_daemon::tools::ToolCatalog;
use harness_daemon::wire::{DaemonHandler, StoreEventLog};

/// Default UDS socket path for the v1 daemon.
const DEFAULT_SOCKET: &str = "/tmp/harness-daemon.sock";
/// Default HTTP+SSE / WS listen address.
const DEFAULT_HTTP: &str = "127.0.0.1:8718";
/// Context window assumed for models with no metadata; compaction
/// calibrates against it.
const DEFAULT_CONTEXT_WINDOW: u32 = 128_000;

/// The headless harness daemon: assemble a runtime from config, hand runs to
/// the scheduler, and serve UDS / HTTP+SSE / WebSocket clients.
#[derive(Parser)]
#[command(name = "harness-daemon", version)]
struct Cli {
    /// Config TOML path. Default: $HARNESS_CONFIG, then the XDG config dir
    /// ($XDG_CONFIG_HOME/harness/config.toml, falling back to
    /// $HOME/.config/harness/config.toml).
    #[arg(long, value_name = "PATH")]
    config: Option<PathBuf>,
    /// UDS socket path; the single-instance pid lock sits next to it as
    /// `<socket>.lock`.
    #[arg(long, default_value = DEFAULT_SOCKET, value_name = "PATH")]
    socket: PathBuf,
    /// HTTP+SSE / WebSocket listen address.
    #[arg(long, default_value = DEFAULT_HTTP, value_name = "ADDR")]
    http: String,
}

/// Build the model adapter from the config's first alias (first-wins) and the
/// upstream address. The API key lives in an env variable whose *name* the
/// config stores — the key value never touches the config file.
fn build_model(config: &Config) -> anyhow::Result<OpenAIModel> {
    let alias = config.models.aliases.first().ok_or_else(|| {
        anyhow::anyhow!("no models configured: add at least one [[models.aliases]] entry")
    })?;
    let api_key = std::env::var(&config.upstream.api_key_var).with_context(|| {
        format!(
            "environment variable {} is not set (the upstream API key)",
            config.upstream.api_key_var
        )
    })?;
    let info = ModelInfo {
        handle: alias.name.clone(),
        provider: "openai".into(),
        model: alias.model_id.clone(),
        context_window: DEFAULT_CONTEXT_WINDOW,
        supports_tool_use: true,
        supports_streaming: true,
    };
    Ok(OpenAIModel::new(
        info,
        config.upstream.base_url.clone(),
        api_key,
    ))
}

/// Assemble the runtime: a spec over the five builtin tools, the injected
/// model, and the config-mapped bindings (limits, aliases, MCP servers).
fn build_runtime(
    config: &Config,
    model: Arc<dyn Model>,
    event_sink: Arc<dyn EventSink>,
    subagent_tool: Option<Arc<dyn Tool>>,
) -> anyhow::Result<AssembledRuntime> {
    // The model instance is injected by the host; the spec declares the same
    // model in its directory so the builder can validate the injection.
    let alias = config.models.aliases.first().ok_or_else(|| {
        anyhow::anyhow!("no models configured: add at least one [[models.aliases]] entry")
    })?;
    let mut spec = HarnessSpec::new("daemon-default");
    spec.default_model = Some(alias.name.clone());
    spec.models = vec![ModelSpec {
        handle: alias.name.clone(),
        provider: "openai".into(),
        model: alias.model_id.clone(),
        context_window: DEFAULT_CONTEXT_WINDOW,
        supports_tool_use: true,
        supports_streaming: true,
    }];
    spec.tools = BUILTIN_TOOLS
        .iter()
        .map(|name| ToolSpec::builtin(*name))
        .collect();
    // The `subagent` tool is declared as external so the builder validates
    // the injection and installs it into the base tool set; per-session
    // allowlists gate it like any other tool.
    spec.tools.extend(
        subagent_tool
            .as_ref()
            .map(|_| ToolSpec::external("subagent")),
    );
    let cwd =
        std::env::current_dir().context("resolving the current directory for tool execution")?;
    let mut builder = HarnessBuilder::new(spec)
        .with_model(Arc::clone(&model))
        .with_policy(Arc::new(harness_daemon::wire::DaemonPolicy))
        .with_event_sink(event_sink)
        .with_cwd(cwd);
    builder = builder
        .with_config(config)
        .context("mapping the config onto builder bindings")?;
    if let Some(tool) = subagent_tool {
        builder = builder.with_tool(tool);
    }
    builder.build().context("assembling the runtime")
}

/// A transient [`EventSink`] that forwards into `event_tx`: used while
/// building the runtime, before [`TransportState`] exists. The runtime is
/// assembled before the handler so its tool set can snapshot into the
/// catalog; both sinks refer to the same channel, so no events are lost.
struct EarlyEventSink(std::sync::Arc<tokio::sync::broadcast::Sender<RunEvent>>);

impl EventSink for EarlyEventSink {
    fn emit(&self, event: RunEvent) {
        // try_send drops the event if the channel is full — subscriber lag
        // is observable but never blocks the loop (see EventSink docs).
        let _ = self.0.send(event);
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("daemon: {err:#}");
            ExitCode::FAILURE
        }
    }
}

/// Expand a leading `~`/`~/` to `$HOME` at the consumption point.  The config
/// doc promises load-time expansion; until the loader performs it, the daemon
/// expands here so `session.directory = "~/..."` resolves.  Returns `path`
/// unchanged when there is no leading `~` or `$HOME` is unset.
fn expand_tilde(path: &str) -> String {
    match path.strip_prefix("~/") {
        // "~/rest" expands to $HOME/rest; a bare "~" expands to $HOME itself,
        // falling back to a literal "~" when HOME is unset.
        Some(rest) => format!("{}/{}", home_dir(), rest),
        None if path == "~" => home_dir(),
        None => path.to_string(),
    }
}

/// `$HOME`, or a literal "~" when the variable is unset.
fn home_dir() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "~".into())
}

/// Read the UDS bearer token from the env variable named by the config.  The
/// config stores the variable name only; an unset or empty variable means
/// token auth is disabled.
fn uds_auth_token(config: &Config) -> Option<String> {
    config
        .session
        .auth_token_var
        .as_ref()
        .and_then(|var| std::env::var(var).ok())
        .filter(|token| !token.is_empty())
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let lock_path = PathBuf::from(format!("{}.lock", cli.socket.display()));
    let lock = PidLock::acquire(&lock_path)?;
    let mut config = match &cli.config {
        Some(path) => harness_config::load_from_path(path)
            .with_context(|| format!("config {}", path.display()))?,
        None => harness_config::load_or_defaults().context("config")?,
    };
    let model: Arc<dyn Model> = Arc::new(build_model(&config)?);
    let scheduler = Scheduler::new(
        config.scheduler.queue_capacity,
        config.scheduler.max_concurrency,
    );
    let session_dir = expand_tilde(&config.session.directory);
    // Ensure the session directory exists so `TursoSessionStore::open` can
    // create the database file; a missing directory is a config error, not a
    // reason to fall back to an in-memory log.
    let has_parent = std::path::Path::new(&session_dir)
        .parent()
        .is_some_and(|parent| !parent.as_os_str().is_empty());
    if has_parent {
        std::fs::create_dir_all(&session_dir)
            .with_context(|| format!("create session directory {session_dir}"))?;
    }
    // Turso (libsql) embedded mode persists to a single local database file
    // beside the session directory — named `.db` (not `.sqlite`) because the
    // engine is libsql, not plain SQLite.
    let store_path = PathBuf::from(&session_dir).join("sessions.db");
    // Build the recall seam from the concrete store so it shares the store's
    // connection and FTS gate, then hand both trait objects to the handler.
    let store_inner = TursoSessionStore::open(&store_path)
        .await
        .with_context(|| format!("open session store at {session_dir}"))?;
    let recall: Arc<dyn RecallStore> = Arc::new(store_inner.recall());
    let store: Arc<dyn SessionStore> = Arc::new(store_inner);
    let event_log: Arc<dyn EventLog> = Arc::new(StoreEventLog::new(store.clone()));
    // The broadcast channel is created up front so the runtime's event sink
    // and the transport state share one `tx`; it also doubles as the event
    // sink for subagent children, whose events fan out to transport clients.
    let (event_tx, _) = tokio::sync::broadcast::channel(1024);
    let event_sink: Arc<dyn EventSink> = Arc::new(EarlyEventSink(Arc::new(event_tx.clone())));

    // The `subagent` tool is registered only when profiles are configured.
    // It is injected before assembly and wired to the catalog right after
    // (the catalog snapshot needs runtime.tools, which needs the tool — the
    // shared sink created above breaks the construction circle).
    let subagent_profiles = std::mem::take(&mut config.agents.subagents);
    let mut profiles: HashMap<String, SubagentProfile> = HashMap::new();
    for profile in subagent_profiles {
        let name = profile.name.clone();
        if profiles.insert(name.clone(), profile).is_some() {
            // A silently overwritten profile is a config mistake the
            // operator must hear about at startup.
            return Err(anyhow::anyhow!(
                "duplicate subagent profile name in [agents]: {name}"
            ));
        }
    }
    let subagent_tool = (!profiles.is_empty()).then(|| {
        Arc::new(SubagentTool::new(
            Arc::clone(&model),
            profiles,
            Arc::clone(&event_sink),
            Arc::new(harness_daemon::wire::DaemonPolicy),
        ))
    });
    let injected: Option<Arc<dyn Tool>> = subagent_tool
        .as_ref()
        .map(|tool| Arc::clone(tool) as Arc<dyn Tool>);

    // The runtime is assembled before the handler: its tool set is the
    // catalog's one-time base snapshot, registered once at startup.
    let runtime = build_runtime(
        &config,
        Arc::clone(&model),
        Arc::clone(&event_sink),
        injected,
    )?;
    let catalog = Arc::new(ToolCatalog::new(runtime.tools.clone()));
    if let Some(tool) = &subagent_tool {
        tool.set_catalog(Arc::clone(&catalog));
    }
    let handler = Arc::new(DaemonHandler::new(
        scheduler.clone(),
        store,
        recall,
        catalog,
        subagent_tool.clone(),
    ));
    let state = Arc::new(TransportState::with_event_tx(
        handler.clone(),
        event_log,
        event_tx,
    ));
    handler.set_event_tx(state.event_tx.clone());
    handler.set_runtime(runtime);

    let app = http::http_routes(state.clone()).merge(ws::ws_routes(state.clone()));
    let tcp = tokio::net::TcpListener::bind(&cli.http)
        .await
        .with_context(|| format!("http bind {}", cli.http))?;
    let http_handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(tcp, app).await {
            eprintln!("daemon: http server failed: {err}");
        }
    });
    let uds_state = state;
    let uds_config = UdsConfig {
        socket_path: cli.socket.display().to_string(),
        // The token value is read at startup from the env variable named by
        // the config (session.auth_token_var stores the name, never the
        // value); an unset or empty variable disables token auth.
        auth_token: uds_auth_token(&config),
    };
    let uds_handle = tokio::spawn(async move {
        if let Err(err) = harness_transport::uds::serve_uds(uds_config, uds_state).await {
            eprintln!("daemon: uds server failed: {err}");
        }
    });
    eprintln!(
        "daemon: listening on uds {} and http {}",
        cli.socket.display(),
        cli.http
    );

    harness_daemon::lifecycle::wait_for_shutdown_signal().await;
    eprintln!("daemon: shutdown signal received");
    // Stop accepting new connections before draining in-flight runs.
    http_handle.abort();
    uds_handle.abort();
    let _ = http_handle.await;
    let _ = uds_handle.await;
    harness_daemon::lifecycle::drain(&handler, &cli.socket).await;
    drop(lock);
    Ok(())
}
