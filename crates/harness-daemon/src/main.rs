//! harness-daemon: process entry, single-instance lock, transport wiring.
//!
//! The daemon is a thin layer: it loads the config, lets the builder assemble
//! the runtime, hands runs to the scheduler, and serves the three transport
//! channels (UDS control plane, HTTP+SSE, WebSocket) over one shared
//! [`TransportState`]. Scheduling, the loop, tools and compaction all live in
//! their own crates; this process only assembles, starts, supervises and ends.

mod lifecycle;
mod pidlock;
mod wire;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use harness_builder::{
    AssembledRuntime, BUILTIN_TOOLS, HarnessBuilder, HarnessSpec, ModelSpec, ToolKind, ToolSpec,
};
use harness_config::Config;
use harness_core::EventSink;
use harness_core::model::ModelInfo;
use harness_model::OpenAIModel;
use harness_scheduler::Scheduler;
use harness_transport::uds::UdsConfig;
use harness_transport::{TransportState, http, ws};

use pidlock::PidLock;
use wire::{DaemonHandler, InMemoryEventLog};

/// Default UDS socket path for the v1 daemon.
const DEFAULT_SOCKET: &str = "/tmp/harness-daemon.sock";
/// Default HTTP+SSE / WS listen address.
const DEFAULT_HTTP: &str = "127.0.0.1:8718";
/// Context window assumed for models with no configured metadata; compaction
/// calibrates against it.
const DEFAULT_CONTEXT_WINDOW: u32 = 128_000;
/// Queue capacity behind the single active run. v1 has no config surface for
/// this; a burst beyond four queued runs is an explicit `QueueFull` error.
const QUEUE_CAPACITY: usize = 4;

/// Parsed process arguments.
struct Options {
    config: Option<PathBuf>,
    socket: PathBuf,
    http: String,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Options, String> {
    let mut options = Options {
        config: None,
        socket: PathBuf::from(DEFAULT_SOCKET),
        http: DEFAULT_HTTP.into(),
    };
    let mut it = args;
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" => {
                let value = it.next().ok_or("--config needs a file path")?;
                options.config = Some(PathBuf::from(value));
            }
            "--socket" => {
                let value = it.next().ok_or("--socket needs a file path")?;
                options.socket = PathBuf::from(value);
            }
            "--http" => options.http = it.next().ok_or("--http needs an address")?,
            other => {
                return Err(format!(
                    "unknown argument: {other} (usage: harness-daemon [--config PATH] [--socket PATH] [--http ADDR])"
                ));
            }
        }
    }
    Ok(options)
}

/// Build the model adapter from the config's first alias (first-wins) and the
/// upstream address. The API key lives in an env variable whose *name* the
/// config stores — the key value never touches the config file.
fn build_model(config: &Config) -> Result<OpenAIModel, String> {
    let alias = config
        .models
        .aliases
        .first()
        .ok_or("no models configured: add at least one [[models.aliases]] entry")?;
    let api_key = std::env::var(&config.upstream.api_key_var).map_err(|_| {
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
    model: OpenAIModel,
    event_sink: Arc<dyn EventSink>,
) -> Result<AssembledRuntime, String> {
    // The model instance is injected by the host; the spec declares the same
    // model in its directory so the builder can validate the injection.
    let alias = config
        .models
        .aliases
        .first()
        .ok_or("no models configured: add at least one [[models.aliases]] entry")?;
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
        .map(|name| ToolSpec {
            name: name.into(),
            kind: ToolKind::Builtin,
            server: None,
            remote: None,
            manifest: Default::default(),
        })
        .to_vec();
    let cwd = std::env::current_dir().map_err(|err| format!("current dir: {err}"))?;
    HarnessBuilder::new(spec)
        .with_model(Arc::new(model))
        .with_policy(Arc::new(wire::DaemonPolicy))
        .with_event_sink(event_sink)
        .with_cwd(cwd)
        .with_config(config)
        .map_err(|err| format!("config bindings: {err}"))?
        .build()
        .map_err(|err| format!("assemble runtime: {err}"))
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = std::env::args_os()
        .skip(1)
        .map(|a| a.to_string_lossy().into_owned());
    match run(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("daemon: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: impl Iterator<Item = String>) -> Result<(), String> {
    let options = parse_args(args)?;
    let lock_path = PathBuf::from(format!("{}.lock", options.socket.display()));
    let lock = PidLock::acquire(&lock_path).map_err(|err| err.to_string())?;
    let config = match &options.config {
        Some(path) => harness_config::load_from_path(path)
            .map_err(|err| format!("config {}: {err}", path.display()))?,
        None => harness_config::load_or_defaults().map_err(|err| format!("config: {err}"))?,
    };
    let model = build_model(&config)?;
    let scheduler = Scheduler::new(QUEUE_CAPACITY);
    let log = Arc::new(InMemoryEventLog::new());
    let handler = Arc::new(DaemonHandler::new(scheduler.clone(), log.clone()));
    let state = Arc::new(TransportState::new(handler.clone(), log));
    handler.set_event_tx(state.event_tx.clone());
    // The builder requires an event sink, but the scheduler replaces the run's
    // sink with its bus — the transport channel is fed by the pump in wire.rs.
    let runtime = build_runtime(&config, model, Arc::new(state.event_sink()))?;
    handler.set_runtime(runtime);

    let app = http::http_routes(state.clone()).merge(ws::ws_routes(state.clone()));
    let tcp = tokio::net::TcpListener::bind(&options.http)
        .await
        .map_err(|err| format!("http bind {}: {err}", options.http))?;
    let http_handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(tcp, app).await {
            eprintln!("daemon: http server failed: {err}");
        }
    });
    let uds_state = state;
    let uds_config = UdsConfig {
        socket_path: options.socket.display().to_string(),
        auth_token: None,
    };
    let uds_handle = tokio::spawn(async move {
        if let Err(err) = harness_transport::uds::serve_uds(uds_config, uds_state).await {
            eprintln!("daemon: uds server failed: {err}");
        }
    });
    eprintln!(
        "daemon: listening on uds {} and http {}",
        options.socket.display(),
        options.http
    );

    lifecycle::wait_for_shutdown_signal().await;
    eprintln!("daemon: shutdown signal received");
    // Stop accepting new connections before draining in-flight runs.
    http_handle.abort();
    uds_handle.abort();
    let _ = http_handle.await;
    let _ = uds_handle.await;
    lifecycle::drain(&handler, &options.socket).await;
    drop(lock);
    Ok(())
}
