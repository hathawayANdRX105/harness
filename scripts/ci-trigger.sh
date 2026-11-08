#!/usr/bin/env bash
# ci-trigger：agent 主动派发公开壳仓 CI，并实时盯结果（harness 版）。
#
# 为什么需要它（而不是等轮询）：
#   私有仓零 workflow，测试全在公开壳仓 harness-ci 跑。壳仓 dispatch-poller 每 5 分钟
#   轮询私有仓的 PR/main 做**兜底**，但 agent 提交后不能干等 ≤5 分钟 —— 要即时知道
#   这次改动的 shell-ci 状态。本脚本就是那条即时路径：agent 提交后手动跑一次，
#   立刻派发 ci.yml + `gh run watch` 实时等结论，合并门（shell-ci 绿）当场可见。
#
# 用法（本仓 root 下）：
#   bash scripts/ci-trigger.sh [PR]           # 当前分支的开放 PR（或指定 PR）→ 快关
#   bash scripts/ci-trigger.sh --full [PR]    # 全量（main push 语义；PR 也能强制全量）
#   bash scripts/ci-trigger.sh --main <sha>   # 测一个 main 提交（全量）
#
# 派发契约（与壳仓 ci.yml workflow_dispatch inputs 对齐，别乱改）：
#   sha        = 被测私有仓 commit（PR 头提交 或 main 提交）
#   status_sha = 回写 shell-ci 状态的私有仓 commit（PR = 头提交，让 gh pr checks 看到）
#   pr         = PR 编号（空 = main push / 手动全量）
#   base       = PR base 分支名
#   full       = true = 全量；false = PR 快关（testless 影响分析）
#
# 环境：需要 gh（登录态可写壳仓 harness-ci）+ 本仓 git。
set -uo pipefail

PRIVATE_REPO="hathawayANdRX105/harness"
SHELL_REPO="hathawayANdRX105/harness-ci"
cd "$(git rev-parse --show-toplevel)" || exit 1

# ── 解析参数 ────────────────────────────────────────────────────────────────
FULL="false"
MODE="pr"          # pr | main
PR=""
MAIN_SHA=""
while [ $# -gt 0 ]; do
  case "$1" in
    --full) FULL="true"; MODE="pr"; shift ;;
    --main) MODE="main"; MAIN_SHA="${2:-}"; shift 2 ;;
    -h|--help)
      sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) PR="$1"; shift ;;
  esac
done

# ── 解析目标 sha / pr / base ────────────────────────────────────────────────
if [ "$MODE" = "main" ]; then
  [ -n "$MAIN_SHA" ] || { echo "✗ --main 需要 commit sha"; exit 1; }
  sha="$MAIN_SHA"
  status_sha="$MAIN_SHA"
  pr=""
  base="main"
  FULL="true"
  echo ">>> main 全量：sha=$sha"
else
  if [ -z "$PR" ]; then
    br="$(git rev-parse --abbrev-ref HEAD)"
    PR="$(gh pr list -R "$PRIVATE_REPO" --head "$br" --state open --json number -q '.[0].number' 2>/dev/null)"
  fi
  [ -n "$PR" ] || { echo "✗ 解析不到 PR（参数缺省时用当前分支的开放 PR）"; exit 1; }
  sha="$(gh pr view "$PR" -R "$PRIVATE_REPO" --json headRefOid -q .headRefOid)"
  base="$(gh pr view "$PR" -R "$PRIVATE_REPO" --json baseRefName -q .baseRefName)"
  status_sha="$sha"
  pr="$PR"
  echo ">>> PR #$PR 快关（full=$FULL）：sha=$sha base=$base"
fi

# ── 派发壳仓 ci ──────────────────────────────────────────────────────────────
echo ">>> gh workflow run ci.yml -R $SHELL_REPO ..."
marker="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
before="$(gh run list -R "$SHELL_REPO" --workflow ci.yml --limit 50 \
  --json databaseId -q '[.[].databaseId]' 2>/dev/null || echo '[]')"
if ! gh workflow run ci.yml -R "$SHELL_REPO" \
  -f sha="$sha" -f status_sha="$status_sha" -f pr="$pr" -f base="$base" -f full="$FULL"; then
  echo "✗ 派发失败（422 通常是传了壳仓不认识的 input，或 PAT 权限不足）"
  exit 1
fi
run_id=""
for _ in $(seq 1 10); do
  run_id="$(gh run list -R "$SHELL_REPO" --workflow ci.yml --limit 20 \
    --json databaseId,createdAt | \
    jq --argjson before "$before" --arg marker "$marker" '
      ($before | map(tostring)) as $seen
      | [ .[] | select(.createdAt >= $marker)
          | select((.databaseId | tostring) as $id | $seen | map(. == $id) | any | not) ]
      | .[0].databaseId')"
  [ -n "$run_id" ] && break
  sleep 3
done
[ -n "$run_id" ] || { echo "✗ 派发后找不到 run（查壳仓 run 列表）"; exit 1; }
echo ">>> 已派发 run #$run_id（sha=$sha）"

# ── 实时盯结果（agent 要即时知道，不等轮询）────────────────────────────────────
echo ">>> gh run watch #$run_id ...（实时，别关）"
gh run watch "$run_id" -R "$SHELL_REPO" --exit-status --interval 20
watch_rc=$?

# ── 汇报合并门 ────────────────────────────────────────────────────────────────
if [ "$MODE" = "pr" ] && [ -n "$PR" ]; then
  echo ">>> gh pr checks #$PR"
  gh pr checks "$PR" -R "$PRIVATE_REPO" || true

  state="$(gh api "repos/$PRIVATE_REPO/commits/$sha/statuses" \
    --jq '[.[] | select(.context=="shell-ci")] | .[0].state // "unknown"' 2>/dev/null || echo unknown)"
  run_url="https://github.com/$SHELL_REPO/actions/runs/$run_id"
  mode_txt=""; [ "$FULL" = "true" ] && mode_txt="全量 "
  body="harness-ci ${mode_txt}结果：\`$state\`（lint/test/app-shell 详见 [run]($run_url)）"
  if gh api "repos/$PRIVATE_REPO/issues/$PR/comments" -f "body=$body" >/dev/null 2>&1; then
    echo ">>> 已回填 PR #$PR comment（$state）"
  else
    echo ">>> 回填 comment 失败（PAT 需 issue:write 权限；commit status 仍已写）"
  fi
else
  echo ">>> main 全量结论：$watch_rc（合并门看 PR 的 shell-ci，main 全量异步不阻塞）"
fi
exit "$watch_rc"
