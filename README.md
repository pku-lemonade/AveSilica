# Silica + TokenZulip

[![project chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://pku-lemonade.zulipchat.com)

Silica is a context-aware Zulip agent for team conversation, memory, and replies.

TokenZulip is the Python runtime behind Silica. It listens to Zulip, maps each visible channel/topic to a persistent Codex thread, and stores compact normalized messages, memory, pending message IDs, scratchpads, and agent turns under `workspace/`.

Zulip is an organized team chat app designed for efficient communication. We thank the Zulip team for generously offering a free standard plan for our team.

## Zulip Setup

1. In Zulip, create a Generic bot.
2. Download the bot's `.zuliprc` file. It contains the bot email, API key, and Zulip site URL.
3. By default, the bot registers for message events from all public channels. Set `TOKENZULIP_LISTEN_ALL_PUBLIC_STREAMS=false` if it should only read subscribed channels.
4. Keep the `.zuliprc` private. Anyone with the bot API key can act as the bot.

Zulip's full bot docs are here:

- [Bots overview](https://zulip.com/help/bots-overview)
- [Deploying bots in production](https://zulip.com/help/deploying-bots)

## Local Setup

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e '.[dev,codex]'
cp examples/.zuliprc.example .zuliprc
$EDITOR .zuliprc
token-zulip init
```

Optional environment:

```bash
export TOKENZULIP_ZULIPRC="$PWD/.zuliprc"
export TOKENZULIP_WORKSPACE="$PWD/workspace"
export TOKENZULIP_CODEX_MODEL=gpt-5.5
export TOKENZULIP_TYPING_ENABLED=true
export TOKENZULIP_BOT_ALIASES=Silica,Sili
```

Run without posting:

```bash
token-zulip run --dry-run
```

Run live:

```bash
token-zulip run
```

## Dry Run

`--dry-run` still connects to Zulip, reads visible channel messages, builds prompts, calls Codex, applies validated memory operations, and writes compact state files. It does not post Zulip replies.

After sending a test message in a visible public channel, inspect:

- `workspace/state/sessions/*/session.json`: session identity, Codex thread ID, and last processed message ID.
- `workspace/state/sessions/*/messages.jsonl`: compact normalized messages for the session.
- `workspace/state/sessions/*/turns.jsonl`: parsed model decisions, memory operations, scratchpad operation, and post status.
- `workspace/state/sessions/*/pending.json`: pending message IDs for an active session.
- `workspace/memory/items.json`: validated durable memory records.

When the outbound decisions look right, remove `--dry-run` or set `TOKENZULIP_POST_REPLIES=true`.

## Container On Debian With Podman

Clone the repo on the Debian host. Keep runtime files in the clone:

```bash
cp examples/.env.example .env
cp examples/.zuliprc.example .zuliprc
$EDITOR .env .zuliprc
podman build -t token-zulip .
```

The commands mount your existing `$HOME/.codex` into the container so Codex can reuse your local login and config.

If you want to initialize or refresh the editable workspace from the container:

```bash
podman run --rm --network host --env-file .env --volume "$PWD:/runtime" --volume "$HOME/.codex:/root/.codex" localhost/token-zulip:latest init
```

Dry-run the container:

```bash
podman run --rm --network host --env-file .env --volume "$PWD:/runtime" --volume "$HOME/.codex:/root/.codex" localhost/token-zulip:latest run --dry-run
```

Run live by removing `--dry-run`.

By default, the example `.env` sets `TOKENZULIP_CODEX_SANDBOX=danger-full-access`, `TOKENZULIP_CODEX_APPROVAL_POLICY=never`, and `TOKENZULIP_CODEX_REASONING_EFFORT=medium`. That is the low-friction Codex mode; the container and mounted paths are the boundary.

The example `.env` also sets HTTP proxy variables to `http://127.0.0.1:50834`. The `--network host` flag lets the container reach that host-local proxy.

Install the systemd service:

```bash
sudo mkdir -p /opt
sudo ln -sfn "$PWD" /opt/token-zulip
systemctl --user link /opt/token-zulip/examples/systemd/token-zulip.service
systemctl --user daemon-reload
systemctl --user enable --now token-zulip.service
```

Useful service commands:

```bash
systemctl --user status token-zulip.service
journalctl --user -u token-zulip.service -f
systemctl --user restart token-zulip.service
```

For auto-start after reboot without an interactive login, enable lingering once: `sudo loginctl enable-linger "$USER"`.

To test the service without posting, set `TOKENZULIP_POST_REPLIES=false` in `.env`, then restart the service.

## Workspace Layout

- `workspace/AGENTS.md`: global identity and high-level behavior.
- `workspace/roles/default.md`: default role, voice, style, and response formatting.
- `workspace/loop/participation.md`: rules for when to speak, stay silent, draft plans, or ask questions.
- `workspace/loop/memory.md`: durable memory proposal policy.
- `workspace/channels/<stream>/AGENTS.md`: optional stream-specific instructions.
- `workspace/channels/<stream>/<topic-hash>/AGENTS.md`: optional topic-specific instructions.
- `workspace/memory/items.json`: orchestrator-owned durable memory records.
- `workspace/state/`: compact session messages, session metadata, pending queues, scratchpads, turns, and error/ignored-event summaries.

## Instruction Architecture

Runtime behavior is driven by the live files under `workspace/`. `src/token_zulip/workspace.py` only seeds missing files during `token-zulip init`; it does not update an existing workspace unless initialization is explicitly run with overwrite behavior. `src/token_zulip/prompt.py` wraps the loaded instructions with the JSON decision schema and runtime contract.

Instruction layers are loaded in this order: hardcoded safety contract, `workspace/AGENTS.md`, `workspace/roles/<role>.md`, `workspace/loop/participation.md`, `workspace/loop/memory.md`, channel `AGENTS.md`, then topic `AGENTS.md`. Later configurable layers can specialize earlier workspace guidance, but they cannot override the hardcoded runtime contract.

Use these ownership boundaries to avoid duplicated or conflicting prompt text:

- `workspace/AGENTS.md`: global identity and high-level behavior.
- `workspace/roles/default.md`: voice, style, and response formatting.
- `workspace/loop/participation.md`: when to reply and which `reply_kind` to choose.
- `workspace/loop/memory.md`: durable memory proposal policy.
- `workspace/channels/.../AGENTS.md`: stream/topic-specific exceptions or preferences.

## Behavior

Incoming Zulip messages are normalized and persisted before any model call. Routine raw Zulip events are not stored. Work is serialized per `zulip:<realm_id>:stream:<stream_id>:topic:<topic_hash>` session, so a busy topic cannot race itself. If messages arrive for an active topic, their IDs are appended to that topic's pending queue and processed in a follow-up turn.

Codex returns structured JSON with `should_reply`, `reply_kind`, `message_to_post`, `memory_ops`, `scratchpad_op`, and confidence. The orchestrator validates and writes memory before posting any reply.

When live posting is enabled, the bot can show Zulip typing indicators for every processed message. Silent channel decisions stop typing after Codex decides not to reply. Set `TOKENZULIP_TYPING_ENABLED=false` to disable typing indicators.
