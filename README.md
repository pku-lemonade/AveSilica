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
- `workspace/memory/MEMORY.md`: compact global memory injected into prompts.
- `workspace/memory/seeds.jsonl`: structured global memory seeds and provenance.
- `workspace/memory/stream-*/MEMORY.md`: compact channel memory.
- `workspace/memory/stream-*/topic-*/MEMORY.md`: compact topic memory.

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

- `workspace/AGENTS.md`: global identity, voice, style, and high-level behavior.
- `workspace/references/participation.md`: rules for when to speak, stay silent, draft plans, or ask questions.
- `workspace/references/memory-policy.md`: memory seed proposal and scope policy.
- `workspace/memory/AGENTS.md`: optional global deployment/team instructions.
- `workspace/memory/MEMORY.md`: compact global memory injected into every prompt.
- `workspace/memory/seeds.jsonl`: structured global memory seeds and provenance.
- `workspace/memory/stream-<id>-<slug>/AGENTS.md`: optional channel-specific instructions.
- `workspace/memory/stream-<id>-<slug>/MEMORY.md`: compact channel memory.
- `workspace/memory/stream-<id>-<slug>/seeds.jsonl`: structured channel memory seeds.
- `workspace/memory/stream-<id>-<slug>/topic-<hash>/AGENTS.md`: optional topic-specific instructions.
- `workspace/memory/stream-<id>-<slug>/topic-<hash>/MEMORY.md`: compact topic memory.
- `workspace/memory/stream-<id>-<slug>/topic-<hash>/seeds.jsonl`: structured topic memory seeds.
- `workspace/memory/private-<user>/MEMORY.md`: compact private-chat memory.
- `workspace/state/`: compact session messages, session metadata, pending queues, scratchpads, turns, and error/ignored-event summaries.

## Instruction Architecture

Runtime behavior is driven by the live files under `workspace/`. `src/token_zulip/workspace.py` only seeds missing files during `token-zulip init`; it does not update an existing workspace unless initialization is explicitly run with overwrite behavior. `src/token_zulip/prompt.py` wraps the loaded instructions with the native Codex output schema and runtime contract.

Instruction layers are loaded in this order: hardcoded safety contract, `workspace/AGENTS.md`, `workspace/references/participation.md`, `workspace/references/memory-policy.md`, optional `workspace/memory/AGENTS.md`, optional channel `AGENTS.md`, then optional topic/private `AGENTS.md` under `workspace/memory/`. Later configurable layers can specialize earlier workspace guidance, but they cannot override the hardcoded runtime contract.

Use these ownership boundaries to avoid duplicated or conflicting prompt text:

- `workspace/AGENTS.md`: global identity, voice, style, and high-level behavior.
- `workspace/references/participation.md`: when to reply and which `reply_kind` to choose.
- `workspace/references/memory-policy.md`: memory seed proposal and scope policy.
- `workspace/memory/AGENTS.md`: human-authored global deployment/team preferences.
- `workspace/memory/.../AGENTS.md`: human-authored channel/topic/private exceptions or preferences.
- `workspace/memory/.../MEMORY.md`: compact remembered context. This is read into prompts.
- `workspace/memory/.../seeds.jsonl`: structured memory seeds used for IDs, status, source attribution, and regeneration of `MEMORY.md`.

Memory follows a Hermes-style split. `MEMORY.md` is the compact always-injected memory surface. `seeds.jsonl` is not raw chat history and is not injected directly; it is the structured backing layer for memory candidates, updates, archives, and provenance. Raw/session history remains in `workspace/state/sessions/*/messages.jsonl` and turn logs remain in `workspace/state/sessions/*/turns.jsonl`.

For Zulip terminology, the code uses `stream` for what Zulip's UI calls a channel. A topic is the thread-like subject inside a channel.

## Behavior

Incoming Zulip messages are normalized and persisted before any model call. Routine raw Zulip events are not stored. Work is serialized per `zulip:<realm_id>:stream:<stream_id>:topic:<topic_hash>` session, so a busy topic cannot race itself. If messages arrive for an active topic, their IDs are appended to that topic's pending queue and processed in a follow-up turn.

Codex returns structured output with `should_reply`, `reply_kind`, `message_to_post`, `memory_ops`, `scratchpad_op`, and confidence via the native `output_schema`. The orchestrator validates memory operations, writes scoped seeds, regenerates compact `MEMORY.md`, and then posts any reply.

When live posting is enabled, the bot can show Zulip typing indicators for every processed message. Silent channel decisions stop typing after Codex decides not to reply. Set `TOKENZULIP_TYPING_ENABLED=false` to disable typing indicators.
