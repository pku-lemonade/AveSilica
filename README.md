# TokenZulip

TokenZulip is a Python Zulip listener that maps each subscribed channel/topic to a persistent Codex thread. It stores prompts, transcripts, memory, pending messages, and outbound decisions in files under `workspace/`.

## Zulip Setup

1. In Zulip, create a Generic bot.
2. Download the bot's `.zuliprc` file. It contains the bot email, API key, and Zulip site URL.
3. Subscribe the bot to the channels it should read and reply in.
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

`--dry-run` still connects to Zulip, reads visible channel messages, builds prompts, calls Codex, applies validated memory updates, and writes state files. It does not post Zulip replies.

After sending a test message in a subscribed channel, inspect:

- `workspace/state/raw/*.jsonl`: raw Zulip events.
- `workspace/state/sessions/*/transcript.jsonl`: normalized topic transcript.
- `workspace/state/sessions/*/metadata.json`: Codex thread ID and last processed message ID.
- `workspace/state/sessions/*/outbound.jsonl`: model decision and the message that would have been posted.
- `workspace/memory/*.md`: validated durable memory updates.

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

By default, the example `.env` sets `TOKENZULIP_CODEX_SANDBOX=danger-full-access` and `TOKENZULIP_CODEX_APPROVAL_POLICY=never`. That is the low-friction Codex mode; the container and mounted paths are the boundary. `TOKENZULIP_CODEX_REASONING_EFFORT` is blank by default, so the adapter does not pass an effort value and the SDK/model default is used.

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

- `workspace/AGENTS.md`: global bot control instructions.
- `workspace/AGENTS.override.md`: temporary global override, ignored while it only contains comments.
- `workspace/roles/default.md`: default role and personality.
- `workspace/loop/participation.md`: rules for speaking, staying silent, drafting plans, and asking questions.
- `workspace/loop/memory.md`: rules for durable memory proposals.
- `workspace/channels/<stream>/AGENTS.md`: optional stream-specific instructions.
- `workspace/channels/<stream>/<topic-hash>/AGENTS.md`: optional topic-specific instructions.
- `workspace/memory/*.md`: orchestrator-owned durable memory.
- `workspace/state/`: raw Zulip events, transcripts, session metadata, pending queues, scratchpads, and outbound logs.

## Behavior

Incoming Zulip events are persisted before any model call. Work is serialized per `zulip:<realm_id>:stream:<stream_id>:topic:<topic_hash>` session, so a busy topic cannot race itself. If messages arrive for an active topic, they are appended to that topic's pending queue and processed in a follow-up turn.

Codex returns structured JSON with `should_reply`, `reply_kind`, `message_to_post`, memory updates, scratchpad updates, and confidence. The orchestrator validates and writes memory before posting any reply.
