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
export TOKENZULIP_CODEX_MODEL=gpt-5.4
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
sudo podman build -t token-zulip .
```

The service below uses root's Podman storage. For manual rootless runs, use the same commands without `sudo`.

If you want to initialize or refresh the editable workspace from the container:

```bash
sudo podman run --rm --env-file .env --volume "$PWD:/runtime" localhost/token-zulip:latest init
```

Dry-run the container:

```bash
sudo podman run --rm --env-file .env --volume "$PWD:/runtime" localhost/token-zulip:latest run --dry-run
```

Run live by removing `--dry-run`.

Install the systemd service:

```bash
sudo mkdir -p /opt
sudo ln -sfn "$PWD" /opt/token-zulip
sudo systemctl link /opt/token-zulip/examples/systemd/token-zulip.service
sudo systemctl daemon-reload
sudo systemctl enable --now token-zulip.service
```

Useful service commands:

```bash
sudo systemctl status token-zulip.service
sudo journalctl -u token-zulip.service -f
sudo systemctl restart token-zulip.service
```

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
