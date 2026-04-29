# Silica + TokenZulip

[![project chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://pku-lemonade.zulipchat.com)

Silica is a context-aware Zulip agent for team conversation, memory, and replies.

TokenZulip is the Python runtime behind Silica. It listens to Zulip, maps each visible channel/topic to a persistent Codex thread, stores curated memory under `workspace/memory/`, and stores generated conversation records under `workspace/records/`.

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
export TOKENZULIP_SCHEDULE_TIMEZONE=Asia/Shanghai
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

`--dry-run` still connects to Zulip, reads visible channel messages, builds prompts, calls Codex, applies validated memory operations, and writes compact record files. It does not post Zulip replies.

After sending a test message in a visible public channel, inspect:

- `workspace/records/stream-*/topic-*/*`: stream/topic session identity, messages, pending queues, turns, and downloaded uploads.
- `workspace/records/private-*/*`: private-chat session identity, messages, pending queues, turns, and downloaded uploads.
- `workspace/memory/MEMORY.md`: compact global memory updated by validated memory operations.
- `workspace/memory/stream-*/MEMORY.md`: compact channel memory.
- `workspace/memory/stream-*/topic-*/MEMORY.md`: compact topic memory.
- `workspace/schedules/jobs.json`: durable scheduled task records.
- `workspace/records/scheduled/<job_id>/runs.jsonl`: scheduled task run audit records.

When the outbound decisions look right, remove `--dry-run` or set `TOKENZULIP_POST_REPLIES=true`.

## Container On Debian With Podman

Clone the repo on the Debian host. Keep runtime files in the clone:

```bash
cp examples/.env.example .env
cp examples/.zuliprc.example .zuliprc
$EDITOR .env .zuliprc
podman build --http-proxy=false -t token-zulip .
```

The commands mount your existing `$HOME/.codex` into the container so Codex can reuse your local login and config.
The build disables Podman's host proxy forwarding so image dependency installation is not tied to any runtime HTTP proxy configured in `.env`.

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

For predictable scheduled tasks in containers, set `TOKENZULIP_SCHEDULE_TIMEZONE` in `.env`, for example `TOKENZULIP_SCHEDULE_TIMEZONE=Asia/Shanghai`. Setting `TZ` is useful for container logs, but TokenZulip parses schedule times from `TOKENZULIP_SCHEDULE_TIMEZONE`.

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
- `workspace/references/runtime-contract.md`: non-negotiable runtime contract included in Codex developer instructions.
- `workspace/references/turn-prompt.md`: per-turn Zulip message prompt template.
- `workspace/references/decision-schema.json`: native Codex structured output schema.
- `workspace/references/participation.md`: rules for when to speak, stay silent, draft plans, or ask questions.
- `workspace/references/memory-policy.md`: memory operation and scope policy.
- `workspace/memory/AGENTS.md`: optional global deployment/team instructions.
- `workspace/memory/MEMORY.md`: compact global memory updated by validated memory operations.
- `workspace/memory/stream-<slug>-<id>/AGENTS.md`: optional channel-specific instructions.
- `workspace/memory/stream-<slug>-<id>/MEMORY.md`: compact channel memory.
- `workspace/memory/stream-<slug>-<id>/topic-<slug>-<6hex>/AGENTS.md`: optional topic-specific instructions.
- `workspace/memory/stream-<slug>-<id>/topic-<slug>-<6hex>/MEMORY.md`: compact topic memory.
- `workspace/memory/private-<user>/MEMORY.md`: compact private-chat memory.
- `workspace/skills/<name>/SKILL.md`: reusable skill instructions that scheduled jobs may load.
- `workspace/schedules/jobs.json`: durable scheduled task definitions, including origin Zulip topic/private chat, next run, repeat state, and optional skill names.
- `workspace/records/stream-<slug>-<id>/topic-<slug>-<6hex>/`: generated stream/topic session messages, session metadata, pending queues, turns, and uploads. Channel renames and Zulip topic/message moves update these readable paths while preserving known local history.
- `workspace/records/private-<user>/`: generated private-chat session messages, session metadata, pending queues, turns, and uploads.
- `workspace/records/scheduled/<job_id>/`: scheduled task run audit records.
- `workspace/records/errors/`: error and ignored-event summaries.

## Instruction Architecture

Runtime behavior is driven by the live files under `workspace/`. `src/token_zulip/workspace.py` copies missing template files from the checked-in `workspace/` tree during `token-zulip init`; it does not contain prompt prose or update existing workspace files unless initialization is explicitly run with overwrite behavior. `src/token_zulip/instructions.py` composes developer instructions, `src/token_zulip/prompt.py` renders the per-turn Zulip update from `workspace/references/turn-prompt.md`, and `src/token_zulip/codex_adapter.py` loads `workspace/references/decision-schema.json` for the native Codex `output_schema`.

Instruction layers are loaded in this order: `workspace/references/runtime-contract.md`, `workspace/AGENTS.md`, `workspace/references/participation.md`, `workspace/references/memory-policy.md`, optional `workspace/memory/AGENTS.md`, optional channel `AGENTS.md`, then optional topic/private `AGENTS.md` under `workspace/memory/`. Later configurable layers can specialize earlier workspace guidance, but they cannot override the runtime contract.

The composed instruction layers are passed to Codex as `developer_instructions` only when a new Codex thread is created. Existing marked threads are resumed without repeating those instructions, and the per-turn prompt contains only the Zulip conversation update.

Use these ownership boundaries to avoid duplicated or conflicting prompt text:

- `workspace/AGENTS.md`: global identity, voice, style, and high-level behavior.
- `workspace/references/runtime-contract.md`: orchestrator contract, structured-output expectations, and reply/memory decision semantics.
- `workspace/references/turn-prompt.md`: the current-message prompt template.
- `workspace/references/decision-schema.json`: schema for `should_reply`, `reply_kind`, `message_to_post`, `memory_ops`, `schedule_ops`, `skill_ops`, and confidence.
- `workspace/references/participation.md`: when to reply and which `reply_kind` to choose.
- `workspace/references/memory-policy.md`: memory operation and scope policy.
- `workspace/memory/AGENTS.md`: human-authored global deployment/team preferences.
- `workspace/memory/.../AGENTS.md`: human-authored channel/topic/private exceptions or preferences.
- `workspace/memory/.../MEMORY.md`: compact remembered context. This is the memory source of truth for validated memory operations and is injected into Codex prompts when the scoped memory snapshot changes.

Memory follows a Hermes-style markdown model. `MEMORY.md` is a compact memory source of truth. Entries are separated by `§`; the orchestrator applies `add`, `replace`, and `remove` memory operations directly to the scoped file. Raw/session history remains in `workspace/records/stream-*/topic-*/messages.jsonl` or `workspace/records/private-*/messages.jsonl`, and `turns.jsonl` keeps the historical log of memory decisions, acknowledgements, and applied or rejected operations.

Scheduled tasks follow a Hermes-inspired job model. Codex requests changes through `schedule_ops`; the orchestrator validates and persists jobs under `workspace/schedules/jobs.json`, appends an acknowledgement only after persistence succeeds, and runs due jobs from a scheduler ticker inside `token-zulip run`. Jobs post back only to their originating Zulip topic or private chat. Jobs may be prompt-only or skill-backed; skill-backed jobs store skill names and load `workspace/skills/<name>/SKILL.md` only when the job fires.

Current skill persistence is orchestrator-owned: the normal Zulip Codex turn may return `skill_ops` containing a skill name, description, and `SKILL.md` content. TokenZulip validates that request and writes `workspace/skills/<name>/SKILL.md`. There is not yet a separate Codex skill-writer thread. Scheduled job threads do not write skills; they only load skill names recorded on the job.

### Thread And Schedule Flows

Normal Zulip turn:

```text
Zulip event
  -> normalize message
  -> append messages.jsonl
  -> session Codex thread for this Zulip topic/private chat
       developer_instructions: workspace instruction layers
       prompt: recent context + current messages + scoped memory + scheduling context
       output: message_to_post, memory_ops, schedule_ops, skill_ops
  -> orchestrator validates and persists ops
       memory_ops   -> workspace/memory/.../MEMORY.md
       skill_ops    -> workspace/skills/<name>/SKILL.md
       schedule_ops -> workspace/schedules/jobs.json
  -> append deterministic acknowledgements
  -> post Zulip reply, or record dry-run post
```

Creating a prompt-only scheduled job:

```text
Human conversation says: follow up Friday / remind us / run this weekly
  -> session Codex thread returns schedule_ops.create
  -> ScheduleStore writes jobs.json
       prompt: self-contained job instruction
       skills: []
       origin: current Zulip topic/private chat
       next_run_at: UTC ISO timestamp
  -> Sili posts a Markdown acknowledgement with name, trigger, next run, and job id
```

Creating a skill-backed scheduled job:

```text
Human conversation asks for a reusable workflow
  -> session Codex thread returns skill_ops.create/update
  -> SkillStore writes workspace/skills/<name>/SKILL.md
  -> same decision may return schedule_ops.create with skills: ["<name>"]
  -> ScheduleStore validates the skill exists and stores only the skill name
  -> Sili posts both "Skill saved: ..." and a Markdown schedule acknowledgement
```

Running a due scheduled job:

```text
scheduler ticker wakes every TOKENZULIP_SCHEDULE_TICK_SECONDS
  -> ScheduleStore.get_due_jobs()
  -> for each due job, use a separate job-scoped Codex thread
       developer_instructions:
         workspace instruction layers
         + "Scheduled Task Runtime"
         + "Do not create, update, or remove schedules from scheduled runs."
       prompt:
         job id/name/time
         job prompt
         loaded SKILL.md contents for job.skills
         scoped memory for the origin Zulip conversation
         output rules
       output: message_to_post and optional memory_ops
  -> ignore any schedule_ops or skill_ops from the job thread
  -> post result to the original Zulip topic/private chat
  -> append workspace/records/scheduled/<job_id>/runs.jsonl
  -> update jobs.json last_run_at/next_run_at/status
```

A scheduled job can suppress delivery by returning no reply, or by returning exactly `[SILENT]`. The run is still recorded locally.

For Zulip terminology, the code uses `stream` for what Zulip's UI calls a channel. A topic is the thread-like subject inside a channel.

## Behavior

Incoming Zulip messages are normalized and persisted before any model call. Routine raw Zulip events are not stored. Work is serialized per `zulip:<realm_id>:stream:<stream_id>:topic:<topic_hash>` session, so a busy topic cannot race itself. If messages arrive for an active topic, their IDs are appended to that topic's pending queue and processed in a follow-up turn.

Zulip upload links in raw Markdown are downloaded to the session's `uploads/<message_id>/` directory before Codex runs. The prompt receives rewritten Markdown pointing at the local downloaded files. Set `TOKENZULIP_UPLOAD_MAX_BYTES` to control the per-file download limit.

When a stream/topic or private-chat session already has a marked Codex thread, TokenZulip resumes that thread and sends only the new Zulip message batch plus any changed scoped memory snapshot. New or legacy unmarked sessions start a fresh Codex thread with composed `developer_instructions` and a capped bootstrap of previous messages. `TOKENZULIP_RECENT_MESSAGES` controls that bootstrap cap and defaults to 100.

Codex returns structured output with `should_reply`, `reply_kind`, `message_to_post`, `memory_ops`, `schedule_ops`, `skill_ops`, and confidence via the native `output_schema`. Schedule operations use a decomposed `schedule_spec`: `once_at` for ISO one-shot times, `once_in` for relative one-shot delays like `30m`, `interval` for recurring durations like `2h`, `cron` for recurring wall-clock schedules like `0 9 * * *`, and `unchanged` for lifecycle operations that do not change timing. The orchestrator validates memory, skill, and schedule operations, persists applied changes, appends deterministic acknowledgements, and then posts any reply.

When schedules are enabled, the listener also runs a background scheduler. Configure it with `TOKENZULIP_SCHEDULES_ENABLED`, `TOKENZULIP_SCHEDULE_TICK_SECONDS`, `TOKENZULIP_SCHEDULE_TIMEZONE`, and `TOKENZULIP_SCHEDULE_RUN_TIMEOUT_SECONDS`. Recurring jobs use separate job-scoped Codex threads so scheduled automation history does not pollute the human Zulip conversation thread.

When live posting is enabled, the bot can show Zulip typing indicators for every processed message. Silent channel decisions stop typing after Codex decides not to reply. Set `TOKENZULIP_TYPING_ENABLED=false` to disable typing indicators.
