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
- `workspace/references/codex-thread-contract.md`: shared non-negotiable contract for every Codex thread.
- `workspace/references/*-prompt.md`: role-specific prompt templates for reply, workers, and scheduled jobs.
- `workspace/references/*-decision-schema.json`: native Codex structured output schemas for reply, memory, skill, schedule, and scheduled-job turns.
- `workspace/references/*-policy.md`: role-specific reply, memory, skill, schedule, and scheduled-job policies.
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

Runtime behavior is driven by the live files under `workspace/`. `src/token_zulip/workspace.py` copies missing template files from the checked-in `workspace/` tree during `token-zulip init`; it does not contain prompt prose or update existing workspace files unless initialization is explicitly run with overwrite behavior. `src/token_zulip/instructions.py` composes role-specific developer instructions, `src/token_zulip/prompt.py` renders role-specific prompts, and `src/token_zulip/codex_adapter.py` runs the native Codex `output_schema` for the reply/session thread and each forked op worker.

Every role starts with `workspace/references/codex-thread-contract.md`, then loads only its role policy and scoped `AGENTS.md` files. The reply/session thread also loads `workspace/AGENTS.md` and `workspace/references/reply-thread-policy.md`; workers load only their worker policy plus scoped `AGENTS.md`; scheduled job threads load `workspace/AGENTS.md`, `workspace/references/scheduled-job-policy.md`, and memory policy.

The composed instruction layers are passed to Codex as `developer_instructions` only when a persistent thread is created, or when an ephemeral worker fork is created. Existing marked reply/session and job threads are resumed without repeating those instructions.

Use these ownership boundaries to avoid duplicated or conflicting prompt text:

- `workspace/AGENTS.md`: global identity, voice, style, and high-level behavior.
- `workspace/references/codex-thread-contract.md`: shared Codex thread contract and structured-output boundaries.
- `workspace/references/reply-thread-policy.md`: when to reply and which `reply_kind` to choose.
- `workspace/references/reply-turn-prompt.md`: reply/session thread prompt template.
- `workspace/references/reply-decision-schema.json`: reply/session thread schema.
- `workspace/references/memory-decision-schema.json`: memory worker schema.
- `workspace/references/skill-decision-schema.json`: skill worker schema.
- `workspace/references/schedule-decision-schema.json`: schedule worker schema.
- `workspace/references/scheduled-job-decision-schema.json`: scheduled job runtime schema.
- `workspace/references/*-worker-prompt.md`: memory, skill, and schedule worker prompt templates.
- `workspace/references/*-worker-policy.md`: memory, skill, and schedule worker policies.
- `workspace/references/scheduled-job-prompt.md`: scheduled job runtime prompt template.
- `workspace/references/scheduled-job-policy.md`: scheduled job runtime policy.
- `workspace/memory/AGENTS.md`: human-authored global deployment/team preferences.
- `workspace/memory/.../AGENTS.md`: human-authored channel/topic/private exceptions or preferences.
- `workspace/memory/.../MEMORY.md`: compact remembered context. This is the memory source of truth for validated memory operations and is injected into Codex prompts when the scoped memory snapshot changes.

Memory follows a Hermes-style markdown model. `MEMORY.md` is a compact memory source of truth. Entries are separated by `§`; TokenZulip applies `add`, `replace`, and `remove` memory operations directly to the scoped file. Raw/session history remains in `workspace/records/stream-*/topic-*/messages.jsonl` or `workspace/records/private-*/messages.jsonl`, and `turns.jsonl` keeps the historical log of memory decisions, acknowledgements, and applied or rejected operations.

Scheduled tasks follow a Hermes-inspired job model. The schedule worker requests changes through `schedule_ops`; the schedule code path validates and persists jobs under `workspace/schedules/jobs.json`, appends an acknowledgement only after persistence succeeds, and runs due jobs from a scheduler ticker inside `token-zulip run`. Jobs post back only to their originating Zulip topic or private chat. Jobs may be prompt-only or skill-backed; skill-backed jobs store skill names and load `workspace/skills/<name>/SKILL.md` only when the job fires.

Skill persistence is owned by the skill worker code path: its forked Codex decision may return `skill_ops` containing a skill name, description, and `SKILL.md` content. TokenZulip validates that request and writes `workspace/skills/<name>/SKILL.md`. Scheduled job threads do not write skills; they only load skill names recorded on the job.

### Thread Context Model

TokenZulip keeps one persistent Codex thread per Zulip conversation as the reply/session thread. On each Zulip update, the runtime forks short-lived worker threads from that session thread for independent operation decisions. Workers return structured output only; TokenZulip applies each worker's code path directly.

```text
Zulip event
    |
    v
persistent reply/session thread
    |
    |-- fork_context=true --> memory worker thread   --> memory_ops code path
    |-- fork_context=true --> skill worker thread    --> skill_ops code path
    `-- fork_context=true --> schedule worker thread --> schedule_ops code path

due scheduled job
    |
    v
fresh scheduled job thread
    |
    `-- posts result to origin Zulip conversation
          |
          `-- enqueue posted_bot_update for origin reply/session thread
```

| Thread | Persistence | Inherited Codex context | Injected prompt context | Output |
| --- | --- | --- | --- | --- |
| Reply/session thread | Long-lived per Zulip DM or stream/topic | Previous Codex turns for this Zulip conversation | Current Zulip message batch, scoped durable memory when changed, pending `posted_bot_update` | User-visible reply decision only |
| Memory worker thread | Ephemeral fork | Previous reply/session thread context | Current Zulip message batch, scoped durable memory when changed, pending `posted_bot_update` | `memory_ops` only |
| Skill worker thread | Ephemeral fork | Previous reply/session thread context | Current Zulip message batch, scoped durable memory when changed, pending `posted_bot_update` | `skill_ops` only |
| Schedule worker thread | Ephemeral fork | Previous reply/session thread context | Current Zulip message batch, scoped durable memory when changed, active schedule context, pending `posted_bot_update` | `schedule_ops` only |
| Scheduled job thread | Fresh per job run | None | Job brief, schedule spec, loaded skill content, scoped durable memory, current time | Scheduled result reply and optional `memory_ops` |

`recent_context` is not injected into Codex prompts. Conversation continuity comes from Codex thread history and forked Codex context.

TokenZulip injects one narrow continuity record when needed: `posted_bot_update`. A `posted_bot_update` is Sili's actual visible contribution to the Zulip conversation after runtime processing. It includes the final posted reply or dry-run text after deterministic acknowledgements, and it also includes scheduled job output posted back into the origin Zulip conversation. The update is injected once into the next normal conversation turn for the reply/session thread and all operation workers, then marked consumed.

This exists because the visible Zulip message may differ from the reply thread's raw JSON. The reply thread may return `message_to_post: "Done."`, then TokenZulip persists a schedule and posts:

```md
Done.

**Schedule created**
- Name: Camera-ready check
- Trigger: once at 2026-05-01 09:00 Asia/Shanghai
```

The next turn receives a compact `posted_bot_update` with that final posted text, so future replies and forked workers know what was actually confirmed.

### Thread And Schedule Flows

Normal Zulip turn:

```text
Zulip event
  -> normalize message
  -> append messages.jsonl
  -> persistent reply/session Codex thread for this Zulip topic/private chat
       output: message_to_post only
  -> fork three ephemeral op workers from the reply/session thread
       memory worker   -> memory_ops   -> workspace/memory/.../MEMORY.md
       skill worker    -> skill_ops    -> workspace/skills/<name>/SKILL.md
       schedule worker -> schedule_ops -> workspace/schedules/jobs.json
  -> append each worker code path's deterministic acknowledgement
  -> post Zulip reply, or record dry-run post
  -> enqueue posted_bot_update for the next conversation turn
```

Creating a prompt-only scheduled job:

```text
Human conversation says: follow up Friday / remind us / run this weekly
  -> schedule worker returns schedule_ops.create
  -> schedule worker code path writes jobs.json
       prompt: self-contained job instruction
       skills: []
       origin: current Zulip topic/private chat
       next_run_at: UTC ISO timestamp
  -> Sili posts a Markdown acknowledgement with name, trigger, next run, and job id
```

Creating a skill-backed scheduled job:

```text
Human conversation asks for a reusable workflow
  -> skill worker returns skill_ops.create/update
  -> skill worker code path writes workspace/skills/<name>/SKILL.md
  -> schedule worker may return schedule_ops.create with skills: ["<name>"]
  -> schedule worker code path validates the skill exists and stores only the skill name
  -> Sili posts both "Skill saved: ..." and a Markdown schedule acknowledgement
```

Running a due scheduled job:

```text
scheduler ticker wakes every TOKENZULIP_SCHEDULE_TICK_SECONDS
  -> ScheduleStore.get_due_jobs()
  -> for each due job, start a fresh scheduled job Codex thread
       developer_instructions:
         codex-thread-contract.md
         scheduled-job-policy.md
         memory-worker-policy.md
         scoped AGENTS.md files
       prompt:
         job id/name/time
         job prompt
         loaded SKILL.md contents for job.skills
         scoped memory for the origin Zulip conversation
         output rules
       output: message_to_post and optional memory_ops
  -> post result to the original Zulip topic/private chat
  -> enqueue posted_bot_update for the origin reply/session thread
  -> append workspace/records/scheduled/<job_id>/runs.jsonl
  -> update jobs.json last_run_at/next_run_at/status
```

A scheduled job can suppress delivery by returning no reply, or by returning exactly `[SILENT]`. The run is still recorded locally.

For Zulip terminology, the code uses `stream` for what Zulip's UI calls a channel. A topic is the thread-like subject inside a channel.

## Behavior

Incoming Zulip messages are normalized and persisted before any model call. Routine raw Zulip events are not stored. Work is serialized per `zulip:<realm_id>:stream:<stream_id>:topic:<topic_hash>` session, so a busy topic cannot race itself. If messages arrive for an active topic, their IDs are appended to that topic's pending queue and processed in a follow-up turn.

Zulip upload links in raw Markdown are downloaded to the session's `uploads/<message_id>/` directory before Codex runs. The prompt receives rewritten Markdown pointing at the local downloaded files. Set `TOKENZULIP_UPLOAD_MAX_BYTES` to control the per-file download limit.

When a stream/topic or private-chat session already has a marked Codex thread, TokenZulip resumes that thread and sends only the new Zulip message batch plus any changed scoped memory snapshot and pending `posted_bot_update`. New or legacy unmarked sessions start a fresh Codex thread with composed `developer_instructions`; they do not replay recent Zulip records into the prompt.

The reply/session thread returns only `should_reply`, `reply_kind`, `message_to_post`, and confidence. Three ephemeral forked workers return memory, skill, and schedule decisions through separate schemas and code paths. Schedule operations use a decomposed `schedule_spec`: `once_at` for ISO one-shot times, `once_in` for relative one-shot delays like `30m`, `interval` for recurring durations like `2h`, `cron` for recurring wall-clock schedules like `0 9 * * *`, and `unchanged` for lifecycle operations that do not change timing. TokenZulip validates and persists applied changes, appends deterministic acknowledgements, and then posts any reply.

When schedules are enabled, the listener also runs a background scheduler. Configure it with `TOKENZULIP_SCHEDULES_ENABLED`, `TOKENZULIP_SCHEDULE_TICK_SECONDS`, `TOKENZULIP_SCHEDULE_TIMEZONE`, and `TOKENZULIP_SCHEDULE_RUN_TIMEOUT_SECONDS`. Scheduled job runs start fresh Codex threads from persisted job data, loaded skills, scoped memory, and current time, so scheduled automation history does not pollute the human Zulip conversation thread.

When live posting is enabled, the bot can show Zulip typing indicators for every processed message. Silent channel decisions stop typing after Codex decides not to reply. Set `TOKENZULIP_TYPING_ENABLED=false` to disable typing indicators.
