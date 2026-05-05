# Silica + TokenZulip

[![project chat](https://img.shields.io/badge/zulip-join_chat-brightgreen.svg)](https://pku-lemonade.zulipchat.com)

Silica is a context-aware Zulip agent for team conversation, scheduling, reusable skills, and replies.

TokenZulip is the Python runtime behind Silica. It listens to Zulip, maps each visible channel/topic to a persistent Codex thread, stores generated conversation records under `workspace/records/`, and writes non-injected reflection candidates under `workspace/reflections/`.

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
export TOKENZULIP_SCHEDULE_DEFAULT_TIME=09:00
export TOKENZULIP_TRACE_RETENTION_DAYS=30
export TOKENZULIP_TRACE_AUTO_CLEANUP=false
export TOKENZULIP_TRACE_CLEANUP_INTERVAL_HOURS=24
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

`--dry-run` still connects to Zulip, reads visible channel messages, builds prompts, calls Codex, applies validated worker operations, and writes compact record files. It does not post Zulip replies.

After sending a test message in a visible public channel, inspect:

- `workspace/records/stream-*/topic-*/*`: stream/topic session identity, messages, pending queues, turns, and downloaded uploads.
- `workspace/records/private-recipient-*/*`: private-chat session identity, messages, pending queues, turns, and downloaded uploads.
- `workspace/reflections/REFLECTIONS.md`: global reflection candidates for later human review.
- `workspace/reflections/stream-*/REFLECTIONS.md`: channel reflection candidates.
- `workspace/reflections/private-recipient-*/REFLECTIONS.md`: private-chat reflection candidates.
- `workspace/schedules/jobs.json`: durable scheduled task records.
- `workspace/records/scheduled/<job_id>/runs.jsonl`: scheduled task run audit records.
- `workspace/records/**/traces/`: pruneable prompt/debug traces with rendered prompts, developer instructions, schemas, raw outputs, and parsed decisions.

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
podman run --rm --init --network host --env-file .env --volume "$PWD:/runtime" --volume "$HOME/.codex:/root/.codex" localhost/token-zulip:latest run --dry-run
```

Run live by removing `--dry-run`.
Use `--init` for long-running `run` containers and systemd services. Without it, `token-zulip` is PID 1 in the container; Linux ignores default-handled SIGTERM for PID 1, so `podman stop` waits for its timeout and then reports a SIGKILL fallback.

By default, the example `.env` sets `TOKENZULIP_CODEX_SANDBOX=danger-full-access`, `TOKENZULIP_CODEX_APPROVAL_POLICY=never`, and `TOKENZULIP_CODEX_REASONING_EFFORT=medium`. That is the low-friction Codex mode; the container and mounted paths are the boundary.

The example `.env` also sets HTTP proxy variables to `http://127.0.0.1:50834`. The `--network host` flag lets the container reach that host-local proxy.

For predictable scheduled tasks in containers, set `TOKENZULIP_SCHEDULE_TIMEZONE` in `.env`, for example `TOKENZULIP_SCHEDULE_TIMEZONE=Asia/Shanghai`. `TOKENZULIP_SCHEDULE_DEFAULT_TIME` controls omitted clock times, defaulting to `09:00`. Setting `TZ` is useful for container logs, but TokenZulip parses schedule times from `TOKENZULIP_SCHEDULE_TIMEZONE`.

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

If an installed user unit predates this README, check that its `ExecStart` contains `podman run --init` and that it uses `KillMode=mixed`, not `KillMode=none`. After changing the unit, run `systemctl --user daemon-reload` before restarting the service.

For auto-start after reboot without an interactive login, enable lingering once: `sudo loginctl enable-linger "$USER"`.

To test the service without posting, set `TOKENZULIP_POST_REPLIES=false` in `.env`, then restart the service.

## Workspace Layout

- `workspace/AGENTS.md`: global identity, voice, style, and high-level behavior.
- `workspace/references/system.md`: shared non-negotiable contract for every Codex thread.
- `workspace/references/<agent>/user.md`: role-specific user prompt template.
- `workspace/references/<agent>/system.md`: role-specific system configuration loaded as Codex developer instructions.
- `workspace/references/<agent>/schema.json`: native Codex structured output schema.
- `workspace/instructions/stream-<slug>-<id>/AGENTS.md`: optional channel-specific human instructions.
- `workspace/instructions/stream-<slug>-<id>/topic-<slug>-<6hex>/AGENTS.md`: optional topic-specific human instructions.
- `workspace/instructions/private-recipient-<recipient>/AGENTS.md`: optional private-chat human instructions.
- `workspace/reflections/REFLECTIONS.md`: global reflection candidates.
- `workspace/reflections/stream-<slug>-<id>/REFLECTIONS.md`: channel reflection candidates.
- `workspace/reflections/private-recipient-<recipient>/REFLECTIONS.md`: private-chat reflection candidates.
- `workspace/skills/<name>/SKILL.md`: reusable skill instructions that scheduled jobs may load.
- `workspace/schedules/jobs.json`: durable scheduled task definitions, including origin Zulip topic/private chat, next run, repeat state, and optional skill names.
- `workspace/records/stream-<slug>-<id>/topic-<slug>-<6hex>/`: generated stream/topic session messages, session metadata, pending queues, turns, and uploads. Channel renames and Zulip topic/message moves update these readable paths while preserving known local history.
- `workspace/records/private-recipient-<recipient>/`: generated private-chat session messages, session metadata, pending queues, turns, and uploads.
- `workspace/records/scheduled/<job_id>/`: scheduled task run audit records.
- `workspace/records/**/traces/`: pruneable prompt/debug traces. These are sidecars, not canonical conversation history.
- `workspace/records/errors/`: error and ignored-event summaries.

## Instruction Architecture

Runtime behavior is driven by the live files under `workspace/`. `src/token_zulip/workspace.py` copies missing template files from the checked-in `workspace/` tree during `token-zulip init`; it does not contain prompt prose or update existing workspace files unless initialization is explicitly run with overwrite behavior. `src/token_zulip/instructions.py` composes role-specific developer instructions, `src/token_zulip/prompt.py` renders role-specific prompts, and `src/token_zulip/codex_adapter.py` runs the native Codex `output_schema` for the reply/session thread and each forked op worker.

Every role starts with `workspace/references/system.md`, then loads only its role system configuration and scoped `AGENTS.md` files. The reply/session thread also loads `workspace/AGENTS.md` and `workspace/references/reply/system.md`; workers load only their worker system configuration plus scoped `AGENTS.md`; scheduled job threads load `workspace/AGENTS.md`, `workspace/references/scheduled_job/system.md`, and scoped `AGENTS.md`.

The composed instruction layers are passed to Codex as `developer_instructions` only when a persistent thread is created, or when a worker fork is created. Existing marked reply/session and job threads are resumed without repeating those instructions.

Use these ownership boundaries to avoid duplicated or conflicting prompt text:

- `workspace/AGENTS.md`: global identity, voice, style, and high-level behavior.
- `workspace/references/system.md`: shared Codex thread contract and structured-output boundaries.
- `workspace/references/reply/`: reply/session agent `user.md`, `system.md`, and `schema.json`.
- `workspace/references/reflections/`: reflections worker agent `user.md`, `system.md`, and `schema.json`.
- `workspace/references/skill/`: skill worker agent `user.md`, `system.md`, and `schema.json`.
- `workspace/references/schedule/`: schedule worker agent `user.md`, `system.md`, and `schema.json`.
- `workspace/references/scheduled_job/`: scheduled job agent `user.md`, `system.md`, and `schema.json`.
- `workspace/instructions/.../AGENTS.md`: human-authored channel/topic/private exceptions or preferences.
- `workspace/reflections/.../REFLECTIONS.md`: non-injected review candidates that may later be promoted manually into instructions, references, skills, or code.

Static model-facing instruction belongs in the Markdown files above. Runtime Python should inject dynamic data sections only, such as current time, mentionable participants, available skill summaries, posted bot updates, and persisted job fields.

Reflections are append-only Markdown candidates, not runtime recall. The reflections worker writes `reflection_ops` to global, channel, or private-chat `REFLECTIONS.md` files. The runtime never injects existing reflections into future prompts and never posts acknowledgement text for reflection-only turns. Raw/session history remains in `workspace/records/stream-*/topic-*/messages.jsonl` or `workspace/records/private-recipient-*/messages.jsonl`, and `turns.jsonl` records applied or skipped reflection operations.

Scheduled tasks follow a Hermes-inspired job model. The schedule worker requests changes through `schedule_ops`; the schedule code path validates and persists jobs under `workspace/schedules/jobs.json`, appends an acknowledgement only after persistence succeeds, and runs due jobs from a scheduler ticker inside `token-zulip run`. Jobs post back only to their originating Zulip topic or private chat. Jobs may be prompt-only or skill-backed; skill-backed jobs store skill names and load `workspace/skills/<name>/SKILL.md` only when the job fires. Reminder jobs may also store zero or more Zulip mention targets that are applied when the job runs.

Skill persistence is owned by the skill worker code path: its forked Codex decision may return `skill_ops` containing a skill name, description, and `SKILL.md` content. TokenZulip validates that request and writes `workspace/skills/<name>/SKILL.md`. Scheduled job threads do not write skills; they only load skill names recorded on the job.

### Thread Context Model

TokenZulip keeps one persistent Codex thread per Zulip conversation as the reply/session thread. On each Zulip update, the runtime resumes or creates that parent thread, then forks worker threads from it for independent operation decisions. Worker forks receive role-specific developer instructions plus only the current message batch and concise role-specific runtime deltas as the explicit run prompt. The Codex `exclude_turns` fork option is used to avoid returning populated turn lists in the fork response; it is not treated as a guarantee that parent model context is absent from the fork.

```text
Zulip event
    |
    v
persistent reply/session thread is resumed or created
    |
    |-- fork --> reflections worker thread --> reflection_ops code path
    |-- fork --> skill worker thread  --> skill_ops code path
    |
    `-- after skill persistence:
        fork --> schedule worker thread --> schedule_ops code path
    |
    `-- reply/session thread receives applied changes and decides visible reply

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
| Reply/session thread | Long-lived per Zulip DM or stream/topic | Previous Codex turns for this Zulip conversation | Current Zulip message batch, pending `posted_bot_update`, and applied deterministic changes from workers | User-visible reply decision only |
| Reflections worker thread | Worker fork | Parent reply/session thread baseline | Current Zulip message batch and reflection scope rules | `reflection_ops` only |
| Skill worker thread | Worker fork | Parent reply/session thread baseline | Current Zulip message batch and available skill summary | `skill_ops` only |
| Schedule worker thread | Worker fork after skill persistence | Parent reply/session thread baseline | Current Zulip message batch, current scheduling time, current jobs here, mentionable Zulip participants, available skill summary, and same-turn skill changes when any | `schedule_ops` only |
| Scheduled job thread | Fresh per job run | None | Job brief, persisted mention targets, loaded skill content, and current time | Scheduled result reply only |

`recent_context` is not injected into Codex prompts. Conversation continuity comes from Codex thread history and forked Codex context; runtime prompts add only the current turn and the deltas each role needs.

Prompt traces are written under each session's `traces/` directory. The canonical conversation history remains `messages.jsonl`, `turns.jsonl`, and scheduled `runs.jsonl`; traces are debug sidecars and can be deleted without changing conversation state. Use `token-zulip traces list`, `token-zulip traces inspect <trace_id>`, and `token-zulip traces cleanup --older-than 30d` for manual inspection and pruning.

TokenZulip injects one narrow continuity record when needed: `posted_bot_update`. A `posted_bot_update` is Sili's actual visible contribution to the Zulip conversation after runtime processing. It includes the final posted reply or dry-run text after deterministic acknowledgements, and it also includes scheduled job output posted back into the origin Zulip conversation. The update is injected once into the next normal reply/session prompt, then marked consumed.

This exists because the visible Zulip message may differ from the reply thread's raw JSON. The reply thread may return `message_to_post: "Done."`, then TokenZulip persists a schedule and posts:

```md
Done.

**Schedule created**
- Name: Camera-ready check
- Trigger: once at 2026-05-01 09:00 Asia/Shanghai
```

The next turn's reply/session prompt receives a compact `posted_bot_update` with that final posted text, so the persistent conversation knows what was actually confirmed without duplicating that text into every worker fork.

### Thread And Schedule Flows

Normal Zulip turn:

```text
Zulip event
  -> normalize message
  -> append messages.jsonl
  -> resume or create persistent reply/session Codex thread for this Zulip topic/private chat
  -> fork op workers from the reply/session thread
       reflections worker -> reflection_ops -> workspace/reflections/.../REFLECTIONS.md
       skill worker  -> skill_ops  -> workspace/skills/<name>/SKILL.md
  -> apply skill results
  -> fork schedule worker with current skill availability
       schedule worker -> schedule_ops -> workspace/schedules/jobs.json
  -> run reply/session thread with applied deterministic changes
       output: message_to_post only
  -> append deterministic acknowledgements for persisted skill and schedule changes
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
       mention_targets: []
       origin: current Zulip topic/private chat
       next_run_at: UTC ISO timestamp
  -> Sili posts a Markdown acknowledgement with name, trigger, next run, and job id
```

Creating a skill-backed scheduled job:

```text
Human conversation asks for a reusable workflow
  -> skill worker returns skill_ops.create/update
  -> skill worker code path writes workspace/skills/<name>/SKILL.md
  -> schedule worker sees the applied skill summary and may return schedule_ops.create with skills: ["<name>"]
  -> schedule worker code path validates the skill exists and stores only the skill name
  -> Sili posts both "Skill saved: ..." and a Markdown schedule acknowledgement
```

Running a due scheduled job:

```text
scheduler ticker wakes every TOKENZULIP_SCHEDULE_TICK_SECONDS
  -> ScheduleStore.get_due_jobs()
  -> for each due job, start a fresh scheduled job Codex thread
       developer_instructions:
         system.md
         scheduled_job/system.md
         scoped AGENTS.md files
       prompt:
         job id/name/time
         job prompt
         persisted mention target list, if any
         loaded SKILL.md contents for job.skills
         output rules
       output: message_to_post only
  -> prepend any missing persisted mentions
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

When a stream/topic or private-chat session already has a marked Codex thread, TokenZulip resumes that thread and sends only the new Zulip message batch plus concise runtime deltas selected for the role. New or legacy unmarked sessions start a fresh Codex thread with composed `developer_instructions`; they do not replay recent Zulip records into the prompt.

Forked workers return reflection, skill, and schedule decisions through separate schemas and code paths; the persistent reply/session thread then receives validated applied skill/schedule changes and returns only `should_reply`, `reply_kind`, `message_to_post`, and confidence. Reflection operations write review candidates only. Schedule operations use a decomposed `schedule_spec`: `once_at` for ISO one-shot times, `once_in` for relative one-shot delays like `30m`, `interval` for recurring durations like `2h`, `cron` for recurring wall-clock schedules like `0 9 * * *`, and `unchanged` for lifecycle operations that do not change timing. Schedule operations may also include multiple `mention_targets`; confirmations use silent full-name mentions, while the due job post uses normal full-name mentions. TokenZulip validates and persists applied skill/schedule changes, appends deterministic acknowledgements for those changes, and then posts any reply.

When schedules are enabled, the listener also runs a background scheduler. Configure it with `TOKENZULIP_SCHEDULES_ENABLED`, `TOKENZULIP_SCHEDULE_TICK_SECONDS`, `TOKENZULIP_SCHEDULE_TIMEZONE`, `TOKENZULIP_SCHEDULE_DEFAULT_TIME`, and `TOKENZULIP_SCHEDULE_RUN_TIMEOUT_SECONDS`. Scheduled job runs start fresh Codex threads from persisted job data, loaded skills, and current time, so scheduled automation history does not pollute the human Zulip conversation thread.

Prompt trace cleanup is configured separately from conversation history. `TOKENZULIP_TRACE_RETENTION_DAYS` sets the age cutoff, `TOKENZULIP_TRACE_AUTO_CLEANUP=true` enables cleanup on startup and then at `TOKENZULIP_TRACE_CLEANUP_INTERVAL_HOURS`. Cleanup deletes only `traces/` sidecars and never deletes `messages.jsonl`, `turns.jsonl`, reflections, schedules, uploads, errors, or scheduled run records.

When live posting is enabled, the bot can show Zulip typing indicators for every processed message. Silent channel decisions stop typing after Codex decides not to reply. Set `TOKENZULIP_TYPING_ENABLED=false` to disable typing indicators.
