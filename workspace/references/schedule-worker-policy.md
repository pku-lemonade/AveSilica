# Schedule Worker Policy

The schedule worker decides scheduled task lifecycle operations only. It does not write replies, memory, or skill content.

- Return only `schedule_ops` in the provided schema.
- Use schedule ops proactively for clear natural-language reminders, follow-ups, recurring tasks, cancellations, modifications, listing requests, or run-now requests.
- Direct mention of Sili is not required when schedule intent is clear from context.
- For ambiguous schedule changes, leave schedule ops empty and let the reply thread ask a concise clarification if needed.
- Simple reminders do not need skills. Create prompt-only jobs with `skills: []` when the task is self-contained.
- For skill-backed jobs, put only skill names from the injected Skill Availability context in `skills`; never duplicate full skill content inside a schedule operation.
- Never invent or predict a skill name. If a reusable workflow is clearly required but no available skill fits, leave `schedule_ops` empty so the reply thread can clarify or a later turn can schedule after skill creation succeeds.
- Do not claim that a scheduled task was saved, changed, or removed; TokenZulip validates, persists, and acknowledges applied changes after this worker returns.

Use decomposed `schedule_spec`, not natural-language schedule text:

- `once_at`: exact one-shot wall-clock time with an ISO timestamp in `run_at`.
- `once_in`: relative one-shot delay with a duration like `30m`, `2h`, or `1d`.
- `interval`: recurring duration like `2h`.
- `cron`: recurring wall-clock schedule with a 5-field cron expression like `0 9 * * *`.
- `unchanged`: update/remove/pause/resume/list/run_now operations that do not change timing.

For natural recurring phrases, convert to cron. For example, "every morning at 9" is `{"kind":"cron","run_at":"","duration":"","cron":"0 9 * * *"}`. Never emit phrases such as `every morning Asia/Shanghai` in schedule fields.
