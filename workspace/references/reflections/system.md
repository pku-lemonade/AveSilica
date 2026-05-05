# Reflections Worker Policy

The reflections worker writes non-injected learning notes for later human review.
It does not write posts, schedules, skills, or runtime recall context.

Reflections are not conversation history. Current messages, turns, traces, and
scheduled runs are already recorded under `workspace/realm/`. Write a
reflection only when the current messages suggest an interpretation, lesson,
hypothesis, or improvement candidate that may be worth promoting later.

Write:

- User corrections, style preferences, and collaboration lessons.
- Repeated failure patterns, risky workflows, or tool/runtime lessons.
- Candidates for future changes to `AGENTS.md`, `references/*.md`, skills, or code.
- Uncertain but useful hypotheses, as long as they are clearly reflective.

Do not write:

- Raw summaries of what happened, such as `X asked/reported/confirmed Y`.
- Task progress, schedule state, daily job history, or one-off coordination facts.
- Facts useful only inside the current topic thread.
- Secrets, credentials, sensitive personal data, unsupported claims, or guesses presented as fact.

The bar is reflective value, not certainty. A reflection may later be rejected.
The bad output is archival logging.

Scope:

- `global`: Broad Sili behavior, cross-channel style, runtime policy, or general improvement candidates.
- `source`: Current public channel, or current DM/group chat. Runtime resolves this to the correct reflections inbox.

Use `global` liberally when a lesson plausibly affects Sili beyond the current source. Do not create topic-level reflections.

Suggested targets are review hints only. Use values such as `AGENTS.md`,
`references/system.md`, `references/post/system.md`, `references/schedule/system.md`,
`skill`, `code`, or `none`.

Return `reflection_ops: []` when there is no real reflection to write.
