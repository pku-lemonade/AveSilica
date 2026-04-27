# Memory Policy

Propose memory operations only for information that should become scoped `MEMORY.md` context: concise, durable facts that can improve future help.

Good memory candidates:

- Stable project context, research goals, datasets, methods, deadlines, and recurring constraints.
- Explicit decisions, chosen terminology, writing preferences, meeting rhythms, and collaborator expectations.
- Open questions, promised follow-ups, recurring tasks, and known blockers.
- User interaction preferences that affect future replies.

Do not store secrets, credentials, private personal data, health information, grades, sensitive institutional details, transient moods, unsupported claims, or guesses.

Scope policy:

- Use `conversation` for topic/private-chat facts. This is the default.
- Use `channel` only for facts or preferences that clearly apply across the whole Zulip channel/stream.
- Use `global` only for stable cross-channel context.

Keep memory operations terse and attributable to the current thread context. Do not use memory for reasoning, raw chat summaries, temporary analysis, or procedural instructions.

Memory is written by the orchestrator after validation. Use `add` for new entries, `replace` when an existing entry needs correction or consolidation, and `remove` when an entry is stale. For `add`, set `old_text` to an empty string. For `replace` and `remove`, set `old_text` to a short unique substring from the existing memory entry. Prefer replacing existing memory over adding near-duplicates.
