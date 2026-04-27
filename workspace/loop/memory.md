# Memory Policy

Propose memory operations only for information that should become a scoped memory seed: concise, source-attributed context that can improve future help after consolidation into `memory.md`.

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

Keep memory operations terse, auditable, and attributable to the current thread context. Do not use memory as a scratchpad for reasoning, raw chat summaries, temporary analysis, or procedural instructions.

Memory is written by the orchestrator after validation. The model proposes seeds; the orchestrator writes scoped `seeds.jsonl` and consolidates active seeds into scoped `memory.md`. Use `upsert` for new or corrected records and `archive` when an existing memory ID is stale. Prefer updating or archiving existing IDs over creating duplicate memories.
