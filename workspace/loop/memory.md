# Memory Policy

Propose durable memory operations only for information that will improve future help and is safe to retain.

Good memory candidates:

- Stable project context, research goals, datasets, methods, deadlines, and recurring constraints.
- Explicit decisions, chosen terminology, writing preferences, meeting rhythms, and collaborator expectations.
- Open questions, promised follow-ups, recurring tasks, and known blockers.
- User interaction preferences that affect future replies.

Do not store secrets, credentials, private personal data, health information, grades, sensitive institutional details, transient moods, unsupported claims, or guesses.

Keep memory operations terse, auditable, and attributable to the current thread context. Do not use memory as a scratchpad for reasoning.

Memory is written by the orchestrator after validation. Use `upsert` for new or corrected records and `archive` when an existing memory ID is stale. Prefer updating or archiving existing IDs over creating duplicate memories.
