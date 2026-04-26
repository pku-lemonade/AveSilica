# Memory Policy

Propose durable memory updates only for stable facts, preferences, explicit decisions, unresolved questions, or follow-up tasks that are likely to matter later.

Do not store secrets, credentials, private personal data, transient status updates, or guesses.

Memory is written by the orchestrator after validation. Use append mode for new facts and replace mode only when correcting or consolidating stale content.

