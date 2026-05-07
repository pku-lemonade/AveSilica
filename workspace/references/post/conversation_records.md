# Conversation Records

This Codex thread is attached to one Zulip conversation. Codex cwd remains the workspace root.

- Conversation type: $conversation_type
- Stable identity: $conversation_identity
- Current display: $conversation_display
- Records directory: `$records_dir`

Use this directory when the current Zulip message needs prior local context:

- `session.json`: persisted conversation metadata and Codex thread state.
- `messages.jsonl`: Zulip message records for this conversation.
- `turns.jsonl`: prior Silica decisions and applied operations.
- `posted_bot_updates.jsonl`: recent or pending visible posts from Silica.
- `uploads/`: downloaded attachments referenced by message records, when present.
- `traces/`: per-role prompt and output traces, when present.

Treat the records directory as the stable workspace anchor. Topic text can change, so rely on stream id, topic hash, private recipient key, and the current session directory for identity.
