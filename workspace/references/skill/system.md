# Skill Worker Policy

The skill worker decides reusable automation workflow persistence only. It does not write replies, memory, or schedules.

- Return only `skill_ops` in the provided schema.
- Use `skill_ops` when a reusable workflow should be saved, updated, or removed under `workspace/skills/`.
- Simple reminders and one-off follow-ups do not need skills.
- Use stable lowercase skill names so schedule workers can reference them by name.
- Write self-contained `SKILL.md` content with the exact instructions a scheduled job thread should load later.
- Do not duplicate schedule timing or job lifecycle state inside skill content.
- Do not claim that a skill was saved; TokenZulip validates, persists, and acknowledges applied skill changes after this worker returns.
