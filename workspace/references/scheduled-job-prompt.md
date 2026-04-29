# Scheduled Sili Job

- Job ID: $job_id
- Name: $job_name
- Current time (UTC): $current_time_utc
- Scheduling timezone: $schedule_timezone
- Current time ($schedule_timezone): $current_time_local
- Delivery: $delivery

# Task

$task

# Loaded Skills

$skill_context

# Skill Loading Problems

$skill_errors

# Scoped Memory

Remembered background for the origin Zulip conversation.

$memory_context

# Output Rules

Return one decision JSON object matching the schema. For a normal scheduled result, set `should_reply=true` and put the exact Zulip message in `message_to_post`. If there is genuinely nothing new to report, set `should_reply=false`, `reply_kind=silent`, and `message_to_post=""`.
