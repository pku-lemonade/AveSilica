# Scheduled Sili Job

- Job ID: $job_id
- Name: $job_name
- Current time (UTC): $current_time_utc
- Scheduling timezone: $schedule_timezone
- Current time ($schedule_timezone): $current_time_local
- Delivery: $delivery

# Task

$task

# Persisted Mention Targets

$mention_targets

$loaded_skills_section

$skill_errors_section

# Output Rules

Return one decision JSON object matching the schema. For a normal scheduled result, set `should_post=true` and put each Zulip message in `messages_to_post` in delivery order. Use a one-item list for one normal message and `[]` when there is genuinely nothing new to report.
