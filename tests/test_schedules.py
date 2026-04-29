from __future__ import annotations

import asyncio
import json
from pathlib import Path

from token_zulip.codex_adapter import CodexRunResult, CodexTurnWithForksResult, CodexWorkerSpec
from token_zulip.config import BotConfig
from token_zulip.instructions import InstructionLoader
from token_zulip.loop import AgentLoop
from token_zulip.memory import MemoryStore
from token_zulip.models import NormalizedMessage, ScheduleOperation, ScheduleSpec
from token_zulip.schedules import ScheduleStore, parse_schedule, parse_schedule_spec
from token_zulip.skills import SkillStore
from token_zulip.storage import WorkspaceStorage
from token_zulip.workspace import initialize_workspace


def _config(workspace: Path, *, post_replies: bool = True) -> BotConfig:
    return BotConfig(
        workspace_dir=workspace,
        zulip_config_file=None,
        realm_id="realm",
        bot_email="bot@example.com",
        bot_user_id=99,
        bot_aliases=("Silica", "Sili"),
        codex_model="gpt-5.4",
        codex_reasoning_effort=None,
        codex_cwd=workspace,
        codex_sandbox="read-only",
        codex_approval_policy="never",
        max_recent_messages=20,
        queue_limit=8,
        worker_count=2,
        instruction_max_bytes=96_000,
        upload_max_bytes=25_000_000,
        post_replies=post_replies,
        listen_all_public_streams=True,
        typing_enabled=False,
        typing_refresh_seconds=8.0,
        schedule_timezone="Asia/Shanghai",
    )


def _message(message_id: int, content: str = "schedule this") -> NormalizedMessage:
    return NormalizedMessage(
        realm_id="realm",
        message_id=message_id,
        stream_id=10,
        stream="Engineering",
        stream_slug="engineering",
        topic="Launch",
        topic_hash="topic123",
        sender_email="alice@example.com",
        sender_full_name="Alice",
        sender_id=1,
        content=content,
        timestamp=None,
        received_at="now",
        raw={},
    )


class PayloadCodex:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.prompts: list[str] = []
        self.thread_ids: list[str | None] = []
        self.developer_instructions: list[str | None] = []

    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
        output_schema_path: Path | None = None,
    ) -> CodexRunResult:
        self.prompts.append(prompt)
        self.thread_ids.append(thread_id)
        self.developer_instructions.append(developer_instructions)
        return CodexRunResult(raw_text=json.dumps(self.payload), thread_id=f"thread-{len(self.prompts)}")

    async def run_turn_with_forks(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None,
        main_output_schema_path: Path,
        worker_specs: list[CodexWorkerSpec],
    ) -> CodexTurnWithForksResult:
        self.worker_prompts = {spec.kind: spec.prompt for spec in worker_specs}
        self.worker_developer_instructions = {spec.kind: spec.developer_instructions for spec in worker_specs}
        main = await self.run_decision(prompt, thread_id, developer_instructions=developer_instructions)
        workers: dict[str, CodexRunResult] = {}
        for spec in worker_specs:
            if spec.kind == "memory":
                worker_payload = {"memory_ops": self.payload.get("memory_ops", [])}
            elif spec.kind == "skill":
                worker_payload = {"skill_ops": self.payload.get("skill_ops", [])}
            elif spec.kind == "schedule":
                worker_payload = {"schedule_ops": self.payload.get("schedule_ops", [])}
            else:
                worker_payload = {}
            workers[spec.kind] = CodexRunResult(
                raw_text=json.dumps(worker_payload),
                thread_id=f"thread-{len(self.prompts)}-{spec.kind}",
            )
        return CodexTurnWithForksResult(main=main, workers=workers, worker_errors={})


class FakePoster:
    def __init__(self) -> None:
        self.posts: list[dict[str, str]] = []

    async def post_reply(self, message: NormalizedMessage, content: str) -> dict[str, str]:
        self.posts.append({"topic": message.topic, "content": content})
        return {"result": "success", "id": 123}


def _silent_payload() -> dict[str, object]:
    return {
        "should_reply": False,
        "reply_kind": "silent",
        "message_to_post": "",
        "memory_ops": [],
        "schedule_ops": [],
        "skill_ops": [],
        "confidence": 0.9,
    }


def test_parse_schedule_uses_configured_timezone_for_naive_iso():
    schedule = parse_schedule("2030-01-02T09:00:00", "Asia/Shanghai")

    assert schedule["kind"] == "once"
    assert schedule["run_at"] == "2030-01-02T01:00:00+00:00"
    assert "Asia" not in schedule["run_at"]


def test_parse_schedule_spec_supports_decomposed_kinds():
    once_at = parse_schedule_spec(ScheduleSpec(kind="once_at", run_at="2030-01-02T09:00:00"), "Asia/Shanghai")
    once_in = parse_schedule_spec(ScheduleSpec(kind="once_in", duration="30m"), "Asia/Shanghai")
    interval = parse_schedule_spec(ScheduleSpec(kind="interval", duration="2h"), "Asia/Shanghai")
    cron = parse_schedule_spec(ScheduleSpec(kind="cron", cron="0 9 * * *"), "Asia/Shanghai")

    assert once_at["kind"] == "once"
    assert once_at["run_at"] == "2030-01-02T01:00:00+00:00"
    assert once_in["kind"] == "once"
    assert interval["kind"] == "interval"
    assert interval["minutes"] == 120
    assert cron["kind"] == "cron"
    assert cron["expr"] == "0 9 * * *"


def test_active_schema_requires_decomposed_schedule_spec():
    schema_path = Path(__file__).parent.parent / "workspace" / "references" / "schedule-decision-schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schedule_props = schema["properties"]["schedule_ops"]["items"]["properties"]
    required = schema["properties"]["schedule_ops"]["items"]["required"]

    assert "schedule" not in schedule_props
    assert "schedule_spec" in required
    assert schedule_props["schedule_spec"]["properties"]["kind"]["enum"] == [
        "unchanged",
        "once_at",
        "once_in",
        "interval",
        "cron",
    ]


def test_schedule_create_validates_referenced_skills(tmp_path):
    initialize_workspace(tmp_path)
    skills = SkillStore(tmp_path / "skills")
    skills.write_skill(
        type(
            "Op",
            (),
            {
                "action": "create",
                "name": "weekly-digest",
                "description": "Use for weekly digests.",
                "content": "Summarize the topic concisely.",
            },
        )()
    )
    store = ScheduleStore(tmp_path, timezone_name="Asia/Shanghai")

    result = store.create_job(
        _message(1),
        ScheduleOperation(
            action="create",
            name="Weekly digest",
            prompt="Prepare a digest.",
            schedule="2030-01-02T09:00:00",
            skills=("weekly-digest",),
        ),
        skills=skills,
    )

    assert result["status"] == "applied"
    assert store.load_jobs()[0]["skills"] == ["weekly-digest"]


def test_skill_and_schedule_ops_are_acknowledged_after_persistence(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        payload = {
            "should_reply": False,
            "reply_kind": "silent",
            "message_to_post": "",
            "memory_ops": [],
            "skill_ops": [
                {
                    "action": "create",
                    "name": "weekly-digest",
                    "description": "Use for weekly digests.",
                    "content": "Summarize the topic concisely.",
                }
            ],
            "schedule_ops": [
                {
                    "action": "create",
                    "job_id": "",
                    "name": "Weekly digest",
                    "match": "",
                    "prompt": "Prepare a digest.",
                    "schedule_spec": {
                        "kind": "once_at",
                        "run_at": "2030-01-02T09:00:00",
                        "duration": "",
                        "cron": "",
                    },
                    "repeat": None,
                    "skills": ["weekly-digest"],
                    "confidence": 0.9,
                }
            ],
            "confidence": 0.9,
        }
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=PayloadCodex(payload),
            zulip=poster,
        )

        await bot._handle_message(_message(1))

        assert (tmp_path / "skills" / "weekly-digest" / "SKILL.md").exists()
        assert (tmp_path / "schedules" / "jobs.json").exists()
        assert "Skill saved: weekly-digest" in poster.posts[0]["content"]
        assert "**Schedule created**" in poster.posts[0]["content"]
        assert "- Name: Weekly digest" in poster.posts[0]["content"]
        assert "- Trigger: once at 2030-01-02 09:00 Asia/Shanghai" in poster.posts[0]["content"]
        assert "- Next run: 2030-01-02 09:00 Asia/Shanghai" in poster.posts[0]["content"]

    asyncio.run(scenario())


def test_daily_morning_request_uses_cron_schedule_spec(tmp_path):
    store = ScheduleStore(tmp_path, timezone_name="Asia/Shanghai")
    op = ScheduleOperation.from_mapping(
        {
            "action": "create",
            "job_id": "",
            "name": "Daily paper digest",
            "match": "",
            "prompt": "Find one paper and summarize it.",
            "schedule_spec": {
                "kind": "cron",
                "run_at": "",
                "duration": "",
                "cron": "0 9 * * *",
            },
            "repeat": None,
            "skills": [],
            "confidence": 0.9,
        }
    )

    result = store.create_job(_message(1), op)

    assert result["status"] == "applied"
    job = store.load_jobs()[0]
    assert job["schedule"]["kind"] == "cron"
    assert job["schedule"]["expr"] == "0 9 * * *"
    assert job["schedule"]["timezone"] == "Asia/Shanghai"


def test_due_scheduled_job_loads_skill_in_separate_thread_and_posts(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        skills = SkillStore(tmp_path / "skills")
        skills.write_skill(
            type(
                "Op",
                (),
                {
                    "action": "create",
                    "name": "weekly-digest",
                    "description": "Use for weekly digests.",
                    "content": "Summarize the topic concisely.",
                },
            )()
        )
        schedules = ScheduleStore(tmp_path, timezone_name="Asia/Shanghai")
        created = schedules.create_job(
            _message(1),
            ScheduleOperation(
                action="create",
                name="Weekly digest",
                prompt="Prepare a digest.",
                schedule="2030-01-02T09:00:00",
                skills=("weekly-digest",),
            ),
            skills=skills,
        )
        schedules.trigger_job(_message(1), ScheduleOperation(action="run_now", job_id=created["job_id"]))
        payload = {
            **_silent_payload(),
            "should_reply": True,
            "reply_kind": "report",
            "message_to_post": "Digest done.",
        }
        codex = PayloadCodex(payload)
        poster = FakePoster()
        storage = WorkspaceStorage(tmp_path)
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=poster,
            skills=skills,
            schedules=schedules,
        )

        assert await bot.run_schedules_once() == 1

        assert "## Skill: weekly-digest" in codex.prompts[0]
        assert codex.thread_ids == [None]
        assert "Scheduled Job Policy" in (codex.developer_instructions[0] or "")
        assert poster.posts == [{"topic": "Launch", "content": "Digest done."}]
        pending = storage.read_pending_posted_bot_updates(_message(1).session_key)
        assert pending[-1]["source"] == "scheduled_job"
        assert pending[-1]["content"] == "Digest done."
        assert pending[-1]["job_id"] == created["job_id"]
        job = schedules.load_jobs()[0]
        assert job["codex_thread_id"] == "thread-1"
        assert job["last_status"] == "ok"

    asyncio.run(scenario())
