from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Iterator
from uuid import uuid4

from .models import utc_now_iso


TOKEN_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)


def _elapsed_ms(start: float, end: float | None = None) -> int:
    stop = perf_counter() if end is None else end
    return max(0, int(round((stop - start) * 1000)))


def _value(source: Any, name: str) -> Any:
    if isinstance(source, dict):
        return source.get(name)
    return getattr(source, name, None)


def _int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _usage_breakdown(value: Any) -> dict[str, int] | None:
    if value is None:
        return None
    record = {name: _int_value(_value(value, name)) for name in TOKEN_FIELDS}
    return record if any(record.values()) else record


def codex_token_usage_record(raw_result: Any) -> dict[str, Any] | None:
    usage = _value(raw_result, "usage")
    if usage is None:
        return None

    record: dict[str, Any] = {}
    last = _usage_breakdown(_value(usage, "last"))
    total = _usage_breakdown(_value(usage, "total"))
    if last is not None:
        record["last"] = last
    if total is not None:
        record["total"] = total

    model_context_window = _value(usage, "model_context_window")
    if model_context_window is not None:
        record["model_context_window"] = _int_value(model_context_window)

    return record or None


def _token_sum(calls: list[dict[str, Any]], bucket: str) -> dict[str, int] | None:
    record = {name: 0 for name in TOKEN_FIELDS}
    found = False
    for call in calls:
        tokens = call.get("tokens")
        if not isinstance(tokens, dict):
            continue
        usage = tokens.get(bucket)
        if not isinstance(usage, dict):
            continue
        found = True
        for name in TOKEN_FIELDS:
            record[name] += _int_value(usage.get(name))
    return record if found else None


def _token_usage_sum(calls: list[dict[str, Any]]) -> dict[str, Any] | None:
    record: dict[str, Any] = {}
    for bucket in ["last", "total"]:
        usage = _token_sum(calls, bucket)
        if usage is not None:
            record[bucket] = usage
    windows = []
    for call in calls:
        tokens = call.get("tokens")
        if isinstance(tokens, dict) and tokens.get("model_context_window") is not None:
            windows.append(_int_value(tokens.get("model_context_window")))
    if windows:
        record["model_context_window"] = max(windows)
    return record or None


def _durations_by_name(records: list[dict[str, Any]]) -> dict[str, int]:
    durations: dict[str, int] = {}
    for record in records:
        name = str(record.get("name") or "")
        if not name:
            continue
        durations[name] = durations.get(name, 0) + _int_value(record.get("duration_ms"))
    return durations


def _counts_by_field(records: list[dict[str, Any]], field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        name = str(record.get(field_name) or "")
        if not name:
            continue
        counts[name] = counts.get(name, 0) + 1
    return counts


def _dict_records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def codex_calls_summary(calls: Any) -> dict[str, Any]:
    records = _dict_records(calls)
    api_call_count = sum(_int_value(record.get("api_call_count")) for record in records)
    duration_ms = sum(_int_value(record.get("duration_ms")) for record in records)
    summary: dict[str, Any] = {
        "call_count": len(records),
        "api_call_count": api_call_count,
        "duration_ms": duration_ms,
        "by_role": _counts_by_field(records, "role"),
        "by_operation": _counts_by_field(records, "operation"),
    }
    tokens = _token_usage_sum(records)
    if tokens:
        summary["tokens"] = tokens
    return summary


def timing_e2e_stats_record(timing: Any) -> dict[str, Any] | None:
    if not isinstance(timing, dict):
        return None
    phases = _dict_records(timing.get("phases"))
    codex_calls = _dict_records(timing.get("codex_calls"))
    record: dict[str, Any] = {
        "record_type": "e2e",
        "telemetry_id": str(timing.get("telemetry_id") or ""),
        "source": timing.get("source"),
        "started_at": timing.get("started_at"),
        "finished_at": timing.get("finished_at"),
        "duration_ms": _int_value(timing.get("duration_ms")),
        "overhead_ms": _int_value(timing.get("overhead_ms")),
        "status": str(timing.get("status") or "ok"),
        "breakdown": {
            "phases": phases,
            "by_phase_ms": _durations_by_name(phases),
            "non_phase_overhead_ms": _int_value(timing.get("overhead_ms")),
        },
        "codex": codex_calls_summary(codex_calls),
    }
    return {key: value for key, value in record.items() if value is not None}


def timing_codex_call_stats_records(timing: Any) -> list[dict[str, Any]]:
    if not isinstance(timing, dict):
        return []
    telemetry_id = str(timing.get("telemetry_id") or "")
    records: list[dict[str, Any]] = []
    for index, call in enumerate(_dict_records(timing.get("codex_calls"))):
        record = dict(call)
        record["record_type"] = "codex_call"
        record["telemetry_id"] = telemetry_id
        record["call_index"] = index
        records.append(record)
    return records


@dataclass
class TimedPhase:
    name: str
    started_at: str
    started_offset_ms: int
    _started_monotonic: float = field(repr=False)
    finished_at: str | None = None
    duration_ms: int | None = None
    status: str = "ok"
    error: str | None = None

    def finish(self, *, status: str = "ok", error: str | None = None) -> None:
        self.finished_at = utc_now_iso()
        self.duration_ms = _elapsed_ms(self._started_monotonic)
        self.status = status
        self.error = error

    @property
    def finished_offset_ms(self) -> int | None:
        if self.duration_ms is None:
            return None
        return self.started_offset_ms + self.duration_ms

    def to_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "name": self.name,
            "started_at": self.started_at,
            "started_offset_ms": self.started_offset_ms,
            "duration_ms": self.duration_ms,
            "status": self.status,
        }
        if self.finished_at:
            record["finished_at"] = self.finished_at
        if self.finished_offset_ms is not None:
            record["finished_offset_ms"] = self.finished_offset_ms
        if self.error:
            record["error"] = self.error
        return record


class CodexCallTimer:
    def __init__(
        self,
        *,
        operation: str,
        model: str,
        effort: str | None,
        model_call: bool,
        input_thread_id: str | None = None,
        parent_thread_id: str | None = None,
    ) -> None:
        self.operation = operation
        self.model = model
        self.effort = effort
        self.model_call = model_call
        self.input_thread_id = input_thread_id
        self.parent_thread_id = parent_thread_id
        self.started_at = utc_now_iso()
        self._started_monotonic = perf_counter()
        self._phases: list[TimedPhase] = []

    @contextmanager
    def phase(self, name: str) -> Iterator[TimedPhase]:
        phase = TimedPhase(
            name=name,
            started_at=utc_now_iso(),
            started_offset_ms=_elapsed_ms(self._started_monotonic),
            _started_monotonic=perf_counter(),
        )
        try:
            yield phase
        except Exception as exc:
            phase.finish(status="error", error=str(exc))
            raise
        else:
            phase.finish()
        finally:
            self._phases.append(phase)

    def finish(
        self,
        *,
        raw_result: Any = None,
        resolved_thread_id: str | None = None,
        status: str = "ok",
        error: str | None = None,
    ) -> dict[str, Any]:
        duration_ms = _elapsed_ms(self._started_monotonic)
        phases = [phase.to_record() for phase in self._phases]
        phase_total = sum(int(phase.get("duration_ms") or 0) for phase in phases)
        record: dict[str, Any] = {
            "operation": self.operation,
            "model": self.model,
            "effort": self.effort,
            "api_call_count": 1 if self.model_call else 0,
            "started_at": self.started_at,
            "finished_at": utc_now_iso(),
            "duration_ms": duration_ms,
            "overhead_ms": max(0, duration_ms - phase_total),
            "status": status,
        }
        if self.input_thread_id is not None:
            record["input_thread_id"] = self.input_thread_id
        if self.parent_thread_id is not None:
            record["parent_thread_id"] = self.parent_thread_id
        if resolved_thread_id is not None:
            record["thread_id"] = resolved_thread_id
        if phases:
            record["phases"] = phases
        if error:
            record["error"] = error

        tokens = codex_token_usage_record(raw_result)
        if tokens:
            record["tokens"] = tokens
        return {key: value for key, value in record.items() if value is not None}


class TurnTelemetry:
    def __init__(self, *, source: str) -> None:
        self.telemetry_id = uuid4().hex
        self.source = source
        self.started_at = utc_now_iso()
        self._started_monotonic = perf_counter()
        self._phases: list[TimedPhase] = []
        self._codex_calls: list[dict[str, Any]] = []

    @contextmanager
    def phase(self, name: str) -> Iterator[TimedPhase]:
        phase = TimedPhase(
            name=name,
            started_at=utc_now_iso(),
            started_offset_ms=_elapsed_ms(self._started_monotonic),
            _started_monotonic=perf_counter(),
        )
        try:
            yield phase
        except Exception as exc:
            phase.finish(status="error", error=str(exc))
            raise
        else:
            phase.finish()
        finally:
            self._phases.append(phase)

    def add_codex_result(
        self,
        result: Any,
        *,
        role: str,
        phase: TimedPhase | None = None,
    ) -> None:
        stats = getattr(result, "stats", None)
        if not isinstance(stats, dict):
            return
        record = dict(stats)
        record["role"] = role
        if phase is not None:
            record["turn_phase"] = phase.name
            record["turn_offset_ms"] = phase.started_offset_ms
            if phase.finished_offset_ms is not None:
                record["turn_finished_offset_ms"] = phase.finished_offset_ms
        self._codex_calls.append(record)

    def finish(self, *, status: str = "ok") -> dict[str, Any]:
        duration_ms = _elapsed_ms(self._started_monotonic)
        phases = [phase.to_record() for phase in self._phases]
        phase_total = sum(int(phase.get("duration_ms") or 0) for phase in phases)
        record = {
            "telemetry_id": self.telemetry_id,
            "source": self.source,
            "started_at": self.started_at,
            "finished_at": utc_now_iso(),
            "duration_ms": duration_ms,
            "overhead_ms": max(0, duration_ms - phase_total),
            "status": status,
            "phases": phases,
            "codex_calls": self._codex_calls,
        }
        e2e_record = timing_e2e_stats_record(record)
        if e2e_record is not None:
            record["breakdown"] = e2e_record["breakdown"]
            record["codex"] = e2e_record["codex"]
        return record
