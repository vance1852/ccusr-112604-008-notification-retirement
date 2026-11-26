"""Durable JSON-file service wrapper.

The state file contains the full event history and derived projections.  A
service restart reloads it; duplicate reports are recognized by stable event id
or canonical content hash and do not advance revision a second time.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable

from .engine import RetirementEngine, fresh_state
from .errors import ValidationError
from .model import require_mapping


class RetirementService:
    def __init__(self, state_path: str | os.PathLike[str], inventory: dict[str, Any] | None = None):
        self.state_path = Path(state_path)
        if self.state_path.exists():
            with self.state_path.open(encoding="utf-8") as source:
                document = json.load(source)
            if not isinstance(document, dict):
                raise ValidationError("state file must contain a JSON object")
            loaded_inventory = document.get("inventory") or inventory or {}
            state = document.get("state") if "state" in document else document
            self.engine = RetirementEngine(loaded_inventory, state)
        else:
            if inventory is None:
                raise ValidationError("a new retirement state requires an inventory")
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self.engine = RetirementEngine(inventory, fresh_state(inventory))
            self.save()

    @classmethod
    def create_or_open(cls, state_path: str | os.PathLike[str],
                       inventory: dict[str, Any] | None = None,
                       inventory_path: str | os.PathLike[str] | None = None) -> "RetirementService":
        if inventory is None and inventory_path is not None:
            with Path(inventory_path).open(encoding="utf-8") as source:
                inventory = json.load(source)
        return cls(state_path, inventory)

    def save(self) -> None:
        document = self.engine.to_dict()
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=self.state_path.name + ".", suffix=".tmp",
                                        dir=self.state_path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as output:
                json.dump(document, output, ensure_ascii=False, indent=2, sort_keys=False)
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, self.state_path)
        except Exception:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
            raise

    def handle(self, record: dict[str, Any]) -> dict[str, Any]:
        result = self.engine.record(record)
        self.save()
        return result

    def ingest(self, records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        results = []
        for record in records:
            results.append(self.engine.record(record))
        self.save()
        return results

    def ingest_jsonl(self, path: str | os.PathLike[str]) -> list[dict[str, Any]]:
        results = []
        with Path(path).open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, start=1):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    require_mapping(record, f"{path}:{line_number}")
                except (json.JSONDecodeError, ValidationError) as error:
                    raise ValidationError(f"{path}:{line_number} invalid event: {error}") from error
                results.append(self.engine.record(record))
        self.save()
        return results

    # Convenience wrappers persist after each write.
    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self.engine, name)
        if not callable(attribute) or name.startswith("_"):
            return attribute

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = attribute(*args, **kwargs)
            self.save()
            return result

        return wrapper


def load_inventory(path: str | os.PathLike[str]) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as source:
        inventory = json.load(source)
    if not isinstance(inventory, dict) or not inventory:
        raise ValidationError("inventory must be a non-empty JSON object")
    return inventory


def load_jsonl(path: str | os.PathLike[str]) -> list[dict[str, Any]]:
    records = []
    with Path(path).open(encoding="utf-8") as source:
        for line in source:
            if line.strip():
                record = json.loads(line)
                require_mapping(record, "JSONL event")
                records.append(record)
    return records


def analyze_timeline(inventory: dict[str, Any], records: Iterable[dict[str, Any]],
                     *, at: int | None = None) -> dict[str, Any]:
    """One-shot deterministic analysis used by ``replay`` and library callers."""

    engine = RetirementEngine(inventory)
    intake = engine.ingest(records)
    effective_at = engine.as_of(at)
    return {
        "intake": intake,
        "plan": engine.plan_retirement(at=effective_at),
        "backlog": engine.backlog(at=effective_at),
        "receiver_state": engine.receiver.state(),
        "line_state": engine.line.state(),
    }
