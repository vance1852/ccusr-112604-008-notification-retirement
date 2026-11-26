"""Built-in offline demonstration over the supplied synthetic fixtures."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from .service import RetirementService, analyze_timeline, load_inventory, load_jsonl

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def executed_workflow() -> dict[str, Any]:
    """Run a small durable workflow: register, migrate, pause, retire, assess rollback."""

    inventory = {
        "routes": [{"route": "legacy-demo", "resources": ["poll-demo"]},
                   {"route": "next", "resources": ["hook-demo"]}],
        "consumers": [{"consumer": "acme", "tenant": "T-demo", "topics": ["alert"]}],
        "tenant_contract": [{"tenant": "T-demo", "legacy_until": 100, "confirm_seconds": 1}],
    }
    with tempfile.TemporaryDirectory(prefix="retirement-demo-") as directory:
        service = RetirementService(Path(directory) / "state.json", inventory)
        service.deliver("T-demo", "m1", "acme", topic="alert", route="legacy-demo", at=1)
        service.receipt("T-demo", "m1", "acme", "applied", route="next", at=2)
        service.confirm("acme", "alert", "*", route="next", at=3)
        duplicate = service.handle({"at": 4, "kind": "probe", "resource": "poll-demo"})
        migration = service.engine.advance_migration(tenant="T-demo", consumer="acme",
                                                      topic="alert", messages="*",
                                                      at=10, execute=True)
        service.save()
        pause = service.engine.advance_decommission(tenant="T-demo",
                                                     resources=["poll-demo"], at=10, execute=True)
        service.save()
        retire = service.engine.advance_decommission(tenant="T-demo",
                                                      resources=["poll-demo"], at=11, execute=True)
        service.save()
        rollback = service.engine.rollback_resource("poll-demo", at=12)
        return {
            "durable_state_written_and_reloaded_by_service": True,
            "probe_report": duplicate,
            "migration": migration,
            "pause": pause,
            "retire": retire,
            "line_state": service.engine.line.state(),
            "rollback": rollback["assessment"],
        }


def run_demo() -> dict[str, Any]:
    inventory = load_inventory(SAMPLES / "inventory.json")
    shift = analyze_timeline(inventory, load_jsonl(SAMPLES / "shift-log.jsonl"))
    clean = analyze_timeline(inventory, load_jsonl(SAMPLES / "clean-delegate.jsonl"))

    def compact(report: dict[str, Any]) -> dict[str, Any]:
        plan = report["plan"]
        return {
            "as_of": plan["as_of"],
            "revision": plan["revision"],
            "backlog_count": plan["backlog_count"],
            "confirmations": [
                {"consumer": c["consumer"], "topic": c["topic"], "scope": c["scope"],
                 "status": c["status"], "formal": c["formal"]}
                for c in plan["confirmations"]
            ],
            "resources": [
                {"resource": r["resource"], "state": r["observed_state"],
                 "next_safe_action": r["next_safe_action"],
                 "blocking_claims": len(r["blocking_claims"])}
                for r in plan["resources"]
            ],
        }

    return {
        "demo": "legacy-notification-retirement",
        "executed_workflow": executed_workflow(),
        "sample_replays": {
            "shift-log": compact(shift),
            "clean-delegate": compact(clean),
        },
        "try_commands": [
            "python -m retirement --state /tmp/retirement/state.json --inventory samples/inventory.json init",
            "python -m retirement --state /tmp/retirement/state.json ingest samples/shift-log.jsonl",
            "python -m retirement --state /tmp/retirement/state.json backlog",
            "python -m retirement --state /tmp/retirement/state.json trace --tenant T-b --message n8",
            "python -m retirement --state /tmp/retirement/state.json plan",
            "python -m retirement --state /tmp/retirement/state.json rollback --resource poll-legacy",
        ],
    }
