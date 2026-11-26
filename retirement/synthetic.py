"""Independent synthetic receiver and legacy-line adapters.

These classes model externally observable infrastructure.  They deliberately do
not make retirement decisions: the coordinator combines their observations with
consumer scope, receipts, contracts, and obligations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .model import normalize_stage


@dataclass
class SyntheticReceiver:
    """A tiny idempotent model of consumer/proxy HTTP endpoints.

    Repeated deliveries increment ``delivery_attempts`` but do not create a
    second logical message or re-run an already applied business operation.
    """

    endpoints: dict[str, dict[str, Any]] = field(default_factory=dict)
    messages: dict[str, dict[str, Any]] = field(default_factory=dict)
    reports: list[dict[str, Any]] = field(default_factory=list)

    def register(self, endpoint: str, consumer: str | None = None, topics: list[str] | None = None) -> None:
        self.endpoints[endpoint] = {
            "endpoint": endpoint,
            "consumer": consumer or endpoint,
            "topics": list(topics or []),
            "state": "healthy",
        }

    @staticmethod
    def _key(tenant: str, message: str) -> str:
        return f"{tenant}/{message}"

    def deliver(self, endpoint: str, tenant: str, message: str, *, topic: str | None = None,
                route: str | None = None, payload: Any = None, at: int | None = None,
                delivery_id: str | None = None) -> dict[str, Any]:
        key = self._key(tenant, message)
        item = self.messages.setdefault(key, {
            "tenant": tenant,
            "message": message,
            "logical_key": key,
            "topic": topic,
            "first_endpoint": endpoint,
            "deliveries": [],
            "received": False,
            "applied": False,
            "applied_result": None,
        })
        duplicate_logical = bool(item["deliveries"])
        attempt = {
            "delivery_id": delivery_id or f"{key}#{len(item['deliveries']) + 1}",
            "endpoint": endpoint,
            "route": route,
            "at": at,
            "duplicate_logical_message": duplicate_logical,
        }
        item["deliveries"].append(attempt)
        report = {
            "kind": "delivery",
            "tenant": tenant,
            "message": message,
            "consumer": self.endpoints.get(endpoint, {}).get("consumer", endpoint),
            "topic": topic,
            "route": route,
            "at": at,
            "delivery_id": attempt["delivery_id"],
            "duplicate_logical_message": duplicate_logical,
        }
        self.reports.append(report)
        return report

    def receipt(self, tenant: str, message: str, *, actor: str, stage: str,
                route: str | None = None, at: int | None = None,
                receipt_id: str | None = None, result: Any = None) -> dict[str, Any]:
        key = self._key(tenant, message)
        item = self.messages.setdefault(key, {"tenant": tenant, "message": message, "deliveries": []})
        normal = normalize_stage(stage)
        idempotent_replay = normal == "applied" and item.get("applied") is True
        if normal == "received":
            item["received"] = True
            item["received_by"] = actor
            item["received_at"] = at
        elif normal == "applied":
            if not item.get("applied"):
                item["applied"] = True
                item["applied_by"] = actor
                item["applied_at"] = at
                item["applied_result"] = result or {"status": "processed"}
        report = {
            "kind": "delivery_receipt",
            "receipt_id": receipt_id,
            "tenant": tenant,
            "message": message,
            "actor": actor,
            "route": route,
            "stage": normal,
            "at": at,
            "idempotent_replay": idempotent_replay,
        }
        self.reports.append(report)
        return report

    def state(self) -> dict[str, Any]:
        return {
            "component": "synthetic_receiver",
            "endpoints": self.endpoints,
            "messages": self.messages,
            "reports": self.reports,
            "report_count": len(self.reports),
        }


_STATE_FROM_EFFECT = {
    "active": "active",
    "enabled": "active",
    "running": "active",
    "restored": "active",
    "rebuilt": "active",
    "still_active": "active",
    "disabled": "disabled",
    "paused": "disabled",
    "stopped": "disabled",
    "not_accepting": "disabled",
    "removed": "removed",
    "deleted": "removed",
    "gone": "removed",
}


@dataclass
class SyntheticLine:
    """Stateful stand-in for legacy polling and shared receipt resources."""

    resources: dict[str, dict[str, Any]] = field(default_factory=dict)
    operations: dict[str, dict[str, Any]] = field(default_factory=dict)
    effects: list[dict[str, Any]] = field(default_factory=list)
    fail_ops: set[str] = field(default_factory=set)
    _op_seq: int = 0

    def add_resource(self, name: str, *, routes: list[str] | None = None, state: str = "active") -> None:
        self.resources[name] = {"resource": name, "routes": list(routes or []), "state": state}

    @classmethod
    def from_inventory(cls, inventory: dict[str, Any]) -> "SyntheticLine":
        line = cls()
        for route in inventory.get("routes", []):
            route_name = route["route"]
            for resource in route.get("resources", []):
                item = line.resources.setdefault(resource, {"resource": resource, "routes": [], "state": "active"})
                if route_name not in item["routes"]:
                    item["routes"].append(route_name)
        return line

    def action(self, resource: str, action: str, *, operation: str | None = None,
               at: int | None = None, tenant: str | None = None) -> dict[str, Any]:
        if resource not in self.resources:
            self.add_resource(resource)
        self._op_seq += 1
        op = operation or f"line-op-{self._op_seq}"
        record = {"kind": "line_action", "operation": op, "resource": resource,
                  "action": action, "at": at, "tenant": tenant}
        self.operations[op] = {"operation": op, "resource": resource, "action": action,
                               "requested_at": at, "status": "requested"}
        return record

    def execute(self, action_record: dict[str, Any], *, fail: bool | None = None) -> dict[str, Any]:
        """Apply an action in the simulator and return its observed effect."""

        op = action_record["operation"]
        resource = action_record["resource"]
        action = action_record["action"]
        current = self.resources.get(resource, {}).get("state", "active")
        forced_fail = op in self.fail_ops if fail is None else fail
        if forced_fail:
            effect_state = "still_active" if current == "active" else current
        elif action in {"disable", "pause", "stop", "close"}:
            effect_state = "disabled"
        elif action in {"retire", "remove", "delete"}:
            if current == "removed":
                effect_state = "removed"
            else:
                effect_state = "removed"
        elif action in {"restore", "enable", "start"}:
            if current == "removed":
                # A plain restore cannot resurrect a confirmed removed resource.
                effect_state = "removed"
            else:
                effect_state = "restored"
        elif action in {"rebuild", "recreate"}:
            effect_state = "rebuilt"
        else:
            effect_state = current
        return self.effect(op, resource=resource, state=effect_state, at=action_record.get("at"))

    def effect(self, operation: str, *, resource: str | None = None, state: str,
               at: int | None = None) -> dict[str, Any]:
        normal = _STATE_FROM_EFFECT.get(str(state).lower(), str(state))
        op = self.operations.get(operation)
        resource_name = resource or (op or {}).get("resource")
        if not resource_name:
            raise ValueError("line_effect requires resource for an unknown operation")
        if resource_name not in self.resources:
            self.add_resource(resource_name, state=normal)
        self.resources[resource_name]["state"] = normal
        effect = {"kind": "line_effect", "operation": operation, "resource": resource_name,
                  "state": state, "normalized_state": normal, "at": at}
        self.effects.append(effect)
        if op:
            op["status"] = "observed"
            op["effect_state"] = state
            op["normalized_effect_state"] = normal
        return effect

    def state(self) -> dict[str, Any]:
        return {"component": "synthetic_line", "resources": self.resources,
                "operations": self.operations, "effects": self.effects,
                "op_seq": self._op_seq, "effect_count": len(self.effects)}


# Short aliases for callers/tests that use the domain role names.
Receiver = SyntheticReceiver
Line = SyntheticLine
NotificationReceiver = SyntheticReceiver
LegacyLine = SyntheticLine
