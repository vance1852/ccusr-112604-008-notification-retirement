"""Core retirement coordinator.

The engine is deterministic and storage-free: give it an inventory and event
dictionaries, then query the resulting decisions.  :class:`RetirementService`
adds durable JSON state and restart semantics.
"""

from __future__ import annotations

import copy
from typing import Any, Iterable

from .errors import NotFoundError, RevisionConflict, ValidationError
from .model import (
    APPLIED_STAGES,
    canonical_json,
    content_hash,
    environment_name,
    is_applied_stage,
    is_production_environment,
    logical_message_key,
    normalize_kind,
    normalize_stage,
    parse_reference_scope,
    require_mapping,
    route_generation,
    scope_key,
    subscription_key,
)
from .synthetic import SyntheticLine, SyntheticReceiver


def fresh_state(inventory: dict[str, Any] | None = None) -> dict[str, Any]:
    inventory = inventory or {}
    line = SyntheticLine.from_inventory(inventory)
    state: dict[str, Any] = {
        "version": 1,
        "revision": 0,
        "events": [],
        "event_hashes": {},
        "event_ids": {},
        "idempotency": {},
        "registry": {"consumers": {}, "delegations": []},
        "tenants": {},
        "subscriptions": {},
        "messages": {},
        "obligations": {},
        "tickets": {},
        "delivery_index": {},
        "receipts": [],
        "confirmations": [],
        "assumptions": [],
        "commitments": {},
        "batches": {},
        "operations": {},
        "resources": {},
        "snapshots": [],
        "takeovers": [],
        "decisions": [],
        "receiver": SyntheticReceiver().state(),
        "line": line.state(),
    }
    _seed_inventory(state, inventory)
    return state


def _seed_inventory(state: dict[str, Any], inventory: dict[str, Any]) -> None:
    for contract in inventory.get("tenant_contract", []):
        tenant = contract["tenant"]
        state["tenants"][tenant] = {
            "tenant": tenant,
            "legacy_until": contract.get("legacy_until"),
            "confirm_seconds": contract.get("confirm_seconds"),
            "cancelled_at": None,
        }

    for consumer in inventory.get("consumers", []):
        name = consumer["consumer"]
        tenant = consumer.get("tenant")
        topics = list(consumer.get("topics", []))
        state["registry"]["consumers"][name] = {
            "consumer": name,
            "tenant": tenant,
            "topics": topics,
            "active": True,
            "source": "inventory",
        }
        if tenant and tenant not in state["tenants"]:
            state["tenants"][tenant] = {"tenant": tenant, "legacy_until": None,
                                        "confirm_seconds": None, "cancelled_at": None}
        for topic in topics:
            key = subscription_key(tenant or "", name, topic)
            state["subscriptions"].setdefault(key, {
                "key": key,
                "tenant": tenant,
                "consumer": name,
                "topic": topic,
                "state": "active",
                "intake": "legacy",
                "disposition": None,
                "target_route": None,
                "batch_id": None,
                "decided_at": None,
                "effective_at": None,
                "observed_routes": [],
            })

    for route in inventory.get("routes", []):
        route_name = route["route"]
        for resource_name in route.get("resources", []):
            resource = state["resources"].setdefault(resource_name, {
                "resource": resource_name,
                "routes": [],
                # The local synthetic line starts known-active from topology.
                # External replays can still override this only via line_effect.
                "observed_state": "active",
                "raw_state": "inventory_default_active",
                "last_effect_seq": None,
                "snapshot_ids": [],
            })
            if route_name not in resource["routes"]:
                resource["routes"].append(route_name)

    # Shared means used by more than one route in the topology.  Cross-tenant
    # sharing on one route is computed dynamically by resource_plan().
    for resource in state["resources"].values():
        resource["shared_route"] = len(resource["routes"]) > 1


class RetirementEngine:
    """Apply source records and answer retirement/routing/trace questions."""

    def __init__(self, inventory: dict[str, Any] | None = None, state: dict[str, Any] | None = None):
        self.inventory = inventory or {}
        if state is None:
            self.state = fresh_state(self.inventory)
        else:
            self.state = state
            self.state.setdefault("version", 1)
            self.state.setdefault("revision", 0)
            # Older/minimal persisted states are completed with seed data.
            if not self.state.get("resources") and self.inventory:
                _seed_inventory(self.state, self.inventory)
        self.receiver = SyntheticReceiver()
        self.line = SyntheticLine.from_inventory(self.inventory)
        self._hydrate_simulators()
        for consumer in self.state.get("registry", {}).get("consumers", {}).values():
            self.receiver.register(consumer["consumer"], consumer["consumer"], consumer.get("topics", []))

    # ------------------------------------------------------------------
    # Construction and persistence
    # ------------------------------------------------------------------
    @classmethod
    def from_dict(cls, doc: dict[str, Any]) -> "RetirementEngine":
        require_mapping(doc, "state document")
        inventory = doc.get("inventory") or {}
        state = doc.get("state") if "state" in doc else doc
        return cls(inventory=inventory, state=state)

    def to_dict(self) -> dict[str, Any]:
        self._persist_simulator_observations()
        return {"version": 1, "inventory": self.inventory, "state": self.state}

    def _hydrate_simulators(self) -> None:
        recv_state = self.state.get("receiver") or {}
        self.receiver.endpoints = recv_state.get("endpoints", {})
        self.receiver.messages = recv_state.get("messages", {})
        self.receiver.reports = recv_state.get("reports", [])
        line_state = self.state.get("line") or {}
        self.line.resources = line_state.get("resources", {})
        self.line.operations = line_state.get("operations", {})
        self.line.effects = line_state.get("effects", [])
        op_seq = line_state.get("op_seq")
        if isinstance(op_seq, int):
            self.line._op_seq = op_seq
        else:
            numeric = [int(op.split("-")[-1]) for op in self.line.operations
                        if str(op).split("-")[-1].isdigit()]
            self.line._op_seq = max(numeric, default=0)

    def _persist_simulator_observations(self) -> None:
        self.state["receiver"] = self.receiver.state()
        self.state["line"] = self.line.state()

    def clone(self) -> "RetirementEngine":
        return RetirementEngine(self.inventory, copy.deepcopy(self.state))

    def prefix(self, at: int) -> "RetirementEngine":
        """Rebuild only facts that had arrived by ``at``."""

        raw_events = []
        for event in self.state["events"]:
            arrived = event.get("received_at")
            if arrived is not None and int(arrived) > int(at):
                continue
            raw_events.append({key: value for key, value in event.items() if not key.startswith("_")})
        prefix_engine = RetirementEngine(self.inventory)
        prefix_engine.ingest(raw_events)
        return prefix_engine

    def _engine_at(self, at: int | None) -> "RetirementEngine":
        if at is None:
            return self
        at_int = self._time(at, "at") if not isinstance(at, int) else at
        return self if at_int >= self.as_of() else self.prefix(at_int)

    # ------------------------------------------------------------------
    # Event intake and idempotency
    # ------------------------------------------------------------------
    def ingest(self, records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        return [self.record(copy.deepcopy(r)) for r in records]

    def record(self, raw: dict[str, Any]) -> dict[str, Any]:
        raw = require_mapping(raw, "event")
        event_hash = content_hash(raw)
        raw_event_id = raw.get("event_id") or raw.get("report_id") or raw.get("id")
        event_id = None if raw_event_id is None else str(raw_event_id)
        idempotency_key = raw.get("idempotency_key")
        if idempotency_key and idempotency_key in self.state["idempotency"]:
            return {**self.state["idempotency"][idempotency_key].get("result", {}),
                    "duplicate": True, "idempotent_replay": True}
        if event_hash in self.state["event_hashes"]:
            seq = self.state["event_hashes"][event_hash]
            return {"duplicate": True, "seq": seq, "event_id": event_id, "hash": event_hash}
        if event_id is not None and event_id in self.state["event_ids"]:
            seq = self.state["event_ids"][event_id]
            return {"duplicate": True, "seq": seq, "event_id": event_id,
                    "hash": self.state["events"][seq - 1].get("hash")}

        seq = len(self.state["events"]) + 1
        kind = normalize_kind(raw.get("kind"))
        at = raw.get("at")
        at_int = None if at is None else self._time(at, "at")
        fact_value = self._first(raw, ("occurred_at", "happened_at", "fact_at", "event_at", "time", "timestamp"))
        fact_at = self._time(fact_value, "fact time") if fact_value is not None else at_int
        event = copy.deepcopy(raw)
        event.update({
            "_seq": seq,
            "_hash": event_hash,
            "kind": kind,
            "received_at": at_int,
            "fact_at": fact_at,
            "time_basis": "record_field" if fact_value is not None else "at_arrival",
        })
        self.state["events"].append(event)
        self.state["event_hashes"][event_hash] = seq
        if event_id is not None:
            self.state["event_ids"][event_id] = seq
        self.state["revision"] = int(self.state.get("revision", 0)) + 1
        self._apply(event)
        self._persist_simulator_observations()
        return {"duplicate": False, "seq": seq, "event_id": event_id, "hash": event_hash}

    @staticmethod
    def _first(record: dict[str, Any], names: tuple[str, ...]) -> Any:
        for name in names:
            if name in record and record[name] is not None:
                return record[name]
        return None

    @staticmethod
    def _time(value: Any, field: str) -> int:
        if isinstance(value, bool):
            raise ValidationError(f"{field} must be an integer, not boolean")
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str) and value.strip().lstrip("-").isdigit():
            return int(value.strip())
        raise ValidationError(f"{field} must be an integer timestamp in this offline build: {value!r}")

    def as_of(self, at: int | None = None) -> int:
        if at is not None:
            return self._time(at, "at")
        times = [e.get("received_at") for e in self.state["events"] if e.get("received_at") is not None]
        return max(times, default=0)

    # ------------------------------------------------------------------
    # Command helpers.  Commands are facts too, so retries use event IDs.
    # ------------------------------------------------------------------
    def command(self, kind: str, payload: dict[str, Any], *, at: int | None = None,
                idempotency_key: str | None = None) -> dict[str, Any]:
        if idempotency_key and idempotency_key in self.state["idempotency"]:
            return {"duplicate": True, **self.state["idempotency"][idempotency_key]}
        event: dict[str, Any] = {"kind": kind}
        event.update(payload)
        event["at"] = self.as_of(at)
        if idempotency_key:
            event["event_id"] = f"cmd:{idempotency_key}"
        result = self.record(event)
        output = {"event": event, "result": result}
        if idempotency_key:
            self.state["idempotency"][idempotency_key] = output
        return output

    def expected_revision(self, expected: int | None) -> None:
        if expected is not None and int(expected) != int(self.state["revision"]):
            raise RevisionConflict(int(expected), int(self.state["revision"]))

    # ------------------------------------------------------------------
    # Event dispatch
    # ------------------------------------------------------------------
    def _apply(self, event: dict[str, Any]) -> None:
        kind = event["kind"]
        handler = getattr(self, f"_on_{kind}", None)
        if handler is None:
            event["projection"] = "unhandled"
            return
        handler(event)

    def _on_probe_observation(self, event: dict[str, Any]) -> None:
        # Liveness and integration-environment pings are deliberately not
        # receipts and never complete an obligation.
        event["evidence_effect"] = "ignored_probe_not_business_receipt"

    def _on_consumer_registered(self, e: dict[str, Any]) -> None:
        name = e.get("consumer") or e.get("name")
        if not name:
            raise ValidationError("consumer registration requires consumer")
        tenant = e.get("tenant")
        topics = [str(x) for x in (e.get("topics") or e.get("notification_kinds") or [])]
        current = self.state["registry"]["consumers"].get(name)
        if current:
            if tenant and current.get("tenant") and tenant != current["tenant"]:
                current.setdefault("tenant_conflicts", []).append(tenant)
            current["topics"] = sorted(set(current.get("topics", [])) | set(topics))
            current["active"] = bool(e.get("active", current.get("active", True)))
        else:
            self.state["registry"]["consumers"][name] = {
                "consumer": name, "tenant": tenant, "topics": sorted(set(topics)),
                "active": bool(e.get("active", True)), "source": "event",
            }
        if tenant:
            self.state["tenants"].setdefault(tenant, {"tenant": tenant, "legacy_until": None,
                                                       "confirm_seconds": None, "cancelled_at": None})
            for topic in topics:
                key = subscription_key(tenant, name, topic)
                self.state["subscriptions"].setdefault(key, {
                    "key": key, "tenant": tenant, "consumer": name, "topic": topic,
                    "state": "active", "intake": "legacy", "disposition": None,
                    "target_route": None, "batch_id": None, "decided_at": None,
                    "effective_at": None, "observed_routes": [],
                })
        registered = self.state["registry"]["consumers"][name]
        self.receiver.register(str(name), str(name), registered.get("topics", []))

    def _on_delegation(self, e: dict[str, Any]) -> None:
        consumer = e.get("consumer") or e.get("principal")
        proxy = e.get("proxy") or e.get("delegate") or e.get("agent")
        if not consumer or not proxy:
            raise ValidationError("delegation requires consumer and proxy")
        start = self._time(self._first(e, ("start", "from", "effective_from", "start_at")), "delegation.start") \
            if self._first(e, ("start", "from", "effective_from", "start_at")) is not None else e["fact_at"]
        until_value = self._first(e, ("until", "end", "to", "expires_at", "end_at"))
        until = self._time(until_value, "delegation.until") if until_value is not None else None
        delegation_id = e.get("delegation_id") or e.get("id") or f"delegation-{e['_seq']}"
        record = {
            "delegation_id": delegation_id,
            "consumer": consumer,
            "proxy": proxy,
            "topic": e.get("topic") or e.get("notification_kind") or "*",
            "start": start,
            "until": until,
            "end_inclusive": bool(e.get("end_inclusive", e.get("inclusive", False))),
            "revoked_at": None,
            "revocation_effective_at": None,
            "source_seq": e["_seq"],
        }
        self.state["registry"]["delegations"].append(record)

    def _on_delegation_revoked(self, e: dict[str, Any]) -> None:
        did = e.get("delegation_id") or e.get("id")
        consumer = e.get("consumer") or e.get("principal")
        proxy = e.get("proxy") or e.get("delegate")
        topic = e.get("topic")
        effective_value = self._first(e, ("effective_at", "from", "start"))
        effective = self._time(effective_value, "revocation.effective_at") if effective_value is not None else e["fact_at"]
        matches = []
        for d in self.state["registry"]["delegations"]:
            if did and d["delegation_id"] == did:
                matches.append(d)
            elif not did and d["consumer"] == consumer and d["proxy"] == proxy and (
                    topic is None or d["topic"] in {topic, "*"}):
                matches.append(d)
        if not matches:
            e["projection"] = "revocation_without_matching_delegation"
            return
        for d in matches:
            d["revoked_at"] = e["received_at"]
            d["revocation_effective_at"] = effective

    def _on_tenant_cancelled(self, e: dict[str, Any]) -> None:
        tenant = e.get("tenant")
        if not tenant:
            raise ValidationError("tenant cancellation requires tenant")
        tenant_state = self.state["tenants"].setdefault(
            tenant, {"tenant": tenant, "legacy_until": None, "confirm_seconds": None, "cancelled_at": None})
        tenant_state["cancelled_at"] = e["fact_at"]

    # ------------------------------------------------------------------
    # Deliveries, obligations, receipts
    # ------------------------------------------------------------------
    def _resolve_topic(self, e: dict[str, Any]) -> str:
        topic = e.get("topic") or e.get("notification_kind") or e.get("kind_name")
        if topic:
            return str(topic)
        consumer = e.get("consumer")
        record = self.state["registry"]["consumers"].get(consumer or "")
        topics = (record or {}).get("topics") or []
        return topics[0] if len(topics) == 1 else "*"

    def _ticket_id(self, e: dict[str, Any], tenant: str, message: str) -> str:
        explicit = self._first(e, ("ticket", "ticket_id", "case_id", "work_order", "work_order_id"))
        return str(explicit) if explicit is not None else f"ticket:{tenant}/{message}"

    def _ensure_obligation(self, tenant: str, consumer: str, topic: str, message: str,
                           ticket: str | None = None) -> dict[str, Any]:
        key = scope_key(tenant, consumer or "", topic or "*", str(message))
        obl = self.state["obligations"].get(key)
        if obl is None:
            ticket_id = ticket or f"ticket:{tenant}/{message}"
            obl = {
                "key": key,
                "tenant": tenant,
                "consumer": consumer,
                "topic": topic,
                "message": str(message),
                "ticket": ticket_id,
                "delivery_ids": [],
                "routes": [],
                "legacy_routes": [],
                "receipt_ids": [],
                "valid_applied_receipt_ids": [],
                "assumption_ids": [],
                "custodians": [],
                "created_seq": len(self.state["events"]) + 1,
            }
            self.state["obligations"][key] = obl
            mk = logical_message_key(tenant, str(message))
            self.state["messages"].setdefault(mk, {"tenant": tenant, "message": str(message),
                                                    "obligation_keys": [], "ticket_ids": []})
            if key not in self.state["messages"][mk]["obligation_keys"]:
                self.state["messages"][mk]["obligation_keys"].append(key)
            if ticket_id not in self.state["messages"][mk]["ticket_ids"]:
                self.state["messages"][mk]["ticket_ids"].append(ticket_id)
            self.state["tickets"].setdefault(ticket_id, {"ticket": ticket_id, "obligation_keys": [],
                                                          "takeovers": []})
            if key not in self.state["tickets"][ticket_id]["obligation_keys"]:
                self.state["tickets"][ticket_id]["obligation_keys"].append(key)
        if ticket and obl.get("ticket") != ticket:
            # An explicit work order can aggregate multiple obligations.
            self.state["tickets"].setdefault(ticket, {"ticket": ticket, "obligation_keys": [], "takeovers": []})
            if key not in self.state["tickets"][ticket]["obligation_keys"]:
                self.state["tickets"][ticket]["obligation_keys"].append(key)
        return obl

    def _on_delivery(self, e: dict[str, Any]) -> None:
        tenant = e.get("tenant")
        message = e.get("message") if e.get("message") is not None else e.get("message_id")
        consumer = e.get("consumer") or e.get("delivered_to")
        if not tenant or message is None or not consumer:
            raise ValidationError("delivery requires tenant, message, and consumer")
        topic = self._resolve_topic(e)
        ticket = self._ticket_id(e, str(tenant), str(message))
        obl = self._ensure_obligation(str(tenant), str(consumer), topic, str(message), ticket)
        delivery_id = str(e.get("delivery_id") or e.get("attempt_id") or f"delivery-{e['_seq']}")
        route = e.get("route")
        delivery = {
            "delivery_id": delivery_id,
            "seq": e["_seq"],
            "at": e["received_at"],
            "fact_at": e["fact_at"],
            "tenant": tenant,
            "message": str(message),
            "consumer": consumer,
            "topic": topic,
            "route": route,
            "ticket": ticket,
        }
        self.state["delivery_index"][delivery_id] = delivery
        if delivery_id not in obl["delivery_ids"]:
            obl["delivery_ids"].append(delivery_id)
        if route and route not in obl["routes"]:
            obl["routes"].append(route)
            if route_generation(route, e) == "legacy" and route not in obl["legacy_routes"]:
                obl["legacy_routes"].append(route)
        sub_key = subscription_key(str(tenant), str(consumer), topic)
        sub = self.state["subscriptions"].get(sub_key)
        if sub and route and route not in sub["observed_routes"]:
            sub["observed_routes"].append(route)

    def _matching_obligation_keys(self, e: dict[str, Any]) -> list[str]:
        if e.get("delivery_id") and e["delivery_id"] in self.state["delivery_index"]:
            d = self.state["delivery_index"][e["delivery_id"]]
            return [scope_key(d["tenant"], d["consumer"], d["topic"], d["message"])]
        tenant = e.get("tenant")
        message = e.get("message") if e.get("message") is not None else e.get("message_id")
        if tenant is None or message is None:
            return []
        mk = logical_message_key(str(tenant), str(message))
        keys = list(self.state["messages"].get(mk, {}).get("obligation_keys", []))
        consumer = e.get("consumer") or e.get("on_behalf_of") or e.get("principal")
        topic = e.get("topic")
        result = []
        for key in keys:
            obl = self.state["obligations"][key]
            if consumer and obl["consumer"] != consumer:
                continue
            if topic and obl["topic"] not in {topic, "*"} and topic != "*":
                continue
            result.append(key)
        return result

    def _active_delegation(self, consumer: str, proxy: str, topic: str, at: int) -> dict[str, Any] | None:
        candidates = []
        for d in self.state["registry"]["delegations"]:
            if d["consumer"] != consumer or d["proxy"] != proxy:
                continue
            if d["topic"] not in {"*", topic}:
                continue
            if at < d["start"]:
                continue
            end = d["revocation_effective_at"] if d["revocation_effective_at"] is not None else d["until"]
            if end is not None:
                if d["end_inclusive"]:
                    if at > end:
                        continue
                elif at >= end:
                    continue
            candidates.append(d)
        return max(candidates, key=lambda d: d["source_seq"], default=None)

    def _matching_assumptions(self, obl: dict[str, Any], at: int) -> list[dict[str, Any]]:
        result = []
        for assumption in self.state["assumptions"]:
            if assumption.get("consumer") != obl.get("consumer"):
                continue
            if assumption.get("tenant") and obl.get("tenant") and assumption["tenant"] != obl["tenant"]:
                continue
            if assumption.get("topic") not in {None, "*", obl.get("topic")}:
                continue
            scope = assumption.get("scope", [])
            if scope != "*" and obl.get("message") not in scope:
                continue
            effective = int(assumption.get("effective_at") or assumption.get("at") or 0)
            if effective <= at:
                result.append(assumption)
        return result

    def _scoped_transfer_route(self, obl: dict[str, Any], at: int) -> str | None:
        sub = self.state["subscriptions"].get(
            subscription_key(obl.get("tenant") or "", obl.get("consumer") or "", obl.get("topic") or "*"))
        if not sub:
            return None
        for action in sub.get("scope_actions", []):
            effective = int(action.get("effective_at") or action.get("decided_at") or 0)
            if effective > at:
                continue
            scope = action.get("scope")
            if scope == "*" or obl.get("message") in (scope or []):
                return action.get("target_route") or "next"
        return None

    def _logical_applied_coverage(self, obl: dict[str, Any], at: int) -> dict[str, Any] | None:
        """Find production applied coverage for the same tenant/message identity."""

        mk = logical_message_key(str(obl["tenant"]), str(obl["message"]))
        message = self.state["messages"].get(mk)
        if not message:
            return None
        receipt_index = self._receipt_index()
        for other_key in message.get("obligation_keys", []):
            other = self.state["obligations"].get(other_key)
            if not other:
                continue
            for receipt_id in other.get("receipt_ids", []):
                receipt = receipt_index.get(receipt_id)
                if not receipt or not self.receipt_is_valid(receipt):
                    continue
                if receipt["stage"] != "applied" or not receipt.get("production_evidence", True):
                    continue
                if int(receipt.get("received_at") or 0) > at:
                    continue
                # Fan-out to distinct consumers remains distinct work.  Cover a
                # redelivered/re-keyed scope when the responsible consumer or
                # acting business identity is the same.
                if other.get("consumer") == obl.get("consumer") or receipt.get("actor") == obl.get("consumer"):
                    return receipt
        return None

    def _authorize_receipt(self, receipt: dict[str, Any], key: str) -> dict[str, Any]:
        obl = self.state["obligations"][key]
        actor = receipt.get("actor") or receipt.get("consumer")
        at = receipt.get("fact_at") if receipt.get("fact_at") is not None else receipt.get("received_at") or 0
        if actor == obl["consumer"]:
            return {"authorized": True, "basis": "principal", "delegation_id": None}
        delegation = self._active_delegation(obl["consumer"], str(actor), obl["topic"], int(at))
        if delegation:
            return {"authorized": True, "basis": "delegation", "delegation_id": delegation["delegation_id"]}
        for custody in obl.get("custodians", []):
            if custody.get("proxy") == actor and custody.get("established_seq", 10**12) < receipt["seq"]:
                return {"authorized": True, "basis": "custody_after_expiry",
                        "delegation_id": custody.get("delegation_id")}
        return {"authorized": False, "basis": "actor_not_authorized", "delegation_id": None}

    def _on_delivery_receipt(self, e: dict[str, Any]) -> None:
        if not e.get("tenant") or e.get("message") is None or not e.get("actor"):
            raise ValidationError("delivery_receipt requires tenant, message, and actor")
        stage = normalize_stage(e.get("stage"))
        receipt_id = str(e.get("receipt_id") or e.get("id") or f"receipt-{e['_seq']}")
        keys = self._matching_obligation_keys(e)
        ticket_id = self._ticket_id(e, str(e.get("tenant")), str(e.get("message")))
        if not keys:
            standalone_consumer = e.get("consumer") or e.get("principal") or e.get("on_behalf_of")
            if standalone_consumer:
                topic = e.get("topic") or "*"
                self._ensure_obligation(str(e["tenant"]), str(standalone_consumer),
                                        str(topic), str(e["message"]), ticket_id)
                keys = self._matching_obligation_keys(e)
        ticket_state = self.state["tickets"].setdefault(
            ticket_id, {"ticket": ticket_id, "obligation_keys": [], "takeovers": []})
        for key in keys:
            if key not in ticket_state["obligation_keys"]:
                ticket_state["obligation_keys"].append(key)
        receipt = {
            "receipt_id": receipt_id,
            "seq": e["_seq"],
            "event_hash": e["_hash"],
            "received_at": e["received_at"],
            "fact_at": e["fact_at"],
            "tenant": e.get("tenant"),
            "message": str(e.get("message")),
            "actor": e.get("actor"),
            "consumer_scope": e.get("consumer") or e.get("on_behalf_of"),
            "topic": e.get("topic"),
            "route": e.get("route"),
            "requested_stage": e.get("stage"),
            "stage": stage,
            "valid": False,
            "coverage": [],
            "authorizations": {},
            "corrected": False,
            "correction": None,
            "environment": environment_name(e),
            "production_evidence": is_production_environment(e),
        }
        if not keys:
            receipt["valid"] = False
            receipt["authorizations"]["_unmatched"] = {
                "authorized": False, "basis": "no_matching_delivery_or_obligation"}
        for key in keys:
            auth = self._authorize_receipt(receipt, key)
            receipt["authorizations"][key] = auth
            if auth["authorized"]:
                receipt["coverage"].append(key)
                receipt["valid"] = True
        self.state["receipts"].append(receipt)

        for key in keys:
            obl = self.state["obligations"][key]
            if receipt_id not in obl["receipt_ids"]:
                obl["receipt_ids"].append(receipt_id)
            if key in receipt["coverage"]:
                auth = receipt["authorizations"][key]
                # A valid delegate received/applied event establishes durable
                # custody of that work even if the mandate later expires.
                if auth["basis"] in {"delegation", "custody_after_expiry"} and stage in {"received", "applied"}:
                    if not any(c.get("proxy") == receipt["actor"] for c in obl["custodians"]):
                        obl["custodians"].append({
                            "proxy": receipt["actor"],
                            "delegation_id": auth.get("delegation_id"),
                            "established_by_receipt": receipt_id,
                            "established_seq": receipt["seq"],
                            "established_at": receipt["fact_at"],
                            "stage": stage,
                        })
                if stage == "applied" and receipt_id not in obl["valid_applied_receipt_ids"]:
                    obl["valid_applied_receipt_ids"].append(receipt_id)

    def _on_receipt_correction(self, e: dict[str, Any]) -> None:
        target_id = e.get("receipt_id") or e.get("target_receipt_id") or e.get("id")
        target_hash = e.get("target_hash")
        for receipt in self.state["receipts"]:
            if (target_id and receipt["receipt_id"] == target_id) or (
                    target_hash and receipt["event_hash"] == target_hash):
                receipt["corrected"] = True
                receipt["correction"] = {
                    "seq": e["_seq"], "at": e["received_at"],
                    "valid": bool(e.get("valid", False)),
                    "stage": normalize_stage(e["stage"]) if e.get("stage") else receipt["stage"],
                    "reason": e.get("reason"),
                }
                if e.get("stage"):
                    receipt["stage"] = normalize_stage(e["stage"])
                receipt["valid"] = bool(e.get("valid", False))
                if not receipt["valid"] or receipt["stage"] not in {"received", "applied"}:
                    for key in receipt.get("coverage", []):
                        obl = self.state["obligations"].get(key)
                        if not obl:
                            continue
                        if receipt["receipt_id"] in obl["valid_applied_receipt_ids"]:
                            obl["valid_applied_receipt_ids"].remove(receipt["receipt_id"])
                        obl["custodians"] = [
                            custody for custody in obl.get("custodians", [])
                            if custody.get("established_by_receipt") != receipt["receipt_id"]
                        ]
        self._recompute_receipt_authorizations()

    def _recompute_receipt_authorizations(self) -> None:
        """Replay authorization after a correction can invalidate custody."""

        for obl in self.state["obligations"].values():
            obl["custodians"] = []
            obl["valid_applied_receipt_ids"] = []
        for receipt in sorted(self.state["receipts"], key=lambda item: item["seq"]):
            correction = receipt.get("correction") if receipt.get("corrected") else None
            effective_stage = normalize_stage(correction.get("stage")) if correction and correction.get("stage") else receipt["stage"]
            lookup = dict(receipt)
            if receipt.get("consumer_scope"):
                lookup["consumer"] = receipt["consumer_scope"]
            keys = self._matching_obligation_keys(lookup)
            receipt["coverage"] = []
            receipt["authorizations"] = {}
            for key in keys:
                auth = self._authorize_receipt({**receipt, "stage": effective_stage}, key)
                receipt["authorizations"][key] = auth
                authoritative_valid = correction.get("valid") if correction else None
                valid = auth["authorized"] if authoritative_valid is None else bool(authoritative_valid)
                if valid:
                    receipt["coverage"].append(key)
            if correction:
                receipt["valid"] = bool(correction.get("valid", False))
                if correction.get("stage"):
                    receipt["stage"] = normalize_stage(correction["stage"])
            else:
                receipt["valid"] = bool(receipt["coverage"])
            if not receipt["valid"]:
                continue
            for key in receipt["coverage"]:
                obl = self.state["obligations"].get(key)
                if not obl:
                    continue
                auth = receipt["authorizations"].get(key, {})
                if auth.get("basis") in {"delegation", "custody_after_expiry"} and effective_stage in {"received", "applied"}:
                    if not any(c.get("proxy") == receipt["actor"] for c in obl["custodians"]):
                        obl["custodians"].append({
                            "proxy": receipt["actor"],
                            "delegation_id": auth.get("delegation_id"),
                            "established_by_receipt": receipt["receipt_id"],
                            "established_seq": receipt["seq"],
                            "established_at": receipt.get("fact_at"),
                            "stage": effective_stage,
                        })
                if effective_stage == "applied" and receipt["receipt_id"] not in obl["valid_applied_receipt_ids"]:
                    obl["valid_applied_receipt_ids"].append(receipt["receipt_id"])

    # ------------------------------------------------------------------
    # Migration scope, assumptions, subscription batches
    # ------------------------------------------------------------------
    def _tenant_for_consumer(self, consumer: str | None, tenant: str | None = None) -> str | None:
        if tenant:
            return tenant
        record = self.state["registry"]["consumers"].get(consumer or "")
        return (record or {}).get("tenant")

    def _on_new_confirm(self, e: dict[str, Any]) -> None:
        consumer = e.get("consumer")
        if not consumer:
            raise ValidationError("new_confirm requires consumer")
        tenant = self._tenant_for_consumer(consumer, e.get("tenant"))
        scope = parse_reference_scope(e)
        confirmation = {
            "confirmation_id": str(e.get("confirmation_id") or e.get("id") or f"confirm-{e['_seq']}"),
            "seq": e["_seq"],
            "declared_at": e["received_at"],
            "fact_at": e["fact_at"],
            "tenant": tenant,
            "consumer": consumer,
            "topic": e.get("topic") or "*",
            "route": e.get("route") or "next",
            "scope": scope,
            "note": e.get("note"),
            "environment": environment_name(e),
            "production": is_production_environment(e),
        }
        self.state["confirmations"].append(confirmation)
        # A confirmation is also an explicit consumer take-over of the named
        # message scope.  Completion still requires an applied receipt.
        self._add_assumption({
            "assumption_id": f"assume-from-{confirmation['confirmation_id']}",
            "seq": e["_seq"],
            "at": e["received_at"],
            "effective_at": e["fact_at"],
            "tenant": tenant,
            "consumer": consumer,
            "topic": confirmation["topic"],
            "scope": scope,
            "to_route": confirmation["route"],
            "basis": "new_confirm",
            "source_confirmation_id": confirmation["confirmation_id"],
        })

    def _add_assumption(self, a: dict[str, Any]) -> None:
        self.state["assumptions"].append(a)
        scope = a.get("scope", [])
        if scope == "*":
            # Topic-wide continuation attaches to every known matching scope;
            # future messages are covered by the subscription disposition.
            for obl in self.state["obligations"].values():
                if a.get("consumer") and obl["consumer"] != a["consumer"]:
                    continue
                if a.get("topic") not in (None, "*") and obl["topic"] not in {a["topic"], "*"}:
                    continue
                if a["assumption_id"] not in obl["assumption_ids"]:
                    obl["assumption_ids"].append(a["assumption_id"])
            return
        for message in scope:
            tenant = a.get("tenant")
            consumer = a.get("consumer")
            topic = a.get("topic") or "*"
            if tenant and consumer:
                obl = self._ensure_obligation(str(tenant), str(consumer), str(topic), str(message))
                if a["assumption_id"] not in obl["assumption_ids"]:
                    obl["assumption_ids"].append(a["assumption_id"])

    def _on_assumption(self, e: dict[str, Any]) -> None:
        consumer = e.get("consumer")
        if not consumer:
            raise ValidationError("assumption requires consumer")
        tenant = self._tenant_for_consumer(consumer, e.get("tenant"))
        scope = parse_reference_scope(e)
        if not scope:
            raise ValidationError("assumption requires explicit reference/message scope or all_messages=true")
        effective_value = self._first(e, ("effective_at", "from", "start"))
        effective_at = self._time(effective_value, "assumption.effective_at") if effective_value is not None else e["fact_at"]
        a = {
            "assumption_id": str(e.get("assumption_id") or e.get("id") or f"assume-{e['_seq']}"),
            "seq": e["_seq"],
            "at": e["received_at"],
            "effective_at": effective_at,
            "tenant": tenant,
            "consumer": consumer,
            "topic": e.get("topic") or "*",
            "scope": scope,
            "to_route": e.get("to_route") or e.get("route") or "next",
            "basis": e.get("basis") or "explicit_assumption",
        }
        self._add_assumption(a)

    def _on_commitment(self, e: dict[str, Any]) -> None:
        tenant = e.get("tenant")
        consumer = e.get("consumer")
        message = e.get("message")
        if message is None:
            message = e.get("commitment_id") if e.get("commitment_id") is not None else e.get("reference")
        if not tenant or message is None:
            raise ValidationError("commitment requires tenant and message/commitment id")
        topic = self._resolve_topic(e) if consumer else (e.get("topic") or "*")
        ticket = self._ticket_id(e, str(tenant), str(message))
        key = scope_key(str(tenant), str(consumer or ""), str(topic), str(message))
        record = {
            "key": key, "tenant": tenant, "consumer": consumer, "topic": topic,
            "message": str(message), "ticket": ticket,
            "status": e.get("status", "promised"), "due_at": e.get("due_at"),
            "note": e.get("note"), "seq": e["_seq"],
        }
        self.state["commitments"][key] = record
        if consumer:
            self._ensure_obligation(str(tenant), str(consumer), str(topic), str(message), ticket)

    def _on_subscription_command(self, e: dict[str, Any]) -> None:
        batch_id = str(e.get("batch_id") or e.get("id") or f"batch-{e['_seq']}")
        action = e.get("action") or e.get("disposition") or "stop"
        tenant = e.get("tenant")
        consumer = e.get("consumer")
        topic = e.get("topic")
        key = e.get("subscription_key")
        keys = [key] if key else self._select_subscription_keys(tenant, consumer, topic)
        batch = self.state["batches"].setdefault(batch_id, {
            "batch_id": batch_id, "action": action, "scope": e.get("scope"),
            "items": [], "created_seq": e["_seq"], "at": e["received_at"],
            "idempotency_key": e.get("idempotency_key"),
        })
        for sub_key in keys:
            sub = self.state["subscriptions"].get(sub_key)
            if not sub:
                continue
            disposition = "transferred" if action in {"transfer", "redirect", "migrate"} else (
                "retained" if action == "retain" else "stopped")
            message_scope = parse_reference_scope(e)
            scoped_transfer = (
                disposition == "transferred"
                and message_scope not in ([], None)
                and message_scope != "*"
            )
            if scoped_transfer:
                scope_action = {
                    "batch_id": batch_id,
                    "disposition": "transferred",
                    "scope": message_scope,
                    "target_route": e.get("target_route") or e.get("to_route") or "next",
                    "decided_at": e["received_at"],
                    "effective_at": e.get("effective_at", e["received_at"]),
                    "event_seq": e["_seq"],
                }
                sub.setdefault("scope_actions", []).append(scope_action)
                if sub.get("disposition") is None:
                    sub["state"] = "partially_redirected"
                assumption_id = f"assume-batch-{batch_id}-{len(sub.get('scope_actions', []))}"
                self._add_assumption({
                    "assumption_id": assumption_id,
                    "seq": e["_seq"],
                    "at": e["received_at"],
                    "effective_at": scope_action["effective_at"],
                    "tenant": sub.get("tenant"),
                    "consumer": sub.get("consumer"),
                    "topic": sub.get("topic"),
                    "scope": message_scope,
                    "to_route": scope_action["target_route"],
                    "basis": "scoped_subscription_batch_transfer",
                    "batch_id": batch_id,
                })
            else:
                sub["disposition"] = disposition
                sub["state"] = "accepting_stopped" if action in {"stop", "close", "pause"} else (
                    "redirected" if disposition == "transferred" else sub["state"])
                if action in {"close_intake", "close_new_intake"}:
                    sub["intake"] = "closed_to_new_legacy"
                if disposition == "transferred":
                    sub["state"] = "redirected"
                    sub["target_route"] = e.get("target_route") or e.get("to_route") or "next"
                if disposition == "retained":
                    sub["state"] = "retained"
                    sub["target_route"] = None
                sub["batch_id"] = batch_id
                sub["decided_at"] = e["received_at"]
                sub["effective_at"] = e.get("effective_at", e["received_at"])
            if sub_key not in batch["items"]:
                batch["items"].append(sub_key)

    def _on_batch_command(self, e: dict[str, Any]) -> None:
        batch_id = str(e.get("batch_id") or e.get("id") or f"batch-{e['_seq']}")
        items = e.get("commands") or e.get("items") or e.get("subscriptions") or []
        if isinstance(items, dict):
            items = [items]
        if not isinstance(items, list):
            raise ValidationError("batch_command items must be a list")
        for index, item in enumerate(items, start=1):
            if isinstance(item, str):
                item = {"subscription_key": item, "action": e.get("action", "stop")}
            if not isinstance(item, dict):
                continue
            command = dict(item)
            command.setdefault("kind", "subscription_command")
            command.setdefault("action", e.get("action", "stop"))
            command["batch_id"] = batch_id
            command.setdefault("at", e.get("received_at"))
            command.setdefault("_seq", e["_seq"])
            command.setdefault("_hash", e.get("_hash"))
            # Reuse the same projection logic without appending nested events.
            self._on_subscription_command(command)

    def _select_subscription_keys(self, tenant: str | None = None, consumer: str | None = None,
                                  topic: str | None = None) -> list[str]:
        result = []
        for key, sub in self.state["subscriptions"].items():
            if tenant and sub.get("tenant") != tenant:
                continue
            if consumer and sub.get("consumer") != consumer:
                continue
            if topic and sub.get("topic") != topic:
                continue
            result.append(key)
        return sorted(result)

    # ------------------------------------------------------------------
    # Line effects, snapshots, takeover
    # ------------------------------------------------------------------
    def _ensure_resource(self, name: str, e: dict[str, Any] | None = None) -> dict[str, Any]:
        resource = self.state["resources"].setdefault(name, {
            "resource": name, "routes": [], "observed_state": "unknown", "raw_state": None,
            "last_effect_seq": None, "snapshot_ids": [],
        })
        if e and e.get("route") and e["route"] not in resource["routes"]:
            resource["routes"].append(e["route"])
            resource["shared_route"] = len(resource["routes"]) > 1
        return resource

    def _on_line_action(self, e: dict[str, Any]) -> None:
        resource_name = e.get("resource")
        action = e.get("action")
        if not resource_name or not action:
            raise ValidationError("line_action requires resource and action")
        op = str(e.get("operation") or e.get("operation_id") or f"op-{e['_seq']}")
        self._ensure_resource(resource_name, e)
        self.state["operations"][op] = {
            "operation": op, "resource": resource_name, "action": action,
            "tenant": e.get("tenant"), "requested_at": e["received_at"],
            "request_seq": e["_seq"], "status": "requested", "effects": [],
            "idempotency_key": e.get("idempotency_key"),
        }
        # Feed the independent synthetic line if this is a command generated
        # for it.  Ingested action records never invent an effect.
        if e.get("_simulate") is True:
            self.line.action(resource_name, action, operation=op, at=e["received_at"], tenant=e.get("tenant"))
            effect = self.line.execute({"operation": op, "resource": resource_name,
                                        "action": action, "at": e["received_at"]},
                                       fail=e.get("_force_fail"))
            effect_event = dict(effect)
            effect_event["at"] = e["received_at"]
            self.record(effect_event)

    def _on_line_effect(self, e: dict[str, Any]) -> None:
        resource_name = e.get("resource")
        raw_state = e.get("state")
        if not resource_name or raw_state is None:
            raise ValidationError("line_effect requires resource and state")
        resource = self._ensure_resource(resource_name, e)
        normalized = {
            "active": "active", "enabled": "active", "running": "active",
            "restored": "active", "rebuilt": "active", "still_active": "active",
            "disabled": "disabled", "paused": "disabled", "stopped": "disabled",
            "removed": "removed", "deleted": "removed", "gone": "removed",
        }.get(str(raw_state).lower(), str(raw_state))
        resource["observed_state"] = normalized
        resource["raw_state"] = raw_state
        resource["last_effect_seq"] = e["_seq"]
        if normalized == "removed":
            resource["ever_confirmed_removed"] = True
            resource["removed_effect_seq"] = e["_seq"]
        elif resource.get("ever_confirmed_removed") and normalized in {"active", "disabled"}:
            # A successful active/disabled effect after a confirmed removal can
            # only represent an explicit rebuild/recreation, not a plain undo.
            resource["rebuilt_after_removal"] = True
            resource["rebuilt_effect_seq"] = e["_seq"]
        op = e.get("operation") or e.get("operation_id")
        if op and op in self.state["operations"]:
            operation = self.state["operations"][op]
            operation["status"] = "effect_observed"
            operation["last_effect"] = {"state": raw_state, "normalized_state": normalized,
                                        "seq": e["_seq"], "at": e["received_at"]}
            operation["effects"].append(e["_seq"])
        # Keep independent simulator observation aligned when replaying logs.
        if op:
            self.line.operations.setdefault(op, {"operation": op, "resource": resource_name})
        self.line.resources.setdefault(resource_name, {"resource": resource_name, "routes": resource["routes"]})
        self.line.resources[resource_name]["state"] = normalized

    def _on_snapshot(self, e: dict[str, Any]) -> None:
        snapshot_id = str(e.get("snapshot_id") or e.get("id") or f"snapshot-{e['_seq']}")
        record = {
            "snapshot_id": snapshot_id,
            "resource": e.get("resource"),
            "route": e.get("route"),
            "at": e["received_at"],
            "path": e.get("path") or e.get("uri"),
            "checksum": e.get("checksum"),
            "restorable": bool(e.get("restorable", True)),
            "includes_runtime_state": bool(e.get("includes_runtime_state", e.get("runtime_state", False))),
        }
        self.state["snapshots"].append(record)
        if record["resource"]:
            resource = self._ensure_resource(record["resource"])
            resource["snapshot_ids"].append(snapshot_id)

    def _on_ticket_takeover(self, e: dict[str, Any]) -> None:
        ticket = e.get("ticket") or e.get("ticket_id")
        agent = e.get("agent") or e.get("owner") or e.get("by")
        if not ticket or not agent:
            raise ValidationError("ticket_takeover requires ticket and agent")
        ticket_state = self.state["tickets"].setdefault(str(ticket), {
            "ticket": str(ticket), "obligation_keys": [], "takeovers": []})
        record = {"agent": agent, "at": e["received_at"], "seq": e["_seq"],
                  "reason": e.get("reason"), "allowed": e.get("allowed")}
        ticket_state["owner"] = agent
        ticket_state.setdefault("takeovers", []).append(record)
        self.state["takeovers"].append({"ticket": str(ticket), **record})

    def _on_decision_recorded(self, e: dict[str, Any]) -> None:
        decision = e.get("decision") or {k: v for k, v in e.items() if not k.startswith("_")}
        self.state["decisions"].append({"seq": e["_seq"], "at": e["received_at"], "decision": decision})

    # ------------------------------------------------------------------
    # Read models
    # ------------------------------------------------------------------
    def _receipt_index(self) -> dict[str, dict[str, Any]]:
        return {r["receipt_id"]: r for r in self.state["receipts"]}

    @staticmethod
    def receipt_is_valid(receipt: dict[str, Any]) -> bool:
        if receipt.get("corrected") and receipt.get("correction"):
            return bool(receipt["correction"].get("valid", False))
        return bool(receipt.get("valid"))

    def obligation_view(self, key: str, at: int | None = None) -> dict[str, Any]:
        temporal = self._engine_at(at)
        if temporal is not self:
            return temporal.obligation_view(key, at=at)
        at = self.as_of(at)
        obl = self.state["obligations"].get(key)
        if obl is None:
            raise NotFoundError(f"unknown obligation: {key}")
        receipt_index = self._receipt_index()
        receipts = [receipt_index[rid] for rid in obl["receipt_ids"] if rid in receipt_index]
        valid_receipts = [r for r in receipts if self.receipt_is_valid(r) and key in r.get("coverage", [])]
        all_applied = [r for r in valid_receipts if r["stage"] == "applied"]
        applied = [r for r in all_applied if r.get("production_evidence", True)]
        non_production_applied = [r for r in all_applied if not r.get("production_evidence", True)]
        received = [r for r in valid_receipts if r["stage"] == "received"]
        assumptions = self._matching_assumptions(obl, at)
        active_assumptions = assumptions
        future_assumptions = [a for a in self.state["assumptions"]
                              if a not in assumptions
                              and (a.get("consumer") == obl.get("consumer"))
                              and int(a.get("effective_at") or a.get("at") or 0) > at]
        logical_receipt = self._logical_applied_coverage(obl, at)
        completed = bool(applied or logical_receipt)
        assumed = bool(active_assumptions)
        scoped_transfer_route = self._scoped_transfer_route(obl, at)
        legacy_released = completed or assumed or bool(scoped_transfer_route)
        last_receipt = max(valid_receipts, key=lambda r: r["seq"], default=None)
        last_applied = max(applied, key=lambda r: (r.get("fact_at") or 0, r["seq"]), default=None)
        active_custodian = obl["custodians"][-1] if obl.get("custodians") else None
        sub = self.state["subscriptions"].get(subscription_key(obl["tenant"] or "", obl["consumer"] or "", obl["topic"]))
        result = copy.deepcopy(obl)
        result.update({
            "as_of": at,
            "completed": completed,
            "open": not completed,
            "assumed": assumed,
            "scoped_transfer_route": scoped_transfer_route,
            "logical_coverage_receipt": logical_receipt,
            "legacy_released": legacy_released,
            "future_assumption": bool(future_assumptions),
            "active_assumptions": active_assumptions,
            "future_assumptions": future_assumptions,
            "valid_receipt_count": len(valid_receipts),
            "applied_receipts": applied,
            "all_applied_receipts": all_applied,
            "non_production_applied_receipts": non_production_applied,
            "received_receipts": received,
            "last_valid_receipt": last_receipt,
            "last_applied_receipt": last_applied,
            "active_custodian": active_custodian,
            "subscription_disposition": (sub or {}).get("disposition"),
            "subscription_state": (sub or {}).get("state"),
        })
        result["routing"] = self.route_obligation(key, at)
        return result

    def route_obligation(self, key: str, at: int | None = None) -> dict[str, Any]:
        temporal = self._engine_at(at)
        if temporal is not self:
            return temporal.route_obligation(key, at=at)
        at = self.as_of(at)
        obl = self.state["obligations"][key]
        receipt_index = self._receipt_index()
        receipts = [receipt_index[rid] for rid in obl["receipt_ids"] if rid in receipt_index]
        valid = [r for r in receipts if self.receipt_is_valid(r) and key in r.get("coverage", [])]
        applied = [r for r in valid if r["stage"] == "applied" and r.get("production_evidence", True)]
        logical_receipt = None if applied else self._logical_applied_coverage(obl, at)
        base = {"tenant": obl["tenant"], "consumer": obl["consumer"], "topic": obl["topic"],
                "message": obl["message"], "ticket": obl["ticket"]}
        if logical_receipt:
            return {**base, "action": "suppress_redelivery", "deliver_to": None,
                    "reason": "same_logical_message_covered_by_valid_applied_receipt",
                    "covered_by_route": logical_receipt.get("route"),
                    "receipt_id": logical_receipt["receipt_id"],
                    "coverage_scope": "logical_tenant_message"}

        if applied:
            last = max(applied, key=lambda r: r["seq"])
            return {**base, "action": "suppress_redelivery", "deliver_to": None,
                    "reason": "valid_applied_receipt_covers_logical_message",
                    "covered_by_route": last.get("route"), "receipt_id": last["receipt_id"]}

        custodian = obl.get("custodians", [None])[-1] if obl.get("custodians") else None
        if custodian:
            return {**base, "action": "await_custodian_application", "deliver_to": None,
                    "follow_up_with": custodian["proxy"], "custody": custodian,
                    "reason": "proxy_validly_collected_before_delegation_expiry"}

        delegation = self._active_delegation(obl["consumer"], "*", obl["topic"], at)
        # _active_delegation requires a concrete proxy; find any active proxy.
        if delegation is None:
            active = [d for d in self.state["registry"]["delegations"]
                      if d["consumer"] == obl["consumer"] and d["topic"] in {"*", obl["topic"]}
                      and self._delegation_active_at(d, at)]
            delegation = max(active, key=lambda d: d["source_seq"], default=None)
        if delegation:
            return {**base, "action": "deliver_to_authorized_proxy",
                    "deliver_to": delegation["proxy"], "recipient": delegation["proxy"],
                    "route": "delegated-endpoint", "delegation_id": delegation["delegation_id"],
                    "reason": "delegation_active"}

        # Delegation existed but is no longer usable and did not establish custody.
        expired = [d for d in self.state["registry"]["delegations"]
                   if d["consumer"] == obl["consumer"] and d["topic"] in {"*", obl["topic"]}]
        expired_proxy = max(expired, key=lambda d: d["source_seq"], default=None)
        sub = self.state["subscriptions"].get(subscription_key(obl["tenant"] or "", obl["consumer"] or "", obl["topic"]))
        assumed = bool(self._matching_assumptions(obl, at))
        scoped_route = self._scoped_transfer_route(obl, at)
        global_transfer = (sub or {}).get("disposition") == "transferred"
        confirmed_transfer = assumed or scoped_route or global_transfer
        if confirmed_transfer:
            route = scoped_route or ((sub or {}).get("target_route") if global_transfer else None) or "next"
            return {**base, "action": "deliver_to_consumer_new_route",
                    "deliver_to": obl["consumer"], "recipient": obl["consumer"], "route": route,
                    "reason": "explicit_continuation_or_transfer_scope"}
        if expired_proxy:
            reason = "delegation_inactive_without_valid_collection_route_to_principal"
        else:
            reason = "no_active_delegation_route_to_responsible_consumer"
        legacy_route = (obl.get("legacy_routes") or obl.get("routes") or [None])[-1]
        contract = self.state["tenants"].get(obl["tenant"] or "", {})
        until = contract.get("legacy_until")
        contract_active = until is None or at <= int(until)
        stopped = (sub or {}).get("disposition") in {"stopped", "transferred"} or (sub or {}).get("state") in {"accepting_stopped", "redirected"}
        retained_active = (sub or {}).get("disposition") == "retained" and contract_active
        if retained_active or (legacy_route and contract_active and not stopped):
            return {**base, "action": "deliver_direct_legacy", "deliver_to": obl["consumer"],
                    "recipient": obl["consumer"], "route": legacy_route,
                    "reason": reason + "_legacy_route_available"}
        return {**base, "action": "hold_for_consumer_manual", "deliver_to": obl["consumer"],
                "recipient": obl["consumer"], "route": "hold-queue",
                "reason": reason + "_legacy_commitment_window_ended_or_stopped"}

    @staticmethod
    def _delegation_active_at(d: dict[str, Any], at: int) -> bool:
        if at < d["start"]:
            return False
        end = d["revocation_effective_at"] if d["revocation_effective_at"] is not None else d["until"]
        if end is None:
            return True
        return at <= end if d.get("end_inclusive") else at < end

    def backlog(self, tenant: str | None = None, at: int | None = None,
                include_completed: bool = False) -> list[dict[str, Any]]:
        temporal = self._engine_at(at)
        if temporal is not self:
            return temporal.backlog(tenant=tenant, at=at, include_completed=include_completed)
        result = []
        for key in sorted(self.state["obligations"]):
            obl = self.state["obligations"][key]
            if tenant and obl.get("tenant") != tenant:
                continue
            view = self.obligation_view(key, at)
            if view["completed"] and not include_completed:
                continue
            result.append({
                "key": key,
                "tenant": obl["tenant"],
                "consumer": obl["consumer"],
                "topic": obl["topic"],
                "message": obl["message"],
                "ticket": obl["ticket"],
                "completed": view["completed"],
                "assumed": view["assumed"],
                "routing": view["routing"],
                "last_valid_receipt_seq": (view["last_valid_receipt"] or {}).get("seq"),
            })
        return result

    def route_message(self, tenant: str, message: str, *, consumer: str | None = None,
                      topic: str | None = None, at: int | None = None) -> list[dict[str, Any]]:
        temporal = self._engine_at(at)
        if temporal is not self:
            return temporal.route_message(tenant, message, consumer=consumer, topic=topic, at=at)
        mk = logical_message_key(str(tenant), str(message))
        keys = self.state["messages"].get(mk, {}).get("obligation_keys", [])
        output = []
        for key in keys:
            obl = self.state["obligations"][key]
            if consumer and obl["consumer"] != consumer:
                continue
            if topic and obl["topic"] != topic:
                continue
            output.append(self.route_obligation(key, at))
        if not output:
            raise NotFoundError(f"no observed delivery for {tenant}/{message}")
        return output

    def legacy_receipt_covers(self, tenant: str, message: str, *, consumer: str | None = None,
                              topic: str | None = None) -> dict[str, Any] | None:
        mk = logical_message_key(str(tenant), str(message))
        for key in self.state["messages"].get(mk, {}).get("obligation_keys", []):
            obl = self.state["obligations"][key]
            if consumer and obl["consumer"] != consumer:
                continue
            if topic and obl["topic"] != topic:
                continue
            view = self.obligation_view(key)
            for receipt in sorted(view["applied_receipts"], key=lambda r: r["seq"], reverse=True):
                if route_generation(receipt.get("route")) == "legacy" or str(receipt.get("route", "")).startswith("legacy"):
                    return {"covered": True, "obligation_key": key, "receipt": receipt}
            if view["applied_receipts"]:
                return {"covered": True, "obligation_key": key,
                        "receipt": sorted(view["applied_receipts"], key=lambda r: r["seq"])[-1],
                        "route_family": "not-specifically-legacy"}
        return None

    def trace_ticket(self, ticket: str | None = None, *, tenant: str | None = None,
                     message: str | None = None, at: int | None = None) -> dict[str, Any]:
        temporal = self._engine_at(at)
        if temporal is not self:
            return temporal.trace_ticket(ticket, tenant=tenant, message=message, at=at)
        if ticket is None and tenant is not None and message is not None:
            ticket = f"ticket:{tenant}/{message}"
        if ticket is None:
            raise ValidationError("trace_ticket requires ticket or tenant+message")
        ticket_state = self.state["tickets"].get(str(ticket))
        keys: list[str] = []
        if ticket_state:
            keys = list(ticket_state.get("obligation_keys", []))
        elif tenant is not None and message is not None:
            mk = logical_message_key(str(tenant), str(message))
            keys = list(self.state["messages"].get(mk, {}).get("obligation_keys", []))
        else:
            # Allow tracing by business message id when caller passes it as ticket.
            matches = [t for t in self.state["tickets"] if t.endswith(f"/{ticket}")]
            if len(matches) == 1:
                ticket = matches[0]
                ticket_state = self.state["tickets"][ticket]
                keys = list(ticket_state.get("obligation_keys", []))
        if not keys:
            raise NotFoundError(f"unknown ticket or message: {ticket}")
        all_receipts = []
        obligation_views = []
        for key in keys:
            view = self.obligation_view(key, at)
            obligation_views.append(view)
            valid_for_key = [
                r for r in self.state["receipts"]
                if self.receipt_is_valid(r) and key in r.get("coverage", [])
            ]
            all_receipts.extend(valid_for_key)
        all_receipts = sorted({r["receipt_id"]: r for r in all_receipts}.values(), key=lambda r: r["seq"])
        last_valid = all_receipts[-1] if all_receipts else None
        applied = [r for r in all_receipts if r["stage"] == "applied"]
        last_applied = applied[-1] if applied else None
        resources = sorted({route for view in obligation_views for route in view.get("legacy_routes", [])})
        resource_names = sorted({name for name, res in self.state["resources"].items()
                                 if set(res.get("routes", [])) & set(resources)})
        rollback_gaps = [self.rollback_assessment(name, at) for name in resource_names
                         if self.state["resources"][name].get("observed_state") == "removed"
                         or self.state["resources"][name].get("ever_confirmed_removed")]
        return {
            "ticket": ticket,
            "found": True,
            "owner": (ticket_state or {}).get("owner"),
            "obligations": obligation_views,
            "receipt_chain": all_receipts,
            "last_valid_receipt": last_valid,
            "last_applied_receipt": last_applied,
            "completed": all(v["completed"] for v in obligation_views),
            "legacy_routes": resources,
            "related_resources": resource_names,
            "removed_resource_gaps": rollback_gaps,
            "takeovers": (ticket_state or {}).get("takeovers", []),
        }

    # ------------------------------------------------------------------
    # Migration windows and retirement plans
    # ------------------------------------------------------------------
    def contract_for(self, tenant: str | None) -> dict[str, Any]:
        return self.state["tenants"].get(tenant or "", {
            "tenant": tenant, "legacy_until": None, "confirm_seconds": None})

    def confirmation_view(self, confirmation_id: str | None = None, *, at: int | None = None,
                          confirmation: dict[str, Any] | None = None) -> dict[str, Any]:
        if confirmation is None:
            temporal = self._engine_at(at)
            if temporal is not self:
                return temporal.confirmation_view(confirmation_id, at=at)
        at = self.as_of(at)
        c = confirmation or next((x for x in self.state["confirmations"]
                                   if x["confirmation_id"] == confirmation_id), None)
        if c is None:
            raise NotFoundError(f"unknown confirmation: {confirmation_id}")
        tenant = c.get("tenant") or self._tenant_for_consumer(c["consumer"])
        contract = self.contract_for(tenant)
        window = contract.get("confirm_seconds")
        declared_at = int(c["declared_at"] or 0)
        window_end = declared_at + int(window) if window is not None else None
        scope = c["scope"]
        keys: list[str] = []
        for key, obl in self.state["obligations"].items():
            if obl["consumer"] != c["consumer"]:
                continue
            if tenant and obl["tenant"] != tenant:
                continue
            if c["topic"] not in {"*", obl["topic"]}:
                continue
            if scope != "*" and obl["message"] not in scope:
                continue
            keys.append(key)
        scoped_messages = scope if scope == "*" else list(scope)
        missing_messages = []
        if scope != "*":
            known = {self.state["obligations"][k]["message"] for k in keys}
            missing_messages = [m for m in scope if m not in known]
        items = []
        all_route_applied = not missing_messages and bool(keys)
        all_timely_route_applied = all_route_applied
        latest_route_applied_seq = 0
        window_seconds = int(window) if window is not None else None
        window_start = declared_at - window_seconds if window_seconds is not None else None
        for key in keys:
            view = self.obligation_view(key, at)
            route_receipts = [r for r in view["applied_receipts"]
                              if r.get("route") == c["route"]
                              or (c["route"] == "next" and route_generation(r.get("route")) == "new")]
            timely_receipts = []
            for receipt in route_receipts:
                evidence_at = receipt.get("fact_at")
                if evidence_at is None:
                    evidence_at = receipt.get("received_at") or declared_at
                offset = int(evidence_at) - declared_at
                timely = window_seconds is None or (
                    -window_seconds <= offset <= window_seconds
                )
                receipt["confirmation_window_offset"] = offset
                receipt["within_confirmation_window"] = timely
                if timely:
                    timely_receipts.append(receipt)
            old_receipts = [r for r in view["applied_receipts"]
                            if route_generation(r.get("route")) == "legacy"]
            route_ok = bool(route_receipts)
            timely_ok = bool(timely_receipts)
            all_route_applied = all_route_applied and route_ok
            all_timely_route_applied = all_timely_route_applied and timely_ok
            if route_receipts:
                latest_route_applied_seq = max(latest_route_applied_seq, max(r["seq"] for r in route_receipts))
            items.append({
                "obligation_key": key,
                "message": self.state["obligations"][key]["message"],
                "new_route_applied": route_ok,
                "timely_new_route_applied": timely_ok,
                "route_receipts": route_receipts,
                "timely_route_receipts": timely_receipts,
                "legacy_applied": bool(old_receipts),
                "legacy_receipt_coverage": old_receipts[-1] if old_receipts else None,
                "completed": view["completed"],
            })
        window_elapsed = window_end is not None and at >= window_end
        production_confirmation = c.get("production", True)
        formal = bool(production_confirmation and all_timely_route_applied
                      and window is not None and window_elapsed)
        if not production_confirmation:
            status = "integration_environment_validation_only"
        elif formal:
            status = "formal"
        elif all_route_applied and not all_timely_route_applied:
            status = "applied_evidence_outside_confirmation_window"
        elif all_route_applied:
            status = "evidence_ready_waiting_window"
        else:
            status = "awaiting_applied_evidence"
        return {
            **c,
            "tenant": tenant,
            "window_seconds": window,
            "window_starts_at": window_start,
            "window_ends_at": window_end,
            "window_elapsed": window_elapsed,
            "evidence_ready": all_route_applied,
            "timely_evidence_ready": all_timely_route_applied,
            "formal": formal,
            "production_migration": production_confirmation,
            "status": status,
            "missing_messages": missing_messages,
            "items": items,
            "latest_route_applied_seq": latest_route_applied_seq or None,
            "as_of": at,
        }

    def confirmation_covering(self, consumer: str, topic: str, messages: Iterable[str] | str,
                              at: int | None = None, formal_only: bool = False) -> dict[str, Any] | None:
        temporal = self._engine_at(at)
        if temporal is not self:
            return temporal.confirmation_covering(consumer, topic, messages, at=at, formal_only=formal_only)
        wanted = "*" if messages == "*" else list(messages or [])
        if wanted != "*" and not wanted:
            return None
        best = None
        for c in self.state["confirmations"]:
            if c["consumer"] != consumer or c["topic"] not in {topic, "*"}:
                continue
            view = self.confirmation_view(confirmation=c, at=at)
            if formal_only and not view["formal"]:
                continue
            scope = c["scope"]
            if wanted == "*":
                # Whole-subscription authority requires an explicit all-messages
                # scope; a named-message confirmation can never be expanded.
                if scope != "*":
                    continue
            elif scope != "*" and not set(wanted).issubset(set(scope)):
                continue
            candidate = view
            if best is None or candidate["seq"] > best["seq"]:
                best = candidate
        return best

    def subscription_view(self, key: str, at: int | None = None) -> dict[str, Any]:
        temporal = self._engine_at(at)
        if temporal is not self:
            return temporal.subscription_view(key, at=at)
        at = self.as_of(at)
        sub = self.state["subscriptions"][key]
        contract = self.contract_for(sub.get("tenant"))
        until = contract.get("legacy_until")
        cancelled_at = contract.get("cancelled_at")
        tenant_cancelled = cancelled_at is not None and int(cancelled_at) <= at
        effective = sub.get("effective_at")
        active_now = effective is None or int(effective) <= at
        disposition = sub.get("disposition")
        released = active_now and disposition in {"transferred", "stopped"}
        retained_active = (not tenant_cancelled and active_now and disposition == "retained"
                           and (until is None or at <= int(until)))
        undecided_active = (not tenant_cancelled and active_now and disposition is None
                            and (until is None or at <= int(until)))
        return {
            **sub,
            "as_of": at,
            "tenant_cancelled": tenant_cancelled,
            "contract_legacy_until": until,
            "released_for_resource_removal": released or tenant_cancelled,
            "contract_claim": retained_active or undecided_active,
            "contract_expired": until is not None and at > int(until),
            "migration_confirmation": self.confirmation_covering(
                sub["consumer"], sub["topic"], "*", at=at),
        }

    def _resource_claims(self, resource_name: str, at: int) -> list[dict[str, Any]]:
        resource = self.state["resources"][resource_name]
        route_names = set(resource.get("routes", []))
        shared = bool(resource.get("shared_route") or len(resource.get("routes", [])) > 1)
        claims: list[dict[str, Any]] = []
        all_legacy_route_names = {
            route_name
            for candidate_resource in self.state["resources"].values()
            for route_name in candidate_resource.get("routes", [])
            if route_generation(route_name) != "new"
        }
        for key in sorted(self.state["obligations"]):
            obl = self.state["obligations"][key]
            matching_routes = set(obl.get("routes", [])) & route_names
            if not matching_routes:
                sub = self.state["subscriptions"].get(subscription_key(
                    obl.get("tenant") or "", obl.get("consumer") or "", obl.get("topic") or "*"))
                observed = {r for r in (sub or {}).get("observed_routes", [])
                            if route_generation(r) != "new"}
                fallback_routes = observed or all_legacy_route_names
                matching_routes = fallback_routes & route_names
            if not matching_routes:
                continue
            view = self.obligation_view(key, at)
            if not view["legacy_released"]:
                matching_route = sorted(matching_routes)[0]
                claims.append({
                    "type": "open_legacy_obligation",
                    "severity": "blocking",
                    "gate": "removal",
                    "blocks_disable": shared,
                    "shared_resource": shared,
                    "tenant": obl["tenant"],
                    "consumer": obl["consumer"],
                    "route": matching_route,
                    "obligation_key": key,
                    "ticket": obl["ticket"],
                    "completed": view["completed"],
                    "assumed": view["assumed"],
                    "custodian": (view.get("active_custodian") or {}).get("proxy"),
                    "reason": "only a valid applied receipt or effective explicit continuation releases this claim",
                })
        for sub_key in self._select_subscription_keys():
            sub = self.subscription_view(sub_key, at)
            candidate_routes = set(sub.get("observed_routes", []))
            if sub.get("legacy_route"):
                candidate_routes.add(sub["legacy_route"])
            # The new route is not a claim on legacy topology resources.
            legacy_routes = {route for route in candidate_routes if route_generation(route) != "new"}
            if not legacy_routes:
                # No traffic has yet revealed the topic/route binding.  Use the
                # conservative topology-wide claim rather than retiring early.
                legacy_routes = set(all_legacy_route_names)
            matching = legacy_routes & route_names
            if not matching:
                continue
            if sub["released_for_resource_removal"]:
                continue
            if sub["contract_claim"]:
                claims.append({
                    "type": "subscription_contract",
                    "severity": "blocking",
                    "gate": "removal",
                    "blocks_disable": shared,
                    "shared_resource": shared,
                    "tenant": sub.get("tenant"),
                    "consumer": sub.get("consumer"),
                    "topic": sub.get("topic"),
                    "route": sorted(matching)[0],
                    "subscription_key": sub_key,
                    "disposition": sub.get("disposition") or "undecided",
                    "legacy_until": sub.get("contract_legacy_until"),
                    "reason": "existing subscription is active or retained within its contractual legacy window",
                })
            elif sub.get("disposition") == "retained" and sub.get("contract_expired"):
                claims.append({
                    "type": "retained_after_contract",
                    "severity": "manual",
                    "gate": "removal",
                    "blocks_disable": False,
                    "shared_resource": shared,
                    "tenant": sub.get("tenant"),
                    "subscription_key": sub_key,
                    "reason": "retained subscription passed legacy_until; a final decision is required",
                })
        return claims

    def rollback_assessment(self, resource_name: str, at: int | None = None) -> dict[str, Any]:
        temporal = self._engine_at(at)
        if temporal is not self:
            return temporal.rollback_assessment(resource_name, at=at)
        at = self.as_of(at)
        resource = self.state["resources"].get(resource_name)
        if resource is None:
            raise NotFoundError(f"unknown resource: {resource_name}")
        state = resource.get("observed_state", "unknown")
        ever_removed = bool(resource.get("ever_confirmed_removed"))
        if state != "removed" and not ever_removed:
            return {
                "resource": resource_name,
                "removed_confirmed": False,
                "plain_restore_possible": True,
                "mode": "restore_existing_resource",
                "rebuild_required": [],
                "irreparable_gaps": [],
                "note": "resource has not been confirmed removed; restore remains a normal reversible operation",
            }
        full_snapshots = [s for s in self.state["snapshots"]
                          if (not s.get("resource") or s.get("resource") == resource_name)
                          and s.get("restorable") and s.get("includes_runtime_state")]
        rebuild = [{
            "action": "rebuild_resource",
            "resource": resource_name,
            "known_routes": resource.get("routes", []),
            "topology_source": "inventory_and_observed_events",
        }]
        if full_snapshots:
            rebuild.append({"action": "restore_runtime_snapshot", "snapshot_ids": [s["snapshot_id"] for s in full_snapshots]})
        gaps = []
        if not full_snapshots:
            gaps.append({
                "code": "runtime_state_not_reconstructable",
                "resource": resource_name,
                "description": "Poll cursors, queue ownership, and in-resource handling state were not snapshotted; automation cannot recreate that exact runtime state.",
                "remediation": "rebuild topology, reset from auditable receipts, and manually reconcile in-flight work",
            })
        claims = self._resource_claims(resource_name, at)
        for claim in claims:
            if claim["type"] == "open_legacy_obligation":
                gaps.append({
                    "code": "unfinished_ticket_without_terminal_receipt",
                    "resource": resource_name,
                    "ticket": claim.get("ticket"),
                    "obligation_key": claim.get("obligation_key"),
                    "description": "The removed resource may have been the only place holding the ticket's latest business state.",
                    "remediation": "contact consumer/agent and reconcile externally; do not replay as a new business action",
                })
        assessment = {
            "resource": resource_name,
            "removed_confirmed": state == "removed",
            "historical_removal_confirmed": ever_removed,
            "currently_rebuilt": bool(resource.get("rebuilt_after_removal")),
            "current_observed_state": state,
            "plain_restore_possible": False,
            "mode": "explicit_rebuild_and_gap_disclosure",
            "rebuild_required": rebuild,
            "irreparable_gaps": gaps,
            "gap_count": len(gaps),
        }
        if state != "removed" and ever_removed:
            assessment.update({
                "mode": "rebuilt_but_historical_gaps_remain",
                "note": "The resource was rebuilt after confirmed removal; disclosed gaps are not erased by recreation.",
            })
        return assessment

    def resource_plan(self, resource_name: str, at: int | None = None) -> dict[str, Any]:
        temporal = self._engine_at(at)
        if temporal is not self:
            return temporal.resource_plan(resource_name, at=at)
        at = self.as_of(at)
        resource = self.state["resources"].get(resource_name)
        if resource is None:
            raise NotFoundError(f"unknown resource: {resource_name}")
        claims = self._resource_claims(resource_name, at)
        blocking = [c for c in claims if c.get("severity") == "blocking"]
        claim_tenants = {c.get("tenant") for c in blocking if c.get("tenant")}
        cross_tenant_shared = len(claim_tenants) > 1
        if cross_tenant_shared:
            # A resource can be effectively shared by serving two tenants on
            # one route even when topology lists that route only once.
            disable_blocking = list(blocking)
            for claim in blocking:
                claim["blocks_disable"] = True
                claim["effective_shared_due_to_tenants"] = True
        else:
            disable_blocking = [c for c in blocking if c.get("blocks_disable")]
        manual = [c for c in claims if c.get("severity") == "manual"]
        state = resource.get("observed_state", "unknown")
        pending = [op for op in self.state["operations"].values()
                   if op.get("resource") == resource_name and op.get("status") == "requested"]
        new_route_only = bool(resource.get("routes")) and all(
            route_generation(route) == "new" for route in resource.get("routes", []))
        rollback = self.rollback_assessment(resource_name, at) if (
            state == "removed" or resource.get("ever_confirmed_removed")) else None
        if new_route_only:
            next_action = "keep_new_route_not_legacy_retirement_target"
            auto = False
        elif state == "removed":
            next_action = "plan_explicit_rollback"
            auto = False
        elif state in {"disabled", "paused"} and blocking:
            next_action = "complete_or_explicitly_transfer_claims_before_removal"
            auto = False
        elif disable_blocking:
            next_action = "hold_shared_resource_until_last_claim_is_released"
            auto = False
        elif state in {"disabled", "paused"}:
            next_action = "retire_or_remove"
            auto = not manual
        elif state == "unknown":
            next_action = "observe_line_state_before_action"
            auto = False
        else:
            next_action = "pause_disable_then_observe_effect"
            # A dedicated resource may enter a reversible stopped phase while
            # customer service takes over; shared resources require clear claims.
            auto = not manual and not disable_blocking
        return {
            "resource": resource_name,
            "routes": resource.get("routes", []),
            "shared_route": bool(resource.get("shared_route") or len(resource.get("routes", [])) > 1),
            "effective_shared": bool(resource.get("shared_route") or len(resource.get("routes", [])) > 1 or cross_tenant_shared),
            "shared_tenant_claims": len(claim_tenants),
            "observed_state": state,
            "raw_state": resource.get("raw_state"),
            "new_route_only": new_route_only,
            "pending_operations": pending,
            "claims": claims,
            "blocking_claims": blocking,
            "disable_blocking_claims": disable_blocking,
            "removal_blocking_claims": blocking,
            "manual_claims": manual,
            "can_request_reversible_stop": not disable_blocking,
            "can_request_retire": not blocking and state in {"active", "disabled", "unknown"},
            "removal_eligible": not blocking and state == "disabled",
            "confirmed_removed": state == "removed",
            "historical_removal_confirmed": bool(resource.get("ever_confirmed_removed")),
            "rebuilt_after_removal": bool(resource.get("rebuilt_after_removal")),
            "disable_eligible": not disable_blocking and state in {"active", "unknown", "disabled"},
            "auto_eligible": auto,
            "next_safe_action": next_action,
            "rollback": rollback,
            "as_of": at,
            "revision": self.state["revision"],
        }

    def commitment_views(self, at: int | None = None) -> list[dict[str, Any]]:
        temporal = self._engine_at(at)
        if temporal is not self:
            return temporal.commitment_views(at=at)
        at = self.as_of(at)
        result = []
        for key, commitment in sorted(self.state["commitments"].items()):
            view = dict(commitment)
            obl = self.state["obligations"].get(key)
            if obl:
                obligation = self.obligation_view(key, at)
                view.update({
                    "completed": obligation["completed"],
                    "assumed": obligation["assumed"],
                    "legacy_released": obligation["legacy_released"],
                    "last_valid_receipt": obligation["last_valid_receipt"],
                })
            else:
                view.update({"completed": False, "assumed": False, "legacy_released": False})
            result.append(view)
        return result

    def plan_retirement(self, tenant: str | None = None, at: int | None = None) -> dict[str, Any]:
        temporal = self._engine_at(at)
        if temporal is not self:
            return temporal.plan_retirement(tenant=tenant, at=at)
        at = self.as_of(at)
        tenants = []
        for name, contract in sorted(self.state["tenants"].items()):
            if tenant and name != tenant:
                continue
            tenants.append({"name": name, **contract})
        subscriptions = [self.subscription_view(k, at) for k in sorted(self.state["subscriptions"])
                         if not tenant or self.state["subscriptions"][k].get("tenant") == tenant]
        confirmations = [self.confirmation_view(c["confirmation_id"], at=at)
                         for c in self.state["confirmations"]
                         if not tenant or c.get("tenant") == tenant]
        resources = [self.resource_plan(name, at) for name in sorted(self.state["resources"])]
        if tenant:
            # Shared resources are always shown, but annotate whether they carry
            # another tenant's claim.
            pass
        backlog = self.backlog(tenant=tenant, at=at)
        commitments = [c for c in self.commitment_views(at)
                       if not tenant or c.get("tenant") == tenant]
        return {
            "as_of": at,
            "revision": self.state["revision"],
            "tenant_filter": tenant,
            "tenants": tenants,
            "subscriptions": subscriptions,
            "confirmations": confirmations,
            "commitments": commitments,
            "backlog": backlog,
            "backlog_count": len(backlog),
            "resources": resources,
            "batches": list(self.state["batches"].values()),
            "decisions": self.state["decisions"],
        }

    # ------------------------------------------------------------------
    # Imperative use cases used by the CLI and durable service
    # ------------------------------------------------------------------
    def register_consumer(self, consumer: str, tenant: str, topics: list[str] | None = None,
                          at: int | None = None, **extra: Any) -> dict[str, Any]:
        return self.command("consumer_registered",
                            {"consumer": consumer, "tenant": tenant, "topics": list(topics or []), **extra},
                            at=at)

    def delegate(self, consumer: str, proxy: str, *, topic: str = "*", start: int | None = None,
                 until: int | None = None, at: int | None = None,
                 delegation_id: str | None = None, **extra: Any) -> dict[str, Any]:
        payload = {"consumer": consumer, "proxy": proxy, "topic": topic,
                   "start": start if start is not None else self.as_of(at), "until": until,
                   **extra}
        if delegation_id:
            payload["delegation_id"] = delegation_id
        return self.command("delegation", payload, at=at)

    def revoke_delegation(self, *, delegation_id: str | None = None, consumer: str | None = None,
                          proxy: str | None = None, topic: str | None = None,
                          at: int | None = None) -> dict[str, Any]:
        return self.command("delegation_revoked",
                            {"delegation_id": delegation_id, "consumer": consumer,
                             "proxy": proxy, "topic": topic}, at=at)

    def deliver(self, tenant: str, message: str, consumer: str, *, topic: str | None = None,
                route: str | None = None, at: int | None = None, **extra: Any) -> dict[str, Any]:
        return self.command("delivery", {"tenant": tenant, "message": message, "consumer": consumer,
                                         "topic": topic, "route": route, **extra}, at=at)

    def receipt(self, tenant: str, message: str, actor: str, stage: str, *,
                route: str | None = None, at: int | None = None, **extra: Any) -> dict[str, Any]:
        return self.command("delivery_receipt", {"tenant": tenant, "message": message, "actor": actor,
                                                 "stage": stage, "route": route, **extra}, at=at)

    def confirm(self, consumer: str, topic: str, reference: Any, *, route: str = "next",
                at: int | None = None, tenant: str | None = None, **extra: Any) -> dict[str, Any]:
        return self.command("new_confirm", {"consumer": consumer, "topic": topic, "reference": reference,
                                            "route": route, "tenant": tenant, **extra}, at=at)

    def assume(self, consumer: str, topic: str, reference: Any, *, tenant: str | None = None,
               to_route: str = "next", at: int | None = None, **extra: Any) -> dict[str, Any]:
        return self.command("assumption", {"consumer": consumer, "topic": topic, "reference": reference,
                                           "tenant": tenant, "to_route": to_route, **extra}, at=at)

    def transition_subscriptions(self, action: str, *, tenant: str | None = None,
                                  consumer: str | None = None, topic: str | None = None,
                                  batch_id: str | None = None, at: int | None = None,
                                  messages: list[str] | str | None = None,
                                  target_route: str | None = None,
                                  idempotency_key: str | None = None) -> dict[str, Any]:
        return self.command("subscription_command", {
            "action": action, "tenant": tenant, "consumer": consumer, "topic": topic,
            "batch_id": batch_id or f"batch-{self.state['revision'] + 1}",
            "messages": messages, "target_route": target_route,
        }, at=at, idempotency_key=idempotency_key or batch_id)

    def line_action(self, resource: str, action: str, *, operation: str | None = None,
                    tenant: str | None = None, at: int | None = None,
                    simulate: bool = False, force_fail: bool | None = None) -> dict[str, Any]:
        return self.command("line_action", {
            "resource": resource, "action": action, "operation": operation,
            "tenant": tenant, "_simulate": simulate, "_force_fail": force_fail,
        }, at=at, idempotency_key=operation)

    def line_effect(self, resource: str, state: str, *, operation: str | None = None,
                    at: int | None = None) -> dict[str, Any]:
        return self.command("line_effect", {"resource": resource, "state": state,
                                             "operation": operation}, at=at)

    def take_over(self, ticket: str, agent: str, *, at: int | None = None,
                  reason: str | None = None) -> dict[str, Any]:
        trace = self.trace_ticket(ticket, at=at)
        access = ("audit_only_after_confirmed_removal" if trace["removed_resource_gaps"]
                  else "legacy_manual_takeover_available")
        result = self.command("ticket_takeover", {"ticket": ticket, "agent": agent,
                                                   "reason": reason, "access": access}, at=at)
        return {"access": access, "trace": trace, "command": result}

    def advance_migration(self, *, tenant: str | None = None, consumer: str | None = None,
                          topic: str | None = None, messages: list[str] | str | None = None,
                          at: int | None = None, execute: bool = False,
                          batch_id: str | None = None,
                          expected_revision: int | None = None) -> dict[str, Any]:
        self.expected_revision(expected_revision)
        requested_at = self.as_of(at)
        at = self.as_of() if execute else requested_at
        start_revision = self.state["revision"]
        if messages is not None and messages != "*" and not list(messages):
            return {"at": requested_at, "execution_at": at, "revision_before": start_revision,
                    "revision_after": start_revision, "executed": execute,
                    "planned_or_applied": [],
                    "deferred": [{"action": "invalid_empty_scope",
                                  "reason": "message scope must contain at least one id or be '*'"}]}
        keys = self._select_subscription_keys(tenant, consumer, topic)
        planned: list[dict[str, Any]] = []
        deferred: list[dict[str, Any]] = []
        for key in keys:
            sub = self.state["subscriptions"][key]
            scope: list[str] | str = "*" if messages is None else (
                "*" if messages == "*" else list(messages or []))
            confirmation = self.confirmation_covering(sub["consumer"], sub["topic"], scope, at=at)
            exact_confirmation = None
            if messages is not None and messages != "*":
                exact_confirmation = self.confirmation_covering(
                    sub["consumer"], sub["topic"], list(messages), at=at)
            basis = exact_confirmation or confirmation
            contract = self.contract_for(sub.get("tenant"))
            until = contract.get("legacy_until")
            if basis is not None and (messages is not None or basis.get("formal")):
                item = {"subscription_key": key, "action": "transfer", "target_route": "next",
                        "basis": "migration_confirmation_scope",
                        "confirmation_id": basis["confirmation_id"], "scope": scope}
                planned.append(item)
                if execute:
                    self.transition_subscriptions(
                        "transfer", tenant=sub["tenant"], consumer=sub["consumer"],
                        topic=sub["topic"], batch_id=batch_id or f"mig-{start_revision}",
                        at=at, messages=(None if scope == "*" else scope), target_route="next",
                        idempotency_key=f"mig-{start_revision}-{key}")
            elif until is not None and at <= int(until):
                deferred.append({"subscription_key": key, "action": "retain",
                                 "until": until, "reason": "no scope-exact formal migration confirmation"})
            else:
                deferred.append({"subscription_key": key, "action": "manual_hold",
                                 "reason": "contract window ended without automatic migration basis"})
        return {"at": at, "revision_before": start_revision,
                "revision_after": self.state["revision"], "executed": execute,
                "planned_or_applied": planned, "deferred": deferred}

    def _resource_has_tenant_affinity(self, resource_name: str, tenant: str) -> bool:
        resource = self.state["resources"].get(resource_name, {})
        route_names = set(resource.get("routes", []))
        for obl in self.state["obligations"].values():
            if obl.get("tenant") != tenant:
                continue
            if not obl.get("routes") or set(obl.get("routes", [])) & route_names:
                return True
        for sub in self.state["subscriptions"].values():
            if sub.get("tenant") != tenant:
                continue
            observed = set(sub.get("observed_routes", []))
            if not observed or observed & route_names:
                return True
        return False

    def advance_decommission(self, *, tenant: str | None = None, at: int | None = None,
                              resources: list[str] | None = None, execute: bool = False,
                              simulate_line: bool = True,
                              expected_revision: int | None = None) -> dict[str, Any]:
        """Recompute against the newest receipts and advance only safe resources."""

        self.expected_revision(expected_revision)
        requested_at = self.as_of(at)
        # Execution must meet the newest arrived receipts/decisions.  Dry-runs
        # can be evaluated at a historical/reporting time; mutating execution
        # never uses a stale prefix.
        at = self.as_of() if execute else requested_at
        revision_before = self.state["revision"]
        if resources:
            names = list(resources)
        else:
            names = []
            for name, resource in sorted(self.state["resources"].items()):
                route_names = resource.get("routes", [])
                all_new = bool(route_names) and all(route_generation(route) == "new" for route in route_names)
                if all_new:
                    continue
                if tenant and not self._resource_has_tenant_affinity(name, tenant):
                    continue
                names.append(name)
        executed: list[dict[str, Any]] = []
        deferred: list[dict[str, Any]] = []
        rollbacks: list[dict[str, Any]] = []
        for name in names:
            plan = self.resource_plan(name, at)
            if plan["observed_state"] == "removed":
                rollbacks.append(plan["rollback"])
                deferred.append({"resource": name, "action": "explicit_rollback_required",
                                 "reason": "resource is confirmed removed; plain restore is not valid"})
                continue
            if tenant:
                outside_tenant_claims = [claim for claim in plan.get("claims", [])
                                         if claim.get("tenant") and claim.get("tenant") != tenant]
                if outside_tenant_claims:
                    deferred.append({"resource": name, "action": "hold_outside_tenant_impact",
                                     "outside_tenant_claims": outside_tenant_claims,
                                     "reason": "tenant-scoped automatic action would affect another tenant's route obligations"})
                    continue
            if plan["observed_state"] == "unknown":
                deferred.append({"resource": name, "action": "observe_state_before_automatic_action",
                                 "reason": "resource state has not been observed; automatic action lacks a basis"})
                continue
            if plan["manual_claims"]:
                deferred.append({"resource": name, "action": "manual_scope_only",
                                 "manual_claims": plan["manual_claims"],
                                 "reason": "at least one latest claim has only a manual basis"})
                continue
            if plan["observed_state"] == "active" and plan["disable_blocking_claims"]:
                deferred.append({"resource": name, "action": "hold_reversible_stop",
                                 "blocking_claims": plan["disable_blocking_claims"],
                                 "reason": "latest shared obligations still depend on this resource"})
                continue
            if plan["observed_state"] == "disabled" and plan["removal_blocking_claims"]:
                deferred.append({"resource": name, "action": "hold_removal",
                                 "blocking_claims": plan["removal_blocking_claims"],
                                 "reason": "latest obligations/subscriptions are not released for removal"})
                continue
            if plan["observed_state"] not in {"active", "disabled"}:
                deferred.append({"resource": name, "action": "observe_unsupported_state",
                                 "observed_state": plan["observed_state"]})
                continue
            if plan["observed_state"] == "active":
                action = "pause" if name.startswith("poll") else "disable"
                if execute:
                    op = f"auto-{action}-{name}-r{self.state['revision'] + 1}"
                    self.line_action(name, action, operation=op, tenant=tenant, at=at,
                                     simulate=simulate_line)
                    executed.append({"resource": name, "action": action, "operation": op,
                                     "phase": "reversible_stop"})
                else:
                    executed.append({"resource": name, "action": action, "operation": None,
                                     "phase": "reversible_stop", "dry_run": True})
            elif plan["observed_state"] == "disabled":
                action = "retire"
                if execute:
                    op = f"auto-retire-{name}-r{self.state['revision'] + 1}"
                    self.line_action(name, action, operation=op, tenant=tenant, at=at,
                                     simulate=simulate_line)
                    executed.append({"resource": name, "action": action, "operation": op,
                                     "phase": "confirmed_removal"})
                else:
                    executed.append({"resource": name, "action": action, "operation": None,
                                     "phase": "confirmed_removal", "dry_run": True})
            if plan["manual_claims"]:
                deferred.append({"resource": name, "action": "manual_scope_only",
                                 "manual_claims": plan["manual_claims"]})
        return {"at": at, "revision_before": revision_before,
                "revision_after": self.state["revision"], "executed": execute,
                "actions": executed, "deferred": deferred, "removed_rollback_assessments": rollbacks}

    def rollback_resource(self, resource: str, *, at: int | None = None,
                           execute: bool = False, simulate_line: bool = True) -> dict[str, Any]:
        assessment = self.rollback_assessment(resource, at)
        actions: list[dict[str, Any]] = []
        if execute:
            if assessment["plain_restore_possible"]:
                op = f"restore-{resource}-r{self.state['revision'] + 1}"
                self.line_action(resource, "restore", operation=op, at=at, simulate=simulate_line)
                actions.append({"operation": op, "action": "restore"})
            else:
                op = f"rebuild-{resource}-r{self.state['revision'] + 1}"
                self.line_action(resource, "rebuild", operation=op, at=at, simulate=simulate_line)
                actions.append({"operation": op, "action": "rebuild",
                                "warning": "rebuild does not erase disclosed irreparable gaps"})
                self.command("decision_recorded", {
                    "decision": {"type": "post_removal_rollback_disclosed",
                                  "resource": resource, "assessment": assessment}}, at=at)
        return {"assessment": assessment, "executed_actions": actions, "executed": execute}

    # Descriptive aliases used by different callers and translations.
    register_delegate = delegate
    authorize_proxy = delegate
    continue_commitment = assume
    handoff_commitment = assume
    record_receipt = receipt
    get_backlog = backlog
    get_routing = route_message
    trace = trace_ticket
    plan = plan_retirement
    advance = advance_decommission





