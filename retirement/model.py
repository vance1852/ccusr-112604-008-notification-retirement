"""Small domain primitives and normalization helpers.

The implementation deliberately uses JSON-native dictionaries.  A persisted
state file can therefore be inspected and repaired without migrating objects.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Iterable

from .errors import ValidationError

STAGE_ORDER = {"observed": 0, "received": 1, "processed": 2, "applied": 2}
APPLIED_STAGES = {"applied", "processed", "succeeded", "success", "complete", "completed", "acked_business"}
RECEIVED_STAGES = {"received", "acked", "acknowledged", "accepted"}

NEW_ROUTE_NAMES = {"next", "new", "v2", "new-notification", "new_notification"}
LEGACY_ROUTE_PREFIXES = ("legacy", "old")

KIND_ALIASES = {
    "consumer_registered": "consumer_registered",
    "register_consumer": "consumer_registered",
    "consumer": "consumer_registered",
    "delegation": "delegation",
    "delegate": "delegation",
    "authorization": "delegation",
    "delegation_revoked": "delegation_revoked",
    "revoke_delegation": "delegation_revoked",
    "revocation": "delegation_revoked",
    "delivery": "delivery",
    "deliver": "delivery",
    "message": "delivery",
    "delivery_receipt": "delivery_receipt",
    "receipt": "delivery_receipt",
    "new_confirm": "new_confirm",
    "migration_confirm": "new_confirm",
    "confirm": "new_confirm",
    "migration_confirmed": "new_confirm",
    "assume": "assumption",
    "assumption": "assumption",
    "commitment_assumed": "assumption",
    "continue_commitment": "assumption",
    "continuation": "assumption",
    "handoff": "assumption",
    "commitment": "commitment",
    "commitment_event": "commitment",
    "line_action": "line_action",
    "action": "line_action",
    "line_effect": "line_effect",
    "effect": "line_effect",
    "subscription": "subscription_command",
    "subscription_command": "subscription_command",
    "stop_accepting": "subscription_command",
    "batch": "batch_command",
    "batch_command": "batch_command",
    "tenant_cancel": "tenant_cancelled",
    "tenant_cancelled": "tenant_cancelled",
    "snapshot": "snapshot",
    "backup": "snapshot",
    "ticket_takeover": "ticket_takeover",
    "takeover": "ticket_takeover",
    "receipt_correction": "receipt_correction",
    "correction": "receipt_correction",
    "decision": "decision_recorded",
    "decision_recorded": "decision_recorded",
    "probe": "probe_observation",
    "health": "probe_observation",
    "health_check": "probe_observation",
    "ping": "probe_observation",
    "liveness": "probe_observation",
    "readiness": "probe_observation",
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def content_hash(record: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(record).encode("utf-8")).hexdigest()


def require_mapping(value: Any, name: str = "record") -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{name} must be a JSON object")
    return value


def first_present(record: dict[str, Any], names: Iterable[str], default: Any = None) -> Any:
    for name in names:
        if name in record and record[name] is not None:
            return record[name]
    return default


def as_int(value: Any, field: str | None = None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValidationError(f"{field or 'time'} must be an integer, not boolean")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if re.fullmatch(r"-?\d+", text):
            return int(text)
        # Accept ISO-8601 only when it has no ambiguous timezone arithmetic.
        m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:[+-]\d{2}:?\d{2}|Z)?", text)
        if m:
            import datetime as _dt

            try:
                dt = _dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
                return int(dt.timestamp())
            except ValueError:
                pass
    raise ValidationError(f"{field or 'value'} must be an integer timestamp: {value!r}")


def normalize_kind(kind: Any) -> str:
    if not isinstance(kind, str) or not kind.strip():
        raise ValidationError("missing event kind")
    key = kind.strip()
    return KIND_ALIASES.get(key, KIND_ALIASES.get(key.lower(), key))


def normalize_stage(stage: Any) -> str:
    if not isinstance(stage, str) or not stage.strip():
        return "unknown"
    value = stage.strip().lower().replace("-", "_")
    if value in APPLIED_STAGES:
        return "applied"
    if value in RECEIVED_STAGES:
        return "received"
    return value


def is_applied_stage(stage: Any) -> bool:
    return normalize_stage(stage) == "applied"


def is_received_stage(stage: Any) -> bool:
    return normalize_stage(stage) == "received"


NON_PRODUCTION_ENV_TOKENS = {
    "test", "testing", "ci", "dev", "development", "sandbox", "staging",
    "integration", "joint-debug", "joint_debug", "debug", "qa", "uat", "preprod",
    "联调", "测试",
}
PRODUCTION_ENV_TOKENS = {"prod", "production", "live", "online", "正式", "生产"}


def environment_name(record: dict[str, Any]) -> str | None:
    value = first_present(record, ("environment", "env", "deploy_environment", "runtime_env"))
    return None if value is None else str(value).strip().lower().replace(" ", "-")


def is_production_environment(record: dict[str, Any]) -> bool:
    """Unknown environment defaults to production for backward-compatible logs."""

    env = environment_name(record)
    if env is None:
        return True
    if env in PRODUCTION_ENV_TOKENS:
        return True
    if env in NON_PRODUCTION_ENV_TOKENS:
        return False
    # Accept suffixed isolated environments such as testing-eu, ci-3, qa-team,
    # uat2, or dev-local without treating them as production.
    non_prod_prefixes = ("test", "testing", "ci", "dev", "development", "sandbox",
                         "staging", "integration", "qa", "uat", "preprod", "debug")
    return not any(
        env == prefix
        or env.startswith(prefix + "-")
        or env.startswith(prefix + "_")
        or (env.startswith(prefix) and env[len(prefix):len(prefix) + 1].isdigit())
        for prefix in non_prod_prefixes
    )


def route_generation(route: str | None, record: dict[str, Any] | None = None) -> str:
    if record:
        explicit = first_present(record, ("generation", "route_generation", "line_generation"))
        if explicit:
            text = str(explicit).strip().lower()
            if text in {"new", "next", "v2", "modern"}:
                return "new"
            if text in {"legacy", "old", "classic"}:
                return "legacy"
    if not route:
        return "unknown"
    name = str(route).strip().lower()
    if name in NEW_ROUTE_NAMES or name.startswith("next") or name.startswith("new"):
        return "new"
    if name.startswith(LEGACY_ROUTE_PREFIXES) or "legacy" in name or name.startswith("old"):
        return "legacy"
    return "unknown"


def logical_message_key(tenant: str, message: str) -> str:
    return f"{tenant}\x1f{message}"


def scope_key(tenant: str, consumer: str, topic: str, message: str | None = None) -> str:
    base = f"{tenant or ''}\x1f{consumer or ''}\x1f{topic or '*'}"
    return base if message is None else f"{base}\x1f{message}"


def subscription_key(tenant: str, consumer: str, topic: str) -> str:
    return scope_key(tenant, consumer, topic)


def parse_reference_scope(record: dict[str, Any]) -> list[str] | str:
    """Normalize a confirmation/assumption message scope.

    Returns ``"*"`` only for an explicit all-messages marker.  Otherwise returns
    a de-duplicated list of logical message IDs in stable input order.
    """

    raw = first_present(
        record,
        ("references", "reference", "messages", "message", "message_ids", "ref"),
    )
    scope = first_present(record, ("message_scope", "scope"))
    if isinstance(scope, dict):
        if scope.get("all") is True or scope.get("reference") == "*":
            return "*"
        raw = scope.get("messages", scope.get("references", raw))
    if raw is None:
        if record.get("all_messages") is True or record.get("reference") == "*":
            return "*"
        return []
    if isinstance(raw, (list, tuple, set)):
        values = [str(x) for x in raw]
    else:
        text = str(raw)
        if text == "*":
            return "*"
        # Python callers sometimes pass the same comma-separated syntax used by
        # the CLI.  Parse it at the domain boundary too.
        values = [part.strip() for part in text.split(",") if part.strip()] if "," in text else [text]
    seen: set[str] = set()
    result = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def scope_matches_messages(scope: list[str] | str, messages: Iterable[str]) -> bool:
    if scope == "*":
        return True
    wanted = set(scope)
    return all(message in wanted for message in messages)


def state_revision(state: dict[str, Any]) -> int:
    return int(state.get("revision", 0))
