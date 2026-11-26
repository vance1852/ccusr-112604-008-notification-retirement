"""Behavioral tests for legacy-line retirement coordination."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from retirement import RetirementEngine, RetirementService, SyntheticLine, SyntheticReceiver
from retirement.service import load_inventory, load_jsonl

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


class SampleTimelineTests(unittest.TestCase):
    def setUp(self):
        self.inventory = load_inventory(SAMPLES / "inventory.json")
        self.shift = load_jsonl(SAMPLES / "shift-log.jsonl")
        self.clean = load_jsonl(SAMPLES / "clean-delegate.jsonl")

    def test_shift_scope_receipts_custody_and_shared_resource(self):
        engine = RetirementEngine(self.inventory)
        results = engine.ingest(self.shift)
        self.assertFalse(any(r["duplicate"] for r in results))

        # A next-route "received" probe is not business evidence.  The late
        # legacy applied receipt nevertheless covers the logical message.
        southern_confirm = engine.plan_retirement()["confirmations"][0]
        self.assertEqual(southern_confirm["consumer"], "southern")
        self.assertFalse(southern_confirm["formal"])
        self.assertFalse(southern_confirm["evidence_ready"])
        coverage = engine.legacy_receipt_covers("T-a", "n8", consumer="southern", topic="case")
        self.assertTrue(coverage["covered"])
        self.assertEqual(coverage["receipt"]["stage"], "applied")
        self.assertEqual(coverage["receipt"]["route"], "legacy-voice")

        # Before relay's received fact, the open item routes through the active
        # delegation.  A valid received collection creates durable custody.
        routing_before = engine.route_message("T-b", "n8", at=27)[0]
        self.assertEqual(routing_before["action"], "deliver_to_authorized_proxy")
        self.assertEqual(routing_before["deliver_to"], "relay")
        custody = engine.route_message("T-b", "n8", at=31)[0]
        self.assertEqual(custody["action"], "await_custodian_application")
        self.assertEqual(custody["follow_up_with"], "relay")

        # The post-expiry relay applied receipt is valid because relay had
        # already validly collected this exact business item.
        trace = engine.trace_ticket(tenant="T-b", message="n8")
        self.assertTrue(trace["completed"])
        self.assertEqual(trace["last_valid_receipt"]["actor"], "relay")
        self.assertEqual(trace["last_valid_receipt"]["stage"], "applied")
        self.assertEqual(trace["last_valid_receipt"]["authorizations"][
            "T-b\x1fcoast\x1fstatus\x1fn8"]["basis"], "custody_after_expiry")

        # At the time off2 was requested, T-b still had no applied receipt.
        at_request = engine.resource_plan("return-mail", at=20)
        types = {(c["type"], c.get("tenant")) for c in at_request["blocking_claims"]}
        self.assertIn(("open_legacy_obligation", "T-b"), types)
        self.assertTrue(at_request["shared_route"])
        self.assertTrue(at_request["disable_blocking_claims"])

        # The observed still_active effect does not remove the resource.
        self.assertEqual(engine.resource_plan("return-mail")["observed_state"], "active")

        # At the end, message work is complete, but undecided subscriptions in
        # both tenants still claim the shared physical mail entry.
        final_mail = engine.resource_plan("return-mail")
        tenants = {c.get("tenant") for c in final_mail["blocking_claims"]
                   if c["type"] == "subscription_contract"}
        self.assertEqual(tenants, {"T-a", "T-b"})

        rollback = engine.rollback_assessment("poll-legacy")
        self.assertFalse(rollback["plain_restore_possible"])
        self.assertTrue(any(x["action"] == "rebuild_resource" for x in rollback["rebuild_required"]))
        self.assertTrue(any(x["code"] == "runtime_state_not_reconstructable"
                            for x in rollback["irreparable_gaps"]))

    def test_clean_delegate_and_formal_window(self):
        engine = RetirementEngine(self.inventory)
        engine.ingest(self.clean)
        report = engine.plan_retirement()
        confirm = report["confirmations"][0]
        self.assertEqual(confirm["status"], "formal")
        self.assertTrue(confirm["formal"])
        self.assertEqual(confirm["window_ends_at"], 22)

        trace = engine.trace_ticket(tenant="T-b", message="a01")
        self.assertTrue(trace["completed"])
        self.assertEqual(trace["last_valid_receipt"]["actor"], "relay")
        self.assertEqual(trace["last_valid_receipt"]["authorizations"][
            "T-b\x1fcoast\x1fstatus\x1fa01"]["basis"], "delegation")

        poll = engine.resource_plan("poll-text")
        self.assertEqual(poll["observed_state"], "disabled")
        restore = engine.rollback_assessment("poll-text")
        self.assertTrue(restore["plain_restore_possible"])
        self.assertEqual(restore["irreparable_gaps"], [])

    def test_duplicate_events_and_restart_continue_decisions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            service = RetirementService(path, self.inventory)
            first = service.ingest(self.shift)
            revision = service.engine.state["revision"]
            self.assertEqual(len(first), len(self.shift))

            service = RetirementService(path)
            duplicated = service.ingest(self.shift)
            self.assertTrue(all(item["duplicate"] for item in duplicated))
            self.assertEqual(service.engine.state["revision"], revision)

            service.confirm("coast", "status", "n8", route="next", at=50)
            service.engine.advance_migration(tenant="T-b", consumer="coast", topic="status",
                                              messages=["n8"], execute=True, at=50)
            service.save()
            reopened = RetirementService(path)
            sub = reopened.engine.state["subscriptions"]["T-b\x1fcoast\x1fstatus"]
            self.assertIsNone(sub["disposition"])  # n8 scope must not expand to the whole topic
            self.assertEqual(sub["scope_actions"][0]["scope"], ["n8"])
            routing = reopened.engine.route_message("T-b", "n8", at=51)[0]
            self.assertEqual(routing["action"], "suppress_redelivery")
            self.assertGreaterEqual(reopened.engine.state["revision"], revision + 2)


class RoutingAndDelegationTests(unittest.TestCase):
    def setUp(self):
        self.engine = RetirementEngine({})
        self.engine.register_consumer("coast", "T-b", ["status"], at=0)

    def test_expired_delegate_without_custody_routes_back_to_principal(self):
        self.engine.delegate("coast", "relay", topic="status", start=1, until=10, at=1)
        self.engine.deliver("T-b", "m1", "coast", topic="status", route="legacy-text", at=12)
        routing = self.engine.route_message("T-b", "m1", at=12)[0]
        self.assertEqual(routing["action"], "deliver_direct_legacy")
        self.assertEqual(routing["deliver_to"], "coast")
        self.assertIn("delegation_inactive", routing["reason"])

        # An expired proxy receipt without earlier valid collection is invalid.
        receipt = self.engine.receipt("T-b", "m1", "relay", "applied",
                                      route="legacy-text", at=12)
        self.assertFalse(receipt["result"]["duplicate"])
        stored = self.engine.state["receipts"][-1]
        self.assertFalse(stored["valid"])
        self.assertFalse(self.engine.trace_ticket(tenant="T-b", message="m1")["completed"])

    def test_old_applied_receipt_suppresses_new_redelivery(self):
        self.engine.deliver("T-a", "n8", "southern", topic="case", route="legacy-voice", at=3)
        self.engine.receipt("T-a", "n8", "southern", "applied", route="legacy-voice", at=19)
        self.engine.deliver("T-a", "n8", "southern", topic="case", route="next", at=21)
        routing = self.engine.route_message("T-a", "n8", at=22)[0]
        self.assertEqual(routing["action"], "suppress_redelivery")
        self.assertEqual(routing["receipt_id"], "receipt-3")
        coverage = self.engine.legacy_receipt_covers("T-a", "n8")
        self.assertTrue(coverage["covered"])

    def test_confirmation_does_not_expand_consumer_or_message_scope(self):
        self.engine.register_consumer("other", "T-a", ["case"], at=0)
        self.engine.deliver("T-a", "n8", "southern", topic="case", route="next", at=1)
        self.engine.deliver("T-a", "n9", "southern", topic="case", route="next", at=2)
        self.engine.confirm("southern", "case", "n8", at=3)
        self.assertIsNotNone(self.engine.confirmation_covering("southern", "case", ["n8"]))
        self.assertIsNone(self.engine.confirmation_covering("southern", "case", ["n9"]))
        self.assertIsNone(self.engine.confirmation_covering("other", "case", ["n8"]))

    def test_probe_received_and_silent_poll_do_not_complete_work(self):
        engine = RetirementEngine({})
        engine.deliver("T", "m", "c", topic="t", route="legacy-poll", at=1)
        engine.record({"at": 2, "kind": "probe", "resource": "legacy-poll", "status": "healthy"})
        engine.receipt("T", "m", "c", "received", route="legacy-poll", at=3)
        engine.line_action("legacy-poll", "pause", operation="p1", at=4)
        engine.line_effect("legacy-poll", "disabled", operation="p1", at=5)
        view = engine.obligation_view("T\x1fc\x1ft\x1fm")
        self.assertFalse(view["completed"])
        self.assertEqual(view["valid_receipt_count"], 1)
        routing = engine.route_message("T", "m", at=6)[0]
        self.assertNotEqual(routing["action"], "suppress_redelivery")

    def test_tenant_cancellation_only_releases_that_tenant_subscription(self):
        inventory = {
            "routes": [{"route": "shared", "resources": ["return-mail"]}],
            "consumers": [
                {"consumer": "a", "tenant": "TA", "topics": ["case"]},
                {"consumer": "b", "tenant": "TB", "topics": ["case"]},
            ],
            "tenant_contract": [],
        }
        engine = RetirementEngine(inventory)
        engine.command("tenant_cancelled", {"tenant": "TA"}, at=1)
        plan = engine.resource_plan("return-mail", at=2)
        claims = [c for c in plan["claims"] if c["type"] == "subscription_contract"]
        self.assertEqual([c["tenant"] for c in claims], ["TB"])
        ta_sub = engine.subscription_view("TA\x1fa\x1fcase", at=2)
        self.assertTrue(ta_sub["tenant_cancelled"])
        self.assertTrue(ta_sub["released_for_resource_removal"])

    def test_revocation_is_immediate(self):
        self.engine.delegate("coast", "relay", topic="status", start=1, until=100, at=1)
        self.engine.revoke_delegation(consumer="coast", proxy="relay", topic="status", at=5)
        self.engine.deliver("T-b", "m2", "coast", topic="status", route="legacy-text", at=6)
        routing = self.engine.route_message("T-b", "m2", at=6)[0]
        self.assertEqual(routing["deliver_to"], "coast")
        self.assertNotEqual(routing.get("deliver_to"), "relay")


class AdvanceConcurrencyTests(unittest.TestCase):
    def test_latest_receipt_changes_what_execution_allows(self):
        inventory = {
            "routes": [{"route": "legacy-text", "resources": ["poll-text"]}],
            "consumers": [{"consumer": "coast", "tenant": "T-b", "topics": ["status"]}],
            "tenant_contract": [{"tenant": "T-b", "legacy_until": 3, "confirm_seconds": 10}],
        }
        engine = RetirementEngine(inventory)
        engine.deliver("T-b", "m", "coast", topic="status", route="legacy-text", at=1)

        before = engine.advance_decommission(resources=["poll-text"], at=2, execute=False)
        self.assertEqual(before["actions"][0]["action"], "pause")
        # Dedicated poll resource can enter reversible pause, but cannot retire.
        execute_pause = engine.advance_decommission(resources=["poll-text"], at=2, execute=True)
        self.assertTrue(execute_pause["actions"])
        self.assertEqual(engine.resource_plan("poll-text")["observed_state"], "disabled")
        blocked_retire = engine.advance_decommission(resources=["poll-text"], at=3, execute=True)
        self.assertTrue(any(x["action"] == "hold_removal" for x in blocked_retire["deferred"]))
        self.assertEqual(engine.resource_plan("poll-text")["observed_state"], "disabled")

        # A late valid applied receipt changes the latest fulfillable duty.
        engine.receipt("T-b", "m", "coast", "applied", route="legacy-text", at=4)
        retired = engine.advance_decommission(resources=["poll-text"], at=5, execute=True)
        self.assertEqual(retired["actions"][0]["action"], "retire")
        self.assertEqual(engine.resource_plan("poll-text")["observed_state"], "removed")

    def test_shared_resource_requires_last_tenant_claim_release(self):
        inventory = {
            "routes": [
                {"route": "legacy-voice", "resources": ["return-mail"]},
                {"route": "legacy-text", "resources": ["return-mail"]},
            ],
            "consumers": [
                {"consumer": "a", "tenant": "TA", "topics": ["case"]},
                {"consumer": "b", "tenant": "TB", "topics": ["status"]},
            ],
            "tenant_contract": [
                {"tenant": "TA", "legacy_until": 1, "confirm_seconds": 1},
                {"tenant": "TB", "legacy_until": 1, "confirm_seconds": 1},
            ],
        }
        engine = RetirementEngine(inventory)
        engine.deliver("TA", "1", "a", topic="case", route="legacy-voice", at=1)
        engine.deliver("TB", "2", "b", topic="status", route="legacy-text", at=1)
        engine.receipt("TA", "1", "a", "applied", at=2)
        plan = engine.resource_plan("return-mail", at=3)
        tenants = {c.get("tenant") for c in plan["disable_blocking_claims"]}
        self.assertIn("TB", tenants)
        self.assertNotIn("TA", tenants)
        self.assertFalse(plan["disable_eligible"])
        engine.receipt("TB", "2", "b", "applied", at=4)
        # No contract rows means no subscription claim; all work is complete.
        self.assertTrue(engine.resource_plan("return-mail", at=5)["disable_eligible"])


class ReviewRegressionTests(unittest.TestCase):
    def test_wildcard_assumption_before_future_delivery(self):
        engine = RetirementEngine({})
        engine.assume("coast", "status", "*", tenant="T-b", at=1)
        engine.deliver("T-b", "later", "coast", topic="status", route="legacy-text", at=2)
        key = "T-b\x1fcoast\x1fstatus\x1flater"
        view = engine.obligation_view(key, at=3)
        self.assertTrue(view["assumed"])
        self.assertTrue(view["legacy_released"])
        self.assertEqual(view["routing"]["action"], "deliver_to_consumer_new_route")

    def test_logical_old_receipt_covers_topic_rekey(self):
        engine = RetirementEngine({})
        engine.deliver("T", "n8", "southern", topic="case", route="legacy-voice", at=1)
        engine.receipt("T", "n8", "southern", "applied", route="legacy-voice", at=2)
        engine.deliver("T", "n8", "southern", topic="*", route="next", at=3)
        rekeyed = "T\x1fsouthern\x1f*\x1fn8"
        self.assertEqual(engine.route_obligation(rekeyed, at=4)["action"], "suppress_redelivery")

    def test_receipt_correction_revokes_custody(self):
        engine = RetirementEngine({})
        engine.delegate("coast", "relay", topic="status", start=1, until=10, at=1)
        engine.deliver("T", "m", "coast", topic="status", route="legacy", at=2)
        engine.receipt("T", "m", "relay", "received", at=3)
        receipt_id = engine.state["receipts"][-1]["receipt_id"]
        engine.record({"at": 20, "kind": "receipt_correction", "receipt_id": receipt_id, "valid": False})
        engine.receipt("T", "m", "relay", "applied", at=21)
        last = engine.state["receipts"][-1]
        self.assertFalse(last["valid"])
        self.assertFalse(engine.trace_ticket(tenant="T", message="m")["completed"])

    def test_named_confirmation_cannot_satisfy_whole_subscription(self):
        inventory = {
            "routes": [{"route": "legacy", "resources": ["poll"]}],
            "consumers": [{"consumer": "c", "tenant": "T", "topics": ["t"]}],
            "tenant_contract": [{"tenant": "T", "legacy_until": 100, "confirm_seconds": 1}],
        }
        engine = RetirementEngine(inventory)
        engine.confirm("c", "t", "m1", at=1)
        self.assertIsNone(engine.confirmation_covering("c", "t", "*", at=20))
        result = engine.advance_migration(tenant="T", execute=True, at=20)
        self.assertEqual(result["planned_or_applied"], [])
        self.assertIsNone(engine.state["subscriptions"]["T\x1fc\x1ft"]["disposition"])

    def test_blanket_decommission_skips_new_route_resource(self):
        inventory = {
            "routes": [
                {"route": "legacy", "resources": ["poll"]},
                {"route": "next", "resources": ["new-hook"]},
            ],
            "consumers": [{"consumer": "c", "tenant": "T", "topics": ["t"]}],
            "tenant_contract": [{"tenant": "T", "legacy_until": 1, "confirm_seconds": 1}],
        }
        engine = RetirementEngine(inventory)
        result = engine.advance_decommission(execute=False, at=2)
        names = {action["resource"] for action in result["actions"]}
        self.assertIn("poll", names)
        self.assertNotIn("new-hook", names)

    def test_route_less_open_obligation_still_blocks_removal(self):
        inventory = {
            "routes": [{"route": "legacy-text", "resources": ["poll-text"]}],
            "consumers": [{"consumer": "c", "tenant": "T", "topics": ["t"]}],
            "tenant_contract": [],
        }
        engine = RetirementEngine(inventory)
        engine.deliver("T", "m", "c", topic="t", at=1)
        engine.transition_subscriptions("stop", tenant="T", consumer="c", topic="t", at=2)
        engine.advance_decommission(resources=["poll-text"], execute=True, at=3)
        blocked = engine.advance_decommission(resources=["poll-text"], execute=True, at=4)
        self.assertTrue(any(x["action"] == "hold_removal" for x in blocked["deferred"]))
        self.assertEqual(engine.resource_plan("poll-text")["observed_state"], "disabled")

    def test_cross_tenant_single_route_is_effectively_shared(self):
        inventory = {
            "routes": [{"route": "legacy", "resources": ["poll"]}],
            "consumers": [
                {"consumer": "a", "tenant": "TA", "topics": ["t"]},
                {"consumer": "b", "tenant": "TB", "topics": ["t"]},
            ],
            "tenant_contract": [],
        }
        engine = RetirementEngine(inventory)
        engine.deliver("TA", "1", "a", topic="t", route="legacy", at=1)
        engine.deliver("TB", "2", "b", topic="t", route="legacy", at=1)
        plan = engine.resource_plan("poll", at=2)
        self.assertTrue(plan["effective_shared"])
        self.assertEqual(len(plan["disable_blocking_claims"]), 4)
        result = engine.advance_decommission(execute=True, at=3)
        self.assertEqual(result["actions"], [])
        self.assertEqual(result["deferred"][0]["action"], "hold_reversible_stop")


class SyntheticStateTests(unittest.TestCase):
    def test_receiver_idempotency_and_line_independence(self):
        receiver = SyntheticReceiver()
        receiver.register("relay", "coast-proxy", ["status"])
        first = receiver.deliver("relay", "T-b", "a1", at=1)
        second = receiver.deliver("relay", "T-b", "a1", at=2)
        self.assertFalse(first["duplicate_logical_message"])
        self.assertTrue(second["duplicate_logical_message"])
        r1 = receiver.receipt("T-b", "a1", actor="relay", stage="applied", at=3)
        r2 = receiver.receipt("T-b", "a1", actor="relay", stage="applied", at=4)
        self.assertFalse(r1["idempotent_replay"])
        self.assertTrue(r2["idempotent_replay"])
        self.assertTrue(receiver.state()["messages"]["T-b/a1"]["applied"])

        line = SyntheticLine()
        line.add_resource("return-mail", routes=["legacy-text"])
        action = line.action("return-mail", "disable", operation="o1")
        self.assertEqual(line.operations["o1"]["status"], "requested")
        # Creating an action does not change line state until an effect occurs.
        self.assertEqual(line.resources["return-mail"]["state"], "active")
        effect = line.execute(action)
        self.assertEqual(effect["state"], "removed" if False else "disabled")
        self.assertEqual(line.resources["return-mail"]["state"], "disabled")

    def test_state_file_is_valid_json_after_commands(self):
        inventory = {"routes": [], "consumers": [], "tenant_contract": []}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            service = RetirementService(path, inventory)
            service.register_consumer("new", "T-x", ["alert"])
            json.loads(path.read_text(encoding="utf-8"))
            reopened = RetirementService(path)
            self.assertIn("new", reopened.engine.state["registry"]["consumers"])


if __name__ == "__main__":
    unittest.main()
