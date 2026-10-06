import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")
        self.dispatcher = Actor("dispatcher", "dispatcher")

    def tearDown(self):
        self.tmp.cleanup()

    def running_fan(self, area_code, capacity):
        return self.service.create(
            self.admin, "ventilation", {"name": "fan-" + area_code, "area_code": area_code, "capacity": capacity}
        )

    def quota_of(self, view, area_code):
        for entry in view["quota"]:
            if entry["area_code"] == area_code:
                return entry
        return None

    def test_basic_allocation_water_fills_by_demand(self):
        self.running_fan("X", 100)
        self.service.create_area(self.admin, "A", "area-A", 60)
        self.service.create_area(self.admin, "B", "area-B", 50)
        view = self.service.ledger_view()
        self.assertEqual(view["capacity"], 100)
        self.assertEqual(self.quota_of(view, "A")["amount"], 60)
        self.assertEqual(self.quota_of(view, "A")["status"], "active")
        self.assertEqual(self.quota_of(view, "B")["amount"], 40)
        self.assertEqual(self.quota_of(view, "B")["status"], "queued")

    def test_queue_orders_by_alarm_priority_then_wait_time(self):
        self.running_fan("X", 100)
        self.service.create_area(self.admin, "A", "area-A", 60)
        self.service.create_area(self.admin, "B", "area-B", 50)
        # B alarms: higher priority jumps ahead of A.
        self.service.set_area_alarm(self.admin, "B", "alarm")
        view = self.service.ledger_view()
        self.assertEqual(self.quota_of(view, "B")["amount"], 50)
        self.assertEqual(self.quota_of(view, "B")["status"], "active")
        self.assertEqual(self.quota_of(view, "A")["amount"], 50)
        self.assertEqual(self.quota_of(view, "A")["status"], "queued")

    def test_alarm_change_voids_and_recomputes(self):
        self.running_fan("X", 100)
        self.service.create_area(self.admin, "A", "area-A", 80)
        self.service.create_area(self.admin, "B", "area-B", 50)
        # Initial: A is fully met, B gets the remainder.
        view = self.service.ledger_view()
        self.assertEqual(self.quota_of(view, "A")["amount"], 80)
        self.assertEqual(self.quota_of(view, "B")["amount"], 20)
        # B alarms: B jumps ahead, A is cut to 50.
        self.service.set_area_alarm(self.admin, "B", "alarm")
        view = self.service.ledger_view()
        self.assertEqual(self.quota_of(view, "B")["amount"], 50)
        self.assertEqual(self.quota_of(view, "A")["amount"], 50)
        self.assertEqual(self.quota_of(view, "A")["status"], "queued")
        # A alarms too: both at alarm priority now; A has waited longer, so A
        # recovers its full demand and B is pushed back to the queue.
        self.service.set_area_alarm(self.admin, "A", "alarm")
        view = self.service.ledger_view()
        self.assertEqual(self.quota_of(view, "A")["amount"], 80)
        self.assertEqual(self.quota_of(view, "A")["status"], "active")
        self.assertEqual(self.quota_of(view, "B")["amount"], 20)
        self.assertEqual(self.quota_of(view, "B")["status"], "queued")

    def test_emergency_transfer_from_lower_priority_area(self):
        self.running_fan("X", 100)
        self.service.create_area(self.admin, "A", "area-A", 80)
        self.service.create_area(self.admin, "B", "area-B", 50)
        self.service.set_area_alarm(self.admin, "B", "alarm")
        # A is lower priority and occupies 50; transfer 20 to B.
        transfer = self.service.request_transfer(self.dispatcher, "A", "B", 20, reason="gas spike")
        self.assertEqual(transfer["status"], "active")
        view = self.service.ledger_view()
        self.assertEqual(self.quota_of(view, "A")["amount"], 30)
        self.assertEqual(self.quota_of(view, "B")["amount"], 70)
        self.assertEqual(self.quota_of(view, "B")["source"], "transfer")

    def test_transfer_requires_lower_priority_donor(self):
        self.running_fan("X", 100)
        self.service.create_area(self.admin, "A", "area-A", 50)
        self.service.create_area(self.admin, "B", "area-B", 50)
        self.service.set_area_alarm(self.admin, "A", "alarm")
        # A is higher priority; B is lower. Transferring from B to A is allowed,
        # but transferring from A (higher) to B (lower) must be rejected.
        with self.assertRaises(ConflictError):
            self.service.request_transfer(self.dispatcher, "A", "B", 10)

    def test_transfer_requires_spare_capacity(self):
        self.running_fan("X", 100)
        self.service.create_area(self.admin, "A", "area-A", 100)
        self.service.create_area(self.admin, "B", "area-B", 100)
        self.service.set_area_alarm(self.admin, "B", "alarm")
        # A occupies 0 (all capacity went to B), so A has nothing to give.
        with self.assertRaises(ConflictError):
            self.service.request_transfer(self.dispatcher, "A", "B", 10)

    def test_recall_returns_recipient_to_queue(self):
        self.running_fan("X", 100)
        self.service.create_area(self.admin, "A", "area-A", 80)
        self.service.create_area(self.admin, "B", "area-B", 50)
        self.service.set_area_alarm(self.admin, "B", "alarm")
        transfer = self.service.request_transfer(self.dispatcher, "A", "B", 20)
        # A itself alarms: it immediately takes back the transferred wind.
        self.service.set_area_alarm(self.admin, "A", "alarm")
        view = self.service.ledger_view()
        self.assertEqual(self.quota_of(view, "A")["amount"], 80)
        self.assertEqual(self.quota_of(view, "A")["status"], "active")
        self.assertEqual(self.quota_of(view, "B")["amount"], 20)
        self.assertEqual(self.quota_of(view, "B")["status"], "queued")
        recalled = next(t for t in view["transfers"] if t["id"] == transfer["id"])
        self.assertEqual(recalled["status"], "recalled")

    def test_explicit_recall(self):
        self.running_fan("X", 100)
        self.service.create_area(self.admin, "A", "area-A", 80)
        self.service.create_area(self.admin, "B", "area-B", 50)
        self.service.set_area_alarm(self.admin, "B", "alarm")
        transfer = self.service.request_transfer(self.dispatcher, "A", "B", 20)
        self.service.recall_transfer(self.admin, transfer["id"])
        view = self.service.ledger_view()
        self.assertEqual(self.quota_of(view, "A")["amount"], 50)
        self.assertEqual(self.quota_of(view, "B")["amount"], 50)
        for t in view["transfers"]:
            self.assertEqual(t["status"], "recalled")

    def test_fan_status_change_recomputes(self):
        fan = self.running_fan("X", 100)
        self.service.create_area(self.admin, "A", "area-A", 100)
        view = self.service.ledger_view()
        self.assertEqual(self.quota_of(view, "A")["amount"], 100)
        # Fan stops: capacity drops to 0, A becomes queued.
        self.service.transition(self.admin, fan["id"], "stop")
        view = self.service.ledger_view()
        self.assertEqual(view["capacity"], 0)
        self.assertEqual(self.quota_of(view, "A")["amount"], 0)
        self.assertEqual(self.quota_of(view, "A")["status"], "queued")

    def test_backfill_from_running_fan_capacity(self):
        self.running_fan("N", 60)
        self.running_fan("S", 40)
        self.running_fan("N", 20)  # same area N
        result = self.service.backfill_ledger(self.admin)
        self.assertEqual(result["backfilled"], 2)
        view = self.service.ledger_view()
        self.assertEqual(view["capacity"], 120)
        self.assertEqual(self.quota_of(view, "N")["amount"], 80)
        self.assertEqual(self.quota_of(view, "S")["amount"], 40)
        # Backfill is idempotent: running it again creates nothing new.
        again = self.service.backfill_ledger(self.admin)
        self.assertEqual(again["backfilled"], 0)

    def test_concurrent_transfer_first_wins_then_retry(self):
        self.running_fan("X", 100)
        self.service.create_area(self.admin, "A", "area-A", 80)
        self.service.create_area(self.admin, "B", "area-B", 50)
        self.service.set_area_alarm(self.admin, "B", "alarm")

        results = {"d1": None, "d2": None}
        barrier = threading.Barrier(2)

        def transfer(actor, key):
            barrier.wait()
            try:
                transfer = self.service.request_transfer(actor, "A", "B", 20)
                results[key] = ("ok", transfer["id"])
            except Exception as exc:
                results[key] = ("fail", type(exc).__name__)

        t1 = threading.Thread(target=transfer, args=(Actor("d1", "dispatcher"), "d1"))
        t2 = threading.Thread(target=transfer, args=(Actor("d2", "dispatcher"), "d2"))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # Both dispatchers committed: the loser hit a version conflict and retried.
        self.assertEqual(results["d1"][0], "ok")
        self.assertEqual(results["d2"][0], "ok")
        view = self.service.ledger_view()
        self.assertEqual(self.quota_of(view, "A")["amount"], 10)
        self.assertEqual(self.quota_of(view, "B")["amount"], 90)
        self.assertEqual(len([t for t in view["transfers"] if t["status"] == "active"]), 2)

    def test_invalid_alarm_level_rejected(self):
        self.running_fan("X", 100)
        self.service.create_area(self.admin, "A", "area-A", 50)
        with self.assertRaises(ValidationError):
            self.service.set_area_alarm(self.admin, "A", "critical-ish")


if __name__ == "__main__":
    unittest.main()
