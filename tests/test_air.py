import tempfile
import threading
import unittest
from pathlib import Path

from src.air import ALARM_RANK, allocate
from src.domain import Actor, ConflictError, PermissionDenied, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class AirLedgerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "air.db"), RuleEngine()
        )
        self.admin = Actor("dispatcher-1", "dispatcher")
        self.other = Actor("dispatcher-2", "dispatcher")
        # 容量在风机回填前显式建账（容量同步属安全/管理员操作）
        self.service.air.set_capacity(Actor("admin-1", "admin"), 100)

    def tearDown(self):
        self.tmp.cleanup()

    def zone(self, area, demand, ident=None):
        return self.service.air.register_zone(self.admin, area, demand, ident)

    def request(self, zone_id, amount=None):
        return self.service.air.submit_request(self.admin, zone_id, amount)

    def served_map(self):
        ledger = self.service.air.ledger()
        return {item["zone_id"]: item for item in ledger["queue"]}

    def test_approved_demand_occupies_capacity_and_overrun_queues(self):
        a = self.zone("A", 60)
        b = self.zone("B", 60)
        self.request(a["id"])
        self.request(b["id"])
        items = self.served_map()
        self.assertEqual(items[a["id"]]["state"], "filled")
        self.assertEqual(items[a["id"]]["occupied"], 60)
        self.assertEqual(items[b["id"]]["state"], "partial")
        self.assertEqual(items[b["id"]]["occupied"], 40)
        self.assertEqual(self.service.air.ledger()["available"], 0)

    def test_alarm_jumps_queue_by_wait_time_then_severity(self):
        a = self.zone("A", 40)
        b = self.zone("B", 40)
        c = self.zone("C", 40)
        self.request(a["id"])
        self.request(b["id"])
        self.request(c["id"])  # 容量100: A40 B40 C20
        self.assertEqual(self.served_map()[c["id"]]["state"], "partial")

        # C 报警，超过早排队的 A/B 抢占
        self.service.air.change_alarm(self.admin, c["id"], "alarm")
        items = self.served_map()
        self.assertEqual(items[c["id"]]["occupied"], 40)
        self.assertEqual(items[c["id"]]["state"], "filled")

        # B 升为 critical：级别更重，先于 alarm
        self.service.air.change_alarm(self.admin, b["id"], "critical")
        items = self.served_map()
        self.assertEqual(items[b["id"]]["occupied"], 40)
        self.assertEqual(items[c["id"]]["occupied"], 40)

    def test_emergency_preemption_is_booked_as_loan(self):
        a = self.zone("A", 60)
        b = self.zone("B", 60)
        self.request(a["id"])
        self.request(b["id"])
        self.service.air.change_alarm(self.admin, b["id"], "alarm")
        items = self.served_map()
        self.assertEqual(items[b["id"]]["occupied"], 60)
        self.assertEqual(items[a["id"]]["occupied"], 40)
        loans = [loan for loan in self.service.air.store.list_loans("active")]
        self.assertEqual(len(loans), 1)
        self.assertEqual(loans[0]["donor_id"], a["id"])
        self.assertEqual(loans[0]["receiver_id"], b["id"])
        self.assertEqual(loans[0]["amount"], 20)

    def test_donor_alarm_recalls_loan_immediately(self):
        a = self.zone("A", 60)
        b = self.zone("B", 60)
        self.request(a["id"])
        self.request(b["id"])
        self.service.air.change_alarm(self.admin, b["id"], "alarm")
        self.assertEqual(self.served_map()[a["id"]]["occupied"], 40)

        # 被让出的 A 自己报警，立刻收回
        self.service.air.change_alarm(self.admin, a["id"], "alarm")
        items = self.served_map()
        self.assertEqual(items[a["id"]]["occupied"], 60)
        self.assertEqual(items[b["id"]]["occupied"], 40)
        self.assertTrue(items[a["id"]]["protected"] >= 20)
        loan = self.service.air.store.list_loans("recalled")[0]
        self.assertEqual((loan["donor_id"], loan["receiver_id"]), (a["id"], b["id"]))

        # 即使 B 升到 critical，A 报警期间也拿不回这笔风
        self.service.air.change_alarm(self.admin, b["id"], "critical")
        items = self.served_map()
        self.assertEqual(items[a["id"]]["occupied"], 60)
        self.assertEqual(items[b["id"]]["occupied"], 40)

        # A 报警解除，保护消失，B 重新占满
        self.service.air.change_alarm(self.admin, a["id"], "none")
        items = self.served_map()
        self.assertEqual(items[b["id"]]["occupied"], 60)
        self.assertEqual(items[a["id"]]["occupied"], 40)

    def test_alarm_change_voids_occupancy_and_requeues_with_original_wait(self):
        a = self.zone("A", 100)
        b = self.zone("B", 50)
        self.request(a["id"])
        self.request(b["id"])
        self.assertEqual(self.served_map()[b["id"]]["occupied"], 0)

        self.service.air.change_alarm(self.admin, b["id"], "alarm")
        self.assertEqual(self.served_map()[b["id"]]["occupied"], 50)

        # 级别一变，原占用立即作废；降到无报警后，排队时间沿用首次入队（A 先、B 后）
        self.service.air.change_alarm(self.admin, b["id"], "none")
        items = self.served_map()
        self.assertEqual(items[a["id"]]["occupied"], 100)
        self.assertEqual(items[b["id"]]["occupied"], 0)
        requests = self.service.air.store.open_requests()
        b_request = next(r for r in requests if r["zone_id"] == b["id"])
        self.assertIsNotNone(b_request["enqueued_at"])

    def test_explicit_yield_takes_only_named_amount_and_is_idempotent(self):
        # 总容量120：A80 B70；B 报警后 B 占 70；C（critical，30）随后入列，
        # 自然归因下 A 只剩 20、C 由 A 让出 30 填满。点名从 B 再让 10 给 C，
        # B 降到 60，释放的风回到 A，A 回到 30。
        self.service.air.set_capacity(Actor("admin-2", "admin"), 120)
        a = self.zone("A", 80)
        b = self.zone("B", 70)
        self.request(a["id"])
        self.request(b["id"])
        self.service.air.change_alarm(self.admin, b["id"], "alarm")
        c = self.zone("C", 30)
        self.request(c["id"])
        self.service.air.change_alarm(self.admin, c["id"], "critical")
        before = self.served_map()
        self.assertEqual(before[b["id"]]["occupied"], 70)
        self.assertEqual(before[c["id"]]["occupied"], 30)

        loan = self.service.air.yield_air(self.admin, b["id"], c["id"], 10, yield_key="y-1")
        self.assertEqual(loan["amount"], 10)
        self.assertTrue(loan["forced"])
        after = self.served_map()
        self.assertEqual(after[b["id"]]["occupied"], before[b["id"]]["occupied"] - 10)
        self.assertEqual(after[c["id"]]["occupied"], before[c["id"]]["occupied"])
        # before: A20 B70 C30。B 点名让 C 10 后：B 降到 60；这 10 中 A 对 B 的自然让渡
        # 同步消失、A 对 C 的让渡也少 10，共 20 回到 A：A40 B60 C30。
        self.assertEqual((after[a["id"]]["occupied"], after[b["id"]]["occupied"],
                          after[c["id"]]["occupied"]), (40, 60, 30))

        # 同 yield_key 重放返回首笔账，不重复划转
        again = self.service.air.yield_air(self.other, b["id"], c["id"], 10, yield_key="y-1")
        self.assertEqual(again["loan_key"], loan["loan_key"])
        active = self.service.air.store.list_loans("active")
        self.assertEqual(sum(1 for l in active if l["loan_key"] == loan["loan_key"]), 1)

    def test_yield_requires_lower_alarm_level(self):
        a = self.zone("A", 50)
        b = self.zone("B", 50)
        self.request(a["id"])
        self.request(b["id"])
        with self.assertRaises(ValidationError):
            self.service.air.yield_air(self.admin, a["id"], b["id"], 10)

    def test_concurrent_identical_yield_only_first_writes(self):
        # 总容量100：A70 B70，B 报警后 B 占 70、A 30，B 仍缺 20，存在让出空间
        a = self.zone("A", 70)
        b = self.zone("B", 70)
        self.request(a["id"])
        self.request(b["id"])
        self.service.air.change_alarm(self.admin, b["id"], "alarm")
        results = []
        barrier = threading.Barrier(2)

        def submit(actor):
            barrier.wait()
            try:
                loan = self.service.air.yield_air(actor, a["id"], b["id"], 15)
                results.append(("ok", loan["loan_key"]))
            except ConflictError as exc:
                results.append(("conflict", str(exc)))
            except Exception as exc:  # pragma: no cover - 测试辅助
                results.append(("error", "%s: %s" % (type(exc).__name__, exc)))

        t1 = threading.Thread(target=submit, args=(self.admin,))
        t2 = threading.Thread(target=submit, args=(self.other,))
        t1.start()
        t2.start()
        t1.join(5)
        t2.join(5)
        self.assertEqual(len(results), 2)
        # 同一对 donor/receiver 只存在一笔活动账，先落账者生效，另一个看到同一笔账
        keys = {loan_key for status, loan_key in results if status == "ok"}
        self.assertEqual(keys, {"%s:%s" % (a["id"], b["id"])})
        active = self.service.air.store.list_loans("active")
        self.assertEqual(len(active), 1)

    def test_capacity_release_is_reallocated_to_queue(self):
        a = self.zone("A", 80)
        b = self.zone("B", 60)
        self.request(a["id"])
        self.request(b["id"])
        self.assertEqual(self.served_map()[b["id"]]["occupied"], 20)
        self.service.air.set_capacity(Actor("admin-2", "admin"), 140)
        items = self.served_map()
        self.assertEqual(items[a["id"]]["occupied"], 80)
        self.assertEqual(items[b["id"]]["occupied"], 60)

    def test_permissions_are_checked(self):
        with self.assertRaises(PermissionDenied):
            self.service.air.register_zone(Actor("v", "viewer"), "Z", 10)

    def test_wait_time_tiebreaks_same_alarm_level(self):
        # 两个同级报警区域，等待时间早的先得风
        a = self.zone("A", 60, "zone-a")
        b = self.zone("B", 60, "zone-b")
        self.request(a["id"])
        self.request(b["id"])
        self.service.air.change_alarm(self.admin, a["id"], "alarm")
        self.service.air.change_alarm(self.admin, b["id"], "alarm")
        items = self.served_map()
        self.assertEqual(items["zone-a"]["occupied"], 60)
        self.assertEqual(items["zone-b"]["occupied"], 40)


class BackfillTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "bf.db"), RuleEngine()
        )
        self.admin = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def test_backfill_from_running_fans_only_once(self):
        self.service.create(self.admin, "ventilation", {"name": "f1", "area_code": "A", "capacity": 300})
        stopped = self.service.create(self.admin, "ventilation", {"name": "f2", "area_code": "B", "capacity": 200})
        self.service.transition(self.admin, stopped["id"], "stop")
        degraded = self.service.create(self.admin, "ventilation", {"name": "f3", "area_code": "C", "capacity": 50})
        self.service.transition(self.admin, degraded["id"], "degrade", {"reason": "maint"})

        outcome = self.service.backfill_air_capacity(self.admin)
        self.assertEqual(outcome["capacity"], 300)
        self.assertTrue(outcome["backfilled"])
        # 再来风机也不会覆盖已建的账
        again = self.service.backfill_air_capacity(self.admin)
        self.assertFalse(again["backfilled"])
        self.assertEqual(again["capacity"], 300)


class AllocatorPureTest(unittest.TestCase):
    def test_pure_allocator_basic(self):
        zones = [
            {"id": "a", "approved_demand": 60, "alarm": "none", "epoch": 0},
            {"id": "b", "approved_demand": 60, "alarm": "alarm", "epoch": 0},
        ]
        requests = [
            {"id": "r1", "zone_id": "a", "amount": 60, "enqueued_at": "2026-10-06T09:00:00+00:00"},
            {"id": "r2", "zone_id": "b", "amount": 60, "enqueued_at": "2026-10-06T09:01:00+00:00"},
        ]
        result = allocate(zones, requests, [], 100)
        self.assertEqual(result.served["r1"], 40)
        self.assertEqual(result.served["r2"], 60)
        self.assertEqual(result.preempted[("a", "b")], 20)

    def test_recalled_protection_blocks_receiver(self):
        zones = [
            {"id": "a", "approved_demand": 60, "alarm": "alarm", "epoch": 2},
            {"id": "b", "approved_demand": 60, "alarm": "critical", "epoch": 0},
        ]
        requests = [
            {"id": "r1", "zone_id": "a", "amount": 60, "enqueued_at": "2026-10-06T09:00:00+00:00"},
            {"id": "r2", "zone_id": "b", "amount": 60, "enqueued_at": "2026-10-06T09:01:00+00:00"},
        ]
        loans = [{
            "loan_key": "a:b", "donor_id": "a", "receiver_id": "b", "amount": 20,
            "status": "recalled", "donor_epoch": 2, "receiver_epoch": 0,
            "yield_key": None, "forced": 1,
        }]
        result = allocate(zones, requests, loans, 100)
        self.assertEqual(result.served["r1"], 60)
        self.assertEqual(result.served["r2"], 40)
        self.assertNotIn(("a", "b"), result.preempted)
        self.assertEqual(ALARM_RANK["critical"], 3)


if __name__ == "__main__":
    unittest.main()
