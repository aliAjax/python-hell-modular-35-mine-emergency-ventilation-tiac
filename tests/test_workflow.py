import tempfile
import unittest
from pathlib import Path

from src.domain import Actor
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.actor = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def create(self, kind, data):
        return self.service.create(self.actor, kind, data)

    def act(self, entity, action, data=None, version=None):
        return self.service.transition(self.actor, entity["id"], action, data or {}, version)

    def test_full_emergency_flow(self):
        incident = self.create("incident", {"area_code": "M-01", "severity": "critical", "summary": "gas leak"})
        incident = self.act(incident, "begin_evacuation")
        incident = self.act(incident, "search")
        incident = self.act(incident, "stabilize")
        incident = self.act(incident, "recover")

        worker = self.create("worker", {"name": "Li Wei", "location_code": "M-01", "team": "A"})
        worker = self.act(worker, "mark_missing")
        worker = self.act(worker, "locate", {"located_at": "2026-09-27T10:00:00Z"})
        worker = self.act(worker, "rescue", {"incident_id": incident["id"]})
        self.assertEqual(worker["status"], "rescued")

        sensor = self.create("sensor", {"location_code": "M-01", "gas_ppm": 120, "threshold_ppm": 80})
        self.assertEqual(sensor["data"]["severity"], "alarm")
        sensor = self.act(sensor, "raise_alarm")
        self.assertEqual(sensor["status"], "alarm")

        vent = self.create("ventilation", {"name": "fan-1", "area_code": "M-01", "capacity": 100})
        vent = self.act(vent, "stop", {})
        vent = self.act(vent, "restore", {"tested_at": "2026-09-27T11:00:00Z"})
        self.assertEqual(vent["status"], "running")

        task = self.create("task", {"incident_id": incident["id"], "task_type": "rescue", "target": "worker-1", "dedupe_key": "rescue-1"})
        task = self.act(task, "assign", {"team": "A"})
        task = self.act(task, "accept", {})
        task = self.act(task, "complete", {"result": "worker recovered"})
        self.assertEqual(task["status"], "completed")

        incident = self.act(incident, "close", {"summary": "all clear"})
        self.assertEqual(incident["status"], "closed")

    def test_offline_merge_is_idempotent(self):
        record = {"source_id": "field-a", "record_id": "42", "recorded_at": "2026-09-27T10:00:00Z", "payload": {"type": "gas", "value": 12}}
        first = self.service.merge_offline(self.actor, [record])
        second = self.service.merge_offline(self.actor, [record])
        self.assertEqual(first[0]["id"], second[0]["id"])
        self.assertEqual(len(self.service.list("offline_record")), 1)


if __name__ == "__main__":
    unittest.main()
