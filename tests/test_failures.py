import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, InvalidTransition, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class FailureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")

    def tearDown(self):
        self.tmp.cleanup()

    def test_permission_denied(self):
        incident = self.service.create(self.admin, "incident", {"area_code": "M", "severity": "high", "summary": "x"})
        with self.assertRaises(PermissionDenied):
            self.service.transition(Actor("viewer", "viewer"), incident["id"], "begin_evacuation")

    def test_version_conflict(self):
        incident = self.service.create(self.admin, "incident", {"area_code": "M", "severity": "high", "summary": "x"})
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, incident["id"], "begin_evacuation", {}, 999)

    def test_invalid_transition(self):
        incident = self.service.create(self.admin, "incident", {"area_code": "M", "severity": "high", "summary": "x"})
        with self.assertRaises(InvalidTransition):
            self.service.transition(self.admin, incident["id"], "close", {"summary": "done"})

    def test_close_requires_restored_ventilation(self):
        incident = self.service.create(self.admin, "incident", {"area_code": "M", "severity": "high", "summary": "x"})
        for action in ("begin_evacuation", "search", "stabilize", "recover"):
            incident = self.service.transition(self.admin, incident["id"], action)
        vent = self.service.create(self.admin, "ventilation", {"name": "fan", "area_code": "M", "capacity": 10})
        self.service.transition(self.admin, vent["id"], "stop")
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, incident["id"], "close", {"summary": "done"})


if __name__ == "__main__":
    unittest.main()
