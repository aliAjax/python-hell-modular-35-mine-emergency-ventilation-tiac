import unittest

from src.domain import Actor, ConflictError, ValidationError
from src.rules import RuleEngine


class RulesTest(unittest.TestCase):
    def setUp(self):
        self.rules = RuleEngine()
        self.actor = Actor("rule-tester", "admin")

    def test_sensor_severity_is_computed(self):
        data = self.rules.validate_create(self.actor, "sensors", {"location_code": "A", "gas_ppm": 120, "threshold_ppm": 80})
        self.assertEqual(data["severity"], "alarm")

    def test_duplicate_active_task_is_rejected(self):
        incident = {"id": "i-1", "kind": "incident", "status": "detected", "data": {}}
        tasks = [{"id": "t-1", "kind": "task", "status": "assigned", "data": {"dedupe_key": "same"}}]
        lookup = lambda kind, field, value: ([incident] if kind == "incident" else tasks if kind == "task" else [])
        with self.assertRaises(ConflictError):
            self.rules.validate_create(self.actor, "task", {"incident_id": "i-1", "task_type": "rescue", "target": "x", "dedupe_key": "same"}, lookup)

    def test_invalid_gas_threshold_is_rejected(self):
        with self.assertRaises(ValidationError):
            self.rules.validate_create(self.actor, "sensor", {"location_code": "A", "gas_ppm": 1, "threshold_ppm": 0})


if __name__ == "__main__":
    unittest.main()
