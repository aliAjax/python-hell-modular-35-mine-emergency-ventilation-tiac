import hashlib
import re
from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError, PermissionDenied, ValidationError
from .repository import utcnow
from .rules import RuleEngine


# 报警等级 -> 排队优先级（数字越大越优先）
ALARM_PRIORITY = {"normal": 1, "warning": 2, "alarm": 3, "critical": 4}
MAX_LEDGER_RETRIES = 8


def _area_id(area_code):
    safe = re.sub(r"[^a-zA-Z0-9_-]", "-", str(area_code)).strip("-") or "x"
    return "area-" + safe


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        self.rules.validate_create(actor, kind, payload, self._lookup)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind, payload)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        # A new running fan adds capacity; recompute the quota ledger.
        if kind == "ventilation":
            self._recompute(actor)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        # A fan going down or coming back changes total capacity; recompute the
        # quota ledger so occupations and the queue reflect the new capacity.
        if entity["kind"] == "ventilation" and updated["status"] != entity["status"]:
            self._recompute(actor)
        return updated

    def merge_offline(self, actor, records):
        """Merge field records by a stable (source_id, record_id) identity."""
        if not isinstance(records, list):
            raise ValidationError("records must be a list")
        created = []
        for raw in records:
            if not isinstance(raw, dict):
                raise ValidationError("each offline record must be an object")
            source_id = str(raw.get("source_id", "")).strip()
            record_id = str(raw.get("record_id", "")).strip()
            if not source_id or not record_id:
                raise ValidationError("source_id and record_id are required")
            digest = hashlib.sha256((source_id + "\0" + record_id).encode("utf-8")).hexdigest()[:32]
            entity_id = "offline-" + digest
            existing = self.repository.get_entity(entity_id)
            if existing:
                created.append(existing)
                continue
            payload = dict(raw)
            self.rules.validate_create(actor, "offline_record", payload, self._lookup)
            entity = self.repository.create_entity(
                entity_id,
                "offline_record",
                self.rules.initial_status("offline_record", payload),
                payload,
                actor.user_id,
            )
            self.audit.record(entity_id, actor, "merge_offline", None, entity["status"], {"source_id": source_id, "record_id": record_id})
            created.append(entity)
        return created

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)

    # ------------------------------------------------------------------
    # 风量公共账
    # ------------------------------------------------------------------
    def _running_capacity(self):
        fans = self.repository.list_entities(kind="ventilation")
        return sum(
            float(fan["data"].get("capacity", 0) or 0)
            for fan in fans
            if fan["status"] == "running"
        )

    def _area_entities(self):
        return self.repository.list_entities(kind="area")

    def _active_transfers(self):
        return [
            transfer
            for transfer in self.repository.list_entities(kind="transfer")
            if transfer["status"] == "active"
        ]

    def _quota_entries(self):
        return self.repository.list_entities(kind="quota")

    def _find_area(self, area_code):
        for area in self._area_entities():
            if area["data"].get("area_code") == area_code:
                return area
        return None

    def _retry_ledger(self, actor, build):
        """Run ``build`` -> (revocations, creations) and commit atomically.

        On a version conflict the whole request is rebuilt and retried, so a
        failed write is re-attempted against the latest committed state.
        """
        for attempt in range(MAX_LEDGER_RETRIES):
            revocations, creations = build()
            try:
                self.repository.replace_quota_ledger(revocations, creations)
                return
            except ConflictError:
                if attempt == MAX_LEDGER_RETRIES - 1:
                    raise

    def _recompute(self, actor):
        """Void every quota entry and re-allocate by priority + queue time.

        Active transfers are honoured: a donor gives up part of its share and a
        recipient receives it. Emergency wind is on top of approved demand, so a
        recipient may exceed its normal demand while an alarm is active.
        """

        def build():
            areas = self._area_entities()
            capacity = self._running_capacity()
            transfers = self._active_transfers()
            old_entries = self._quota_entries()

            # Preserve the earliest queue time each continuously-waiting area has
            # waited since. An area that was fully satisfied starts fresh.
            queue_since = {}
            was_queued = {}
            for entry in old_entries:
                code = entry["data"].get("area_code")
                if not code:
                    continue
                was_queued[code] = entry["status"] == "queued"
                queued_at = entry["data"].get("queued_at")
                if queued_at:
                    queue_since[code] = min(queue_since.get(code, queued_at), queued_at)

            now = utcnow()
            demands = []
            for area in areas:
                code = area["data"].get("area_code")
                demands.append(
                    {
                        "area_code": code,
                        "demand": float(area["data"].get("approved_demand", 0) or 0),
                        "priority": int(area["data"].get("priority", 1) or 1),
                        "queued_at": queue_since[code] if was_queued.get(code) else now,
                    }
                )

            # Higher alarm priority first; ties broken by longer waiting time.
            demands.sort(key=lambda item: (-item["priority"], item["queued_at"]))

            remaining = capacity
            allocated = {}
            for item in demands:
                share = min(item["demand"], remaining)
                remaining -= share
                allocated[item["area_code"]] = share

            # Honour active transfers: donor gives up, recipient receives.
            for transfer in transfers:
                donor = transfer["data"].get("from_area")
                recipient = transfer["data"].get("to_area")
                amount = float(transfer["data"].get("amount", 0) or 0)
                allocated[donor] = max(0.0, allocated.get(donor, 0.0) - amount)
                allocated[recipient] = allocated.get(recipient, 0.0) + amount

            revocations = [(entry["id"], entry["version"]) for entry in old_entries]
            creations = []
            for item in demands:
                code = item["area_code"]
                amount = allocated.get(code, 0.0)
                status = "active" if amount >= item["demand"] and item["demand"] > 0 else "queued"
                creations.append(
                    (
                        "quota-" + uuid4().hex,
                        "quota",
                        status,
                        {
                            "area_code": code,
                            "amount": amount,
                            "priority": item["priority"],
                            "queued_at": item["queued_at"],
                            "source": "basic",
                        },
                        actor.user_id,
                    )
                )
            return revocations, creations

        self._retry_ledger(actor, build)
        return self._quota_entries()

    def create_area(self, actor, area_code, name, approved_demand):
        area_code = str(area_code or "").strip()
        if not area_code:
            raise ValidationError("area_code is required")
        if self._find_area(area_code):
            raise ConflictError("area already exists: " + area_code)
        try:
            demand = float(approved_demand)
        except (TypeError, ValueError):
            raise ValidationError("approved_demand must be numeric")
        if demand <= 0:
            raise ValidationError("approved_demand must be positive")
        area_id = _area_id(area_code)
        area = self.repository.create_entity(
            area_id,
            "area",
            "active",
            {
                "area_code": area_code,
                "name": str(name or area_code),
                "approved_demand": demand,
                "alarm_level": "normal",
                "priority": ALARM_PRIORITY["normal"],
            },
            actor.user_id,
        )
        self.audit.record(area_id, actor, "create_area", None, "active", {"area_code": area_code})
        self._recompute(actor)
        return self._find_area(area_code)

    def set_area_alarm(self, actor, area_code, alarm_level):
        area = self._find_area(area_code)
        if not area:
            raise NotFoundError("area not found: " + str(area_code))
        if alarm_level not in ALARM_PRIORITY:
            raise ValidationError("invalid alarm_level: " + str(alarm_level))
        data = dict(area["data"])
        data["alarm_level"] = alarm_level
        data["priority"] = ALARM_PRIORITY[alarm_level]
        updated = self.repository.update_entity(area["id"], area["version"], "active", data)
        # An alarm change voids every standing occupation and transfer.
        for transfer in self._active_transfers():
            self.repository.update_entity(
                transfer["id"], transfer["version"], "recalled", dict(transfer["data"])
            )
        self.audit.record(
            area["id"],
            actor,
            "set_area_alarm",
            area["data"].get("alarm_level"),
            alarm_level,
            {"priority": data["priority"]},
        )
        self._recompute(actor)
        return self._find_area(area_code)

    def request_transfer(self, actor, from_area, to_area, amount, reason=None):
        from_area = str(from_area or "").strip()
        to_area = str(to_area or "").strip()
        if not from_area or not to_area:
            raise ValidationError("from_area and to_area are required")
        if from_area == to_area:
            raise ValidationError("cannot transfer wind to the same area")
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            raise ValidationError("amount must be numeric")
        if amount <= 0:
            raise ValidationError("amount must be positive")

        donor = self._find_area(from_area)
        recipient = self._find_area(to_area)
        if not donor:
            raise NotFoundError("donor area not found: " + from_area)
        if not recipient:
            raise NotFoundError("recipient area not found: " + to_area)
        if int(donor["data"].get("priority", 1)) >= int(recipient["data"].get("priority", 1)):
            raise ConflictError("emergency transfer requires a lower-priority donor")

        donor_entry = next(
            (
                entry
                for entry in self._quota_entries()
                if entry["data"].get("area_code") == from_area
                and float(entry["data"].get("amount", 0) or 0) > 0
            ),
            None,
        )
        if not donor_entry or float(donor_entry["data"].get("amount", 0) or 0) < amount:
            raise ConflictError("donor area has no spare capacity to transfer")
        recipient_entry = next(
            (
                entry
                for entry in self._quota_entries()
                if entry["data"].get("area_code") == to_area
            ),
            None,
        )
        recipient_active = float(recipient_entry["data"].get("amount", 0) or 0) if recipient_entry else 0.0

        transfer_id = "transfer-" + uuid4().hex

        def build():
            transfers = self._active_transfers()
            old_entries = self._quota_entries()
            # Re-read the donor's current entry so the version check reflects the
            # latest committed state (first writer wins).
            donor_now = next(
                (
                    entry
                    for entry in self._quota_entries()
                    if entry["data"].get("area_code") == from_area
                    and float(entry["data"].get("amount", 0) or 0) > 0
                ),
                None,
            )
            if not donor_now or float(donor_now["data"].get("amount", 0) or 0) < amount:
                raise ConflictError("donor area has no spare capacity to transfer")

            # Recompute allocations honouring the new transfer.
            areas = self._area_entities()
            capacity = self._running_capacity()
            queue_since = {}
            was_queued = {}
            for entry in old_entries:
                code = entry["data"].get("area_code")
                if not code:
                    continue
                was_queued[code] = entry["status"] == "queued"
                queued_at = entry["data"].get("queued_at")
                if queued_at:
                    queue_since[code] = min(queue_since.get(code, queued_at), queued_at)
            now = utcnow()
            demands = []
            for area in areas:
                code = area["data"].get("area_code")
                demands.append(
                    {
                        "area_code": code,
                        "demand": float(area["data"].get("approved_demand", 0) or 0),
                        "priority": int(area["data"].get("priority", 1) or 1),
                        "queued_at": queue_since[code] if was_queued.get(code) else now,
                    }
                )
            demands.sort(key=lambda item: (-item["priority"], item["queued_at"]))
            remaining = capacity
            allocated = {}
            for item in demands:
                share = min(item["demand"], remaining)
                remaining -= share
                allocated[item["area_code"]] = share
            for transfer in transfers:
                d = transfer["data"].get("from_area")
                r = transfer["data"].get("to_area")
                a = float(transfer["data"].get("amount", 0) or 0)
                allocated[d] = max(0.0, allocated.get(d, 0.0) - a)
                allocated[r] = allocated.get(r, 0.0) + a
            # Apply the new transfer. Emergency wind is on top of approved demand.
            allocated[from_area] = max(0.0, allocated.get(from_area, 0.0) - amount)
            allocated[to_area] = allocated.get(to_area, 0.0) + amount

            revocations = [(entry["id"], entry["version"]) for entry in old_entries]
            creations = [
                (
                    transfer_id,
                    "transfer",
                    "active",
                    {
                        "from_area": from_area,
                        "to_area": to_area,
                        "amount": amount,
                        "reason": reason,
                    },
                    actor.user_id,
                )
            ]
            for item in demands:
                code = item["area_code"]
                a = allocated.get(code, 0.0)
                status = "active" if a >= item["demand"] and item["demand"] > 0 else "queued"
                creations.append(
                    (
                        "quota-" + uuid4().hex,
                        "quota",
                        status,
                        {
                            "area_code": code,
                            "amount": a,
                            "priority": item["priority"],
                            "queued_at": item["queued_at"],
                            "source": "transfer" if code == to_area else "basic",
                            "transfer_id": transfer_id if code == to_area else None,
                        },
                        actor.user_id,
                    )
                )
            return revocations, creations

        self._retry_ledger(actor, build)
        self.audit.record(
            transfer_id,
            actor,
            "request_transfer",
            None,
            "active",
            {"from_area": from_area, "to_area": to_area, "amount": amount},
        )
        return self.get(transfer_id)

    def recall_transfer(self, actor, transfer_id):
        transfer = self.repository.get_entity(transfer_id)
        if not transfer or transfer["kind"] != "transfer":
            raise NotFoundError("transfer not found: " + str(transfer_id))
        if transfer["status"] != "active":
            raise ConflictError("transfer is not active: " + transfer_id)
        updated = self.repository.update_entity(
            transfer_id, transfer["version"], "recalled", dict(transfer["data"])
        )
        self.audit.record(
            transfer_id,
            actor,
            "recall_transfer",
            "active",
            "recalled",
            {"from_area": transfer["data"].get("from_area"), "to_area": transfer["data"].get("to_area")},
        )
        self._recompute(actor)
        return self.get(transfer_id)

    def backfill_ledger(self, actor):
        """Backfill wind records from running fan capacity for areas with none.

        Old data has no wind records, so on upgrade each area with running fans
        gets an initial quota entry equal to the total running fan capacity in
        that area, and an area record with matching approved demand.
        """
        fans = self.repository.list_entities(kind="ventilation")
        capacity_by_area = {}
        for fan in fans:
            if fan["status"] != "running":
                continue
            code = fan["data"].get("area_code")
            if not code:
                continue
            capacity_by_area[code] = capacity_by_area.get(code, 0.0) + float(
                fan["data"].get("capacity", 0) or 0
            )

        now = utcnow()
        created = []
        for code, capacity in capacity_by_area.items():
            if self._find_area(code):
                continue
            area = self.repository.create_entity(
                _area_id(code),
                "area",
                "active",
                {
                    "area_code": code,
                    "name": str(code),
                    "approved_demand": capacity,
                    "alarm_level": "normal",
                    "priority": ALARM_PRIORITY["normal"],
                },
                actor.user_id,
            )
            self.repository.create_entity(
                "quota-" + uuid4().hex,
                "quota",
                "active",
                {
                    "area_code": code,
                    "amount": capacity,
                    "priority": ALARM_PRIORITY["normal"],
                    "queued_at": now,
                    "source": "backfill",
                },
                actor.user_id,
            )
            created.append(area)
        return {"backfilled": len(created), "areas": [a["data"].get("area_code") for a in created]}

    def ledger_view(self):
        return {
            "capacity": self._running_capacity(),
            "areas": [
                {
                    "area_code": a["data"].get("area_code"),
                    "name": a["data"].get("name"),
                    "approved_demand": a["data"].get("approved_demand"),
                    "alarm_level": a["data"].get("alarm_level"),
                    "priority": a["data"].get("priority"),
                }
                for a in self._area_entities()
            ],
            "quota": [
                {
                    "id": e["id"],
                    "area_code": e["data"].get("area_code"),
                    "amount": e["data"].get("amount"),
                    "status": e["status"],
                    "priority": e["data"].get("priority"),
                    "queued_at": e["data"].get("queued_at"),
                    "source": e["data"].get("source"),
                    "transfer_id": e["data"].get("transfer_id"),
                }
                for e in self._quota_entries()
            ],
            "transfers": [
                {
                    "id": t["id"],
                    "from_area": t["data"].get("from_area"),
                    "to_area": t["data"].get("to_area"),
                    "amount": t["data"].get("amount"),
                    "status": t["status"],
                    "reason": t["data"].get("reason"),
                }
                for t in self.repository.list_entities(kind="transfer")
            ],
        }
