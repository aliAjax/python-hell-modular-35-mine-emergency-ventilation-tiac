"""风量公共账。

设计要点（对应调度规则）：
- 每个区域(zone)按 ``approved_demand`` 核定需求占用风机总容量；
- 容量不足时申请排队，顺序 = 报警级别(重→轻) -> 等待时间(早→晚) -> 请求ID；
- 高级别应急申请可从较低级别区域让出部分风量（记一笔 loan/让渡账）；
- 被让出区域一旦自己报警，立即收回（loan 置 recalled 并形成保护区，接收方在其报警期间拿不回这笔风）；
- 报警等级一变，该区域原占用立即作废（served 清零、epoch+1），整体重算，使用方退回队列，排队时间沿用首次入队时间；
- 两名调度同时提交同一笔让渡：同一 (donor, receiver) 只有一笔活动账，先落账者生效，
  写冲突（版本号/锁）后用原始申请参数重试，重复 yield_key 直接返回首笔账；
- 升级时没有旧风量记录：按在运行风机(ventilation status=running)容量之和回填。

模块只依赖标准库，``allocate`` 是纯函数，便于单测；AirStore 负责 SQLite 落账，
AirLedger 负责用例编排和写入失败后的原申请重试。
"""

import json
import sqlite3
import threading
from datetime import datetime, timezone
from uuid import uuid4

from .domain import ConflictError, NotFoundError, PermissionDenied, ValidationError

ALARM_LEVELS = ("none", "warning", "alarm", "critical")
ALARM_RANK = {level: index for index, level in enumerate(ALARM_LEVELS)}

# 写入失败时按原申请重试的次数（含首次）
MAX_WRITE_ATTEMPTS = 8


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _zone_snapshot(zone):
    return {
        "id": zone["id"],
        "area_code": zone["area_code"],
        "approved_demand": zone["approved_demand"],
        "alarm": zone["alarm"],
        "epoch": zone["epoch"],
    }


class Allocation:
    """一次纯计算的结果。"""

    def __init__(self):
        self.served = {}          # request_id -> 占用风量
        self.preempted = {}       # (donor_zone_id, receiver_request_id) -> 让渡量
        self.protected = {}       # zone_id -> 收回保护区风量


def _protection(zone_by_id, loans):
    """recalled 让渡形成的保护区：捐赠方仍在报警且 epoch 未翻页时有效。"""
    totals = {}
    for loan in loans:
        if loan["status"] != "recalled":
            continue
        donor = zone_by_id.get(loan["donor_id"])
        if not donor or donor["alarm"] == "none" or donor["epoch"] != loan["donor_epoch"]:
            continue
        totals[donor["id"]] = totals.get(donor["id"], 0) + loan["amount"]
    return {zone_id: min(amount, zone_by_id[zone_id]["approved_demand"])
            for zone_id, amount in totals.items()}


def allocate(zones, requests, loans, total_capacity, prefer=None, directives=None):
    """纯函数：按当前区域/申请/让渡账重算全部占用。

    - zones: [{id, approved_demand, alarm, epoch}]
    - requests: [{id, zone_id, amount, enqueued_at, seq}]（open 请求）
    - loans: 已持久化的让渡账（active/recalled）
    - prefer: 可选 {receiver_request_id, donor_id, amount}，本次点名让渡时优先从该区域扣减
    - directives: 点名让渡强制划转 {loan_key: amount}，先于自由分配执行
    返回 Allocation。

    让渡量由两部分组成：点名让渡作为硬约束先划转；自然抢占由"无报警基线排队(FIFO)"
    与"按报警优先级的最终占用"之差归因，高级别多得的风来自严格较低级别区域。
    """
    result = Allocation()
    zone_by_id = {zone["id"]: zone for zone in zones}
    directives = directives or {}
    result.protected = _protection(zone_by_id, loans)
    frozen_pairs = set()
    for loan in loans:
        donor = zone_by_id.get(loan["donor_id"])
        if (loan["status"] == "recalled" and donor and donor["alarm"] != "none"
                and donor["epoch"] == loan["donor_epoch"]):
            frozen_pairs.add((loan["donor_id"], loan["receiver_id"]))

    open_requests = [r for r in requests if zone_by_id.get(r["zone_id"])]
    capacity = max(0, int(total_capacity))
    req_of_zone = {r["zone_id"]: r for r in open_requests}

    # 点名让渡作为硬约束先划转：捐赠方扣减、接收方预收（不超过接收方需求量）
    pre_grant, pre_take = {}, {}
    for loan in loans:
        if loan["status"] != "active" or not loan["forced"]:
            continue
        amount = directives.get(loan["loan_key"], loan["amount"])
        donor_req = req_of_zone.get(loan["donor_id"])
        receiver_req = req_of_zone.get(loan["receiver_id"])
        if not donor_req or not receiver_req:
            continue
        donor_zone = zone_by_id[loan["donor_id"]]
        receiver_zone = zone_by_id[loan["receiver_id"]]
        if (ALARM_RANK.get(donor_zone["alarm"], 0) >= ALARM_RANK.get(receiver_zone["alarm"], 0)
                or (donor_zone["id"], receiver_zone["id"]) in frozen_pairs):
            continue
        take = min(int(amount), int(donor_req["amount"]), int(receiver_req["amount"]))
        if take <= 0:
            continue
        pre_take[donor_req["id"]] = pre_take.get(donor_req["id"], 0) + take
        pre_grant[receiver_req["id"]] = pre_grant.get(receiver_req["id"], 0) + take
        key = (loan["donor_id"], loan["receiver_id"])
        result.preempted[key] = result.preempted.get(key, 0) + take

    # 最终方案：保护区 -> 报警级别↓ -> 等待时间(入队序号)↑ -> 请求ID
    def priority_key(req):
        zone = zone_by_id[req["zone_id"]]
        return (
            0 if result.protected.get(zone["id"]) else 1,
            -ALARM_RANK.get(zone["alarm"], 0),
            req.get("seq", 0),
            req["enqueued_at"],
            req["id"],
        )

    # 基线方案：保护区不变，其余当作全部无报警按排队时间分配。
    # 强制让渡在基线中仍保留（捐赠方照样让出），只是接收方预收不参与——这样本次新点名
    # 会从原有自然让出关系里把风改拨给点名接收方，而不会凭空释放容量。
    def baseline_key(req):
        zone = zone_by_id[req["zone_id"]]
        return (0 if result.protected.get(zone["id"]) else 1,
                req.get("seq", 0), req["enqueued_at"], req["id"])

    def greedy(ranker, with_grants=False):
        served = {}
        remaining = capacity
        for req in open_requests:
            reserved = min(result.protected.get(req["zone_id"], 0), int(req["amount"]))
            grant_to = pre_grant.get(req["id"], 0) if with_grants else 0
            take_from = pre_take.get(req["id"], 0)
            base = reserved + grant_to
            served[req["id"]] = min(base, int(req["amount"]))
            remaining -= base
            remaining += take_from  # 让渡是权属转移：捐赠方让出的风由接收方预收占用
        remaining = max(0, remaining)
        for req in sorted(open_requests, key=ranker):
            already = served.get(req["id"], 0)
            # 捐赠方被点名划出的部分不得再从自由容量补回；接收方仍可按级别/排队拿自由风
            ceiling = int(req["amount"]) - pre_take.get(req["id"], 0)
            grant = min(max(0, ceiling - already), remaining)
            served[req["id"]] = already + grant
            remaining -= grant
        return served

    final = greedy(priority_key, with_grants=True)
    baseline = greedy(baseline_key, with_grants=False)
    result.served = final

    # 自然抢占归因：最终占用减去基线占用，再扣掉点名预收（点名划转单独记账）。
    # 捐赠方的点名划出已在其 delta 中体现，直接用原始差额即可。
    zone_of_req = {req["id"]: zone_by_id[req["zone_id"]] for req in open_requests}
    gains, losses = {}, {}
    for req in open_requests:
        delta = int(final.get(req["id"], 0)) - int(baseline.get(req["id"], 0))
        if delta > 0 and ALARM_RANK.get(zone_of_req[req["id"]]["alarm"], 0) > 0:
            gains[req["id"]] = delta - pre_grant.get(req["id"], 0)
        elif delta < 0:
            losses[req["id"]] = -delta
    gains = {rid: gain for rid, gain in gains.items() if gain > 0}

    prefer_target = prefer.get("receiver_request_id") if prefer else None

    def donor_order(item):
        donor_req_id, donor_zone, loss = item
        preferred = (prefer and prefer_target is not None
                     and prefer_target in gains and prefer.get("donor_id") == donor_zone["id"])
        return (0 if preferred else 1, ALARM_RANK.get(donor_zone["alarm"], 0), -loss,
                donor_zone["id"], donor_req_id)

    for receiver_req in sorted(
            (r for r in open_requests if r["id"] in gains), key=priority_key):
        receiver_req_id = receiver_req["id"]
        receiver_zone = zone_of_req[receiver_req_id]
        deficit = gains[receiver_req_id]
        candidates = []
        for donor_req_id, loss in losses.items():
            donor_zone = zone_of_req[donor_req_id]
            if loss <= 0:
                continue
            if ALARM_RANK.get(donor_zone["alarm"], 0) >= ALARM_RANK.get(receiver_zone["alarm"], 0):
                continue  # 只能从严格较低级别让出
            if (donor_zone["id"], receiver_zone["id"]) in frozen_pairs:
                continue
            candidates.append((donor_req_id, donor_zone, loss))
        for donor_req_id, donor_zone, loss in sorted(candidates, key=donor_order):
            if deficit <= 0:
                break
            take = min(deficit, losses[donor_req_id])
            if (prefer and prefer_target == receiver_req_id
                    and prefer.get("donor_id") == donor_zone["id"]
                    and prefer.get("amount") is not None):
                take = min(take, int(prefer["amount"]))
            losses[donor_req_id] -= take
            deficit -= take
            key = (donor_zone["id"], receiver_zone["id"])
            result.preempted[key] = result.preempted.get(key, 0) + take
    return result


class AirStore:
    """风量账的 SQLite 落账；所有写操作在单写事务中串行化。"""

    def __init__(self, repository):
        self.repository = repository
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self):
        return self.repository._connect()

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS air_zones (
                    id TEXT PRIMARY KEY,
                    area_code TEXT NOT NULL UNIQUE,
                    approved_demand INTEGER NOT NULL,
                    alarm TEXT NOT NULL DEFAULT 'none',
                    epoch INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS air_requests (
                    id TEXT PRIMARY KEY,
                    zone_id TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    enqueued_at TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    served INTEGER NOT NULL DEFAULT 0,
                    open INTEGER NOT NULL DEFAULT 1,
                    epoch_opened INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_air_requests_open ON air_requests(open);
                CREATE TABLE IF NOT EXISTS air_loans (
                    loan_key TEXT PRIMARY KEY,
                    donor_id TEXT NOT NULL,
                    receiver_id TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    donor_epoch INTEGER NOT NULL,
                    receiver_epoch INTEGER NOT NULL,
                    yield_key TEXT,
                    forced INTEGER NOT NULL DEFAULT 0,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_air_loans_active
                    ON air_loans(donor_id, receiver_id) WHERE status = 'active';
                CREATE TABLE IF NOT EXISTS air_yield_idem (
                    yield_key TEXT PRIMARY KEY,
                    loan_key TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS air_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)

    # ---------- 读取 ----------

    def list_zones(self):
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM air_zones ORDER BY area_code, id").fetchall()
        return [dict(row) for row in rows]

    def get_zone(self, zone_id):
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM air_zones WHERE id = ?", (zone_id,)).fetchone()
        return dict(row) if row else None

    def zone_by_area(self, area_code):
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM air_zones WHERE area_code = ?", (area_code,)).fetchone()
        return dict(row) if row else None

    def open_requests(self):
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM air_requests WHERE open = 1 ORDER BY seq, id"
            ).fetchall()
        return [dict(row) for row in rows]

    def open_request_for_zone(self, zone_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM air_requests WHERE zone_id = ? AND open = 1 ORDER BY enqueued_at, id LIMIT 1",
                (zone_id,),
            ).fetchone()
        return dict(row) if row else None

    def list_loans(self, status=None):
        with self._connect() as connection:
            if status:
                rows = connection.execute(
                    "SELECT * FROM air_loans WHERE status = ? ORDER BY updated_at, loan_key", (status,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM air_loans ORDER BY updated_at, loan_key").fetchall()
        return [dict(row) for row in rows]

    def get_loan(self, loan_key):
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM air_loans WHERE loan_key = ?", (loan_key,)).fetchone()
        return dict(row) if row else None

    def loan_by_yield_key(self, yield_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM air_yield_idem WHERE yield_key = ?", (yield_key,)
            ).fetchone()
            if not row:
                return None
            return self.get_loan(row["loan_key"])

    def get_capacity(self):
        with self._connect() as connection:
            row = connection.execute("SELECT value FROM air_meta WHERE key = 'capacity'").fetchone()
        return int(json.loads(row["value"])) if row else None

    def ledger_version(self, connection):
        row = connection.execute("SELECT value FROM air_meta WHERE key = 'version'").fetchone()
        return int(json.loads(row["value"])) if row else 0

    def snapshot(self):
        return {
            "capacity": self.get_capacity() or 0,
            "zones": self.list_zones(),
            "requests": self.open_requests(),
            "loans": self.list_loans(),
        }

    # ---------- 单写事务 ----------

    def mutate(self, expected_version, fn):
        """在 BEGIN IMMEDIATE 事务内执行 ``fn(connection)``。

        版本号不匹配抛 ConflictError，由上层按原申请重试。
        """
        with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                if self.ledger_version(connection) != expected_version:
                    raise ConflictError("air ledger version conflict")
                output = fn(connection)
                connection.execute(
                    "INSERT INTO air_meta(key, value, updated_at) VALUES('version', ?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                    (json.dumps(expected_version + 1), utcnow()),
                )
                connection.commit()
                return output
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    def current_version(self):
        with self._connect() as connection:
            return self.ledger_version(connection)


class AirLedger:
    """风量公共账用例编排。"""

    def __init__(self, repository, rules=None):
        self.store = AirStore(repository)
        self.repository = repository
        self.rules = rules

    # ---------- 审计 ----------

    def _audit(self, connection, actor, action, detail, entity_id="air-ledger",
               from_status=None, to_status="active"):
        connection.execute(
            "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entity_id,
                actor.user_id,
                actor.role,
                action,
                from_status,
                to_status,
                json.dumps(detail, ensure_ascii=False, sort_keys=True),
                utcnow(),
            ),
        )

    # ---------- 重试包装：写失败后按原申请重试 ----------

    def _with_retry(self, command):
        from .rules import RuleEngine
        self.rules = self.rules or RuleEngine()
        last_error = None
        for _ in range(MAX_WRITE_ATTEMPTS):
            version = self.store.current_version()
            try:
                return self.store.mutate(version, lambda connection: command(connection, version))
            except (ConflictError, sqlite3.OperationalError) as exc:
                last_error = exc
                continue
        raise last_error

    # ---------- 基础校验 ----------

    @staticmethod
    def _demand(value):
        try:
            amount = int(value)
        except (TypeError, ValueError):
            raise ValidationError("approved_demand must be an integer")
        if amount <= 0:
            raise ValidationError("approved_demand must be positive")
        return amount

    @staticmethod
    def _alarm(value):
        if value not in ALARM_LEVELS:
            raise ValidationError("alarm must be one of: " + ", ".join(ALARM_LEVELS))
        return value

    @staticmethod
    def _ensure_role(actor, allowed):
        if actor.role not in allowed:
            raise PermissionDenied("role %s is not allowed here" % actor.role)

    # ---------- 区域与容量 ----------

    def register_zone(self, actor, area_code, approved_demand, zone_id=None):
        self._ensure_role(actor, ("admin", "safety", "dispatcher"))
        area_code = str(area_code or "").strip()
        if not area_code:
            raise ValidationError("area_code is required")
        demand = self._demand(approved_demand)

        def command(connection, version):
            row = connection.execute(
                "SELECT id FROM air_zones WHERE area_code = ?", (area_code,)
            ).fetchone()
            if row:
                raise ConflictError("zone already exists for area: " + area_code)
            new_id = zone_id or "zone-" + uuid4().hex[:12]
            now = utcnow()
            connection.execute(
                "INSERT INTO air_zones(id, area_code, approved_demand, alarm, epoch, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 'none', 0, ?, ?, ?)",
                (new_id, area_code, demand, actor.user_id, now, now),
            )
            self._audit(connection, actor, "air.zone_register",
                        {"area_code": area_code, "approved_demand": demand}, entity_id=new_id)
            self._recompute(connection, actor, reason="zone_register:" + area_code)
            return dict(connection.execute("SELECT * FROM air_zones WHERE id = ?", (new_id,)).fetchone())

        return self._with_retry(command)

    def update_demand(self, actor, zone_id, approved_demand):
        self._ensure_role(actor, ("admin", "safety", "dispatcher"))
        demand = self._demand(approved_demand)

        def command(connection, version):
            zone = self._load_zone(connection, zone_id)
            old = zone["approved_demand"]
            connection.execute(
                "UPDATE air_zones SET approved_demand = ?, updated_at = ? WHERE id = ?",
                (demand, utcnow(), zone_id),
            )
            request = connection.execute(
                "SELECT * FROM air_requests WHERE zone_id = ? AND open = 1 ORDER BY enqueued_at, id LIMIT 1",
                (zone_id,),
            ).fetchone()
            if request:
                # 需求变化视同原占用作废重算
                connection.execute(
                    "UPDATE air_requests SET amount = ?, served = 0 WHERE id = ?",
                    (demand, request["id"]),
                )
                self._bump_epoch(connection, [zone_id])
            self._audit(connection, actor, "air.demand_change",
                        {"old": old, "new": demand}, entity_id=zone_id)
            self._recompute(connection, actor, reason="demand_change:" + zone_id)
            return dict(connection.execute("SELECT * FROM air_zones WHERE id = ?", (zone_id,)).fetchone())

        return self._with_retry(command)

    def backfill_capacity(self, actor=None):
        """升级回填：按在运行风机(ventilation, running)的 capacity 之和建账。

        以 air_meta('capacity_backfilled') 作为回填标记：首次启动时没有风机，会记下
        容量 0 且标记完成；旧库升级（已有风机但没有风量记录）首次启动即按风机和回填。
        已经回填过的库不再覆盖容量，显式同步走 set_capacity。
        """
        fans = self.repository.list_entities(kind="ventilation", status="running")
        total = sum(int(fan["data"].get("capacity", 0) or 0) for fan in fans)

        def command(connection, version):
            marker = connection.execute(
                "SELECT value FROM air_meta WHERE key = 'capacity_backfilled'"
            ).fetchone()
            backfilled = marker is not None
            cap_row = connection.execute("SELECT value FROM air_meta WHERE key = 'capacity'").fetchone()
            total_now = int(json.loads(cap_row["value"])) if cap_row else 0
            if not backfilled and total > 0:
                # 真正按在运行风机回填（首次启动无风机时 capacity=0 且不打标记，
                # 待风机录入后本调用仍可完成升级回填）
                total_now = total
                now = utcnow()
                connection.execute(
                    "INSERT INTO air_meta(key, value, updated_at) VALUES('capacity', ?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                    (json.dumps(total), now),
                )
                connection.execute(
                    "INSERT INTO air_meta(key, value, updated_at) VALUES('capacity_backfilled', ?, ?)",
                    (json.dumps(True), now),
                )
                self._audit(connection, actor or _SYSTEM_ACTOR, "air.capacity_backfill",
                            {"fan_count": len(fans), "capacity": total, "source": "running_ventilations"})
                did_backfill = True
            else:
                did_backfill = False
            self._recompute(connection, actor or _SYSTEM_ACTOR, reason="capacity_backfill")
            return {"capacity": total_now, "backfilled": did_backfill, "running_fans": len(fans)}

        return self._with_retry(command)

    def set_capacity(self, actor, total_capacity):
        """显式设定/同步在运行风机容量（同样会触发重算）。"""
        self._ensure_role(actor, ("admin", "safety"))
        try:
            total = int(total_capacity)
        except (TypeError, ValueError):
            raise ValidationError("capacity must be an integer")
        if total < 0:
            raise ValidationError("capacity must be non-negative")

        def command(connection, version):
            connection.execute(
                "INSERT INTO air_meta(key, value, updated_at) VALUES('capacity', ?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                (json.dumps(total), utcnow()),
            )
            self._audit(connection, actor, "air.capacity_set", {"capacity": total})
            self._recompute(connection, actor, reason="capacity_set")
            return {"capacity": total}

        return self._with_retry(command)

    # ---------- 申请与报警 ----------

    def submit_request(self, actor, zone_id, amount=None, request_id=None):
        """区域提交送风申请；每区域只保留一笔 open 申请（重复提交返回原申请，排队时间不变）。"""
        self._ensure_role(actor, ("admin", "safety", "dispatcher", "field"))

        def command(connection, version):
            zone = self._load_zone(connection, zone_id)
            want = int(amount) if amount is not None else zone["approved_demand"]
            if want <= 0:
                raise ValidationError("request amount must be positive")
            existing = connection.execute(
                "SELECT * FROM air_requests WHERE zone_id = ? AND open = 1 ORDER BY enqueued_at, id LIMIT 1",
                (zone_id,),
            ).fetchone()
            if existing:
                return dict(existing)
            new_id = request_id or "req-" + uuid4().hex[:12]
            seq_row = connection.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq FROM air_requests").fetchone()
            next_seq = int(seq_row["next_seq"])
            connection.execute(
                "INSERT INTO air_requests(id, zone_id, amount, enqueued_at, seq, served, open, epoch_opened, created_by) "
                "VALUES (?, ?, ?, ?, ?, 0, 1, ?, ?)",
                (new_id, zone_id, want, utcnow(), next_seq, zone["epoch"], actor.user_id),
            )
            self._audit(connection, actor, "air.request_submit",
                        {"zone_id": zone_id, "amount": want, "enqueued": True}, entity_id=new_id)
            self._recompute(connection, actor, reason="request_submit:" + new_id)
            return dict(connection.execute("SELECT * FROM air_requests WHERE id = ?", (new_id,)).fetchone())

        return self._with_retry(command)

    def change_alarm(self, actor, zone_id, alarm):
        """报警等级变化：原占用立即作废（清零、epoch+1）、让渡账收口，再整体重算。"""
        self._ensure_role(actor, ("admin", "safety", "dispatcher", "field"))
        alarm = self._alarm(alarm)

        def command(connection, version):
            zone = self._load_zone(connection, zone_id)
            old = zone["alarm"]
            if old == alarm:
                return dict(zone)
            now = utcnow()
            connection.execute(
                "UPDATE air_zones SET alarm = ?, epoch = epoch + 1, updated_at = ? WHERE id = ?",
                (alarm, now, zone_id),
            )
            zone = dict(connection.execute("SELECT * FROM air_zones WHERE id = ?", (zone_id,)).fetchone())
            new_epoch = zone["epoch"]

            # 让渡账收口：捐赠方进入报警 => 立即收回(recalled)；报警解除 => 关账。
            # 接收方报警变化（epoch 翻页）由统一重算把旧账判为过期并关账。
            donor_loans = connection.execute(
                "SELECT * FROM air_loans WHERE donor_id = ? AND status IN ('active', 'recalled')",
                (zone_id,),
            ).fetchall()
            for loan in donor_loans:
                if alarm == "none":
                    self._close_loan(connection, dict(loan), "closed", now)
                else:
                    self._recall_loan(connection, dict(loan), new_epoch, now)

            # 原占用立即作废，使用方退回队列；排队时间沿用 enqueued_at
            connection.execute(
                "UPDATE air_requests SET served = 0 WHERE zone_id = ? AND open = 1", (zone_id,)
            )
            self._audit(connection, actor, "air.alarm_change",
                        {"zone_id": zone_id, "old": old, "new": alarm,
                         "occupancy_voided": True, "epoch": new_epoch},
                        entity_id=zone_id, from_status=old, to_status=alarm)
            self._recompute(connection, actor, reason="alarm_change:" + zone_id)
            return dict(connection.execute("SELECT * FROM air_zones WHERE id = ?", (zone_id,)).fetchone())

        return self._with_retry(command)

    def cancel_request(self, actor, request_id):
        self._ensure_role(actor, ("admin", "safety", "dispatcher"))

        def command(connection, version):
            row = connection.execute("SELECT * FROM air_requests WHERE id = ?", (request_id,)).fetchone()
            if not row:
                raise NotFoundError("air request not found: " + request_id)
            connection.execute("UPDATE air_requests SET open = 0, served = 0 WHERE id = ?", (request_id,))
            self._audit(connection, actor, "air.request_cancel",
                        {"zone_id": row["zone_id"]}, entity_id=request_id,
                        from_status="open", to_status="cancelled")
            self._recompute(connection, actor, reason="request_cancel:" + request_id)
            return {"id": request_id, "open": 0}

        return self._with_retry(command)

    # ---------- 调度点名让渡 ----------

    def yield_air(self, actor, donor_id, receiver_id, amount, yield_key=None):
        """两名调度可能同时提交同一笔让渡。

        同一 (donor, receiver) 仅一笔 active 账（DB 唯一约束兜底，先落账者生效）；
        带 yield_key 的重复提交直接返回首笔账；写冲突按原参数重试。
        真正的风量划转由统一重算完成，本调用只校验意图、点名捐赠方。
        """
        self._ensure_role(actor, ("admin", "safety", "dispatcher"))
        try:
            amount = int(amount)
        except (TypeError, ValueError):
            raise ValidationError("amount must be an integer")
        if amount <= 0:
            raise ValidationError("amount must be positive")
        if donor_id == receiver_id:
            raise ValidationError("donor and receiver must differ")
        if yield_key:
            existing = self.store.loan_by_yield_key(yield_key)
            if existing:
                return existing

        def command(connection, version):
            if yield_key:
                row = connection.execute(
                    "SELECT l.* FROM air_yield_idem y JOIN air_loans l ON l.loan_key = y.loan_key "
                    "WHERE y.yield_key = ?",
                    (yield_key,),
                ).fetchone()
                if row:
                    return dict(row)
            donor = self._load_zone(connection, donor_id)
            receiver = self._load_zone(connection, receiver_id)
            if ALARM_RANK[donor["alarm"]] >= ALARM_RANK[receiver["alarm"]]:
                raise ValidationError("air can only be yielded from a lower alarm level")
            receiver_req = connection.execute(
                "SELECT * FROM air_requests WHERE zone_id = ? AND open = 1 ORDER BY enqueued_at, id LIMIT 1",
                (receiver_id,),
            ).fetchone()
            if not receiver_req:
                raise ValidationError("receiver has no open air request")

            # 先落账者生效：同对 donor/receiver 的 active 账已存在则直接返回
            loan_key = "%s:%s" % (donor_id, receiver_id)
            existing_loan = connection.execute(
                "SELECT * FROM air_loans WHERE loan_key = ?", (loan_key,)
            ).fetchone()
            if existing_loan and existing_loan["status"] == "active":
                return dict(existing_loan)

            prefer = {"receiver_request_id": receiver_req["id"], "donor_id": donor_id, "amount": amount}
            self._recompute(connection, actor, reason="yield:" + loan_key, prefer=prefer,
                            expect_yield=(loan_key, donor_id, receiver_req["zone_id"], amount, yield_key))
            loan = connection.execute("SELECT * FROM air_loans WHERE loan_key = ?", (loan_key,)).fetchone()
            if not loan or loan["status"] != "active" or not loan["forced"]:
                raise ConflictError("yield could not be booked: donor has no lower-level air to give")
            return dict(loan)

        return self._with_retry(command)

    # ---------- 公共账视图 ----------

    def ledger(self):
        snap = self.store.snapshot()
        zones = snap["zones"]
        requests = snap["requests"]
        loans = snap["loans"]
        total = snap["capacity"]
        allocation = allocate(
            [_zone_snapshot(z) for z in zones],
            [{"id": r["id"], "zone_id": r["zone_id"], "amount": r["amount"],
              "enqueued_at": r["enqueued_at"], "seq": r["seq"]} for r in requests],
            loans,
            total,
        )
        zone_by_id = {z["id"]: z for z in zones}
        items = []
        occupied = 0
        for req in requests:
            served = allocation.served.get(req["id"], 0)
            occupied += served
            zone = zone_by_id[req["zone_id"]]
            items.append({
                "request_id": req["id"],
                "zone_id": zone["id"],
                "area_code": zone["area_code"],
                "approved_demand": zone["approved_demand"],
                "alarm": zone["alarm"],
                "requested": req["amount"],
                "occupied": served,
                "state": "filled" if served >= req["amount"] else "partial" if served > 0 else "queued",
                "enqueued_at": req["enqueued_at"],
                "seq": req["seq"],
                "protected": allocation.protected.get(zone["id"], 0),
            })
        # 与分配器同一顺序输出，排队先后一目了然
        items.sort(key=lambda item: (
            0 if item["protected"] else 1,
            -ALARM_RANK.get(item["alarm"], 0),
            item["seq"],
            item["enqueued_at"],
            item["request_id"],
        ))
        return {
            "capacity": total,
            "occupied": occupied,
            "available": max(0, total - occupied),
            "queue": items,
            "loans": [
                {
                    "loan_key": loan["loan_key"],
                    "donor_id": loan["donor_id"],
                    "receiver_id": loan["receiver_id"],
                    "amount": loan["amount"],
                    "status": loan["status"],
                    "forced": bool(loan["forced"]),
                    "yield_key": loan["yield_key"],
                    "updated_at": loan["updated_at"],
                }
                for loan in loans if loan["status"] in ("active", "recalled")
            ],
        }

    # ---------- 内部：统一重算与落账 ----------

    @staticmethod
    def _load_zone(connection, zone_id):
        row = connection.execute("SELECT * FROM air_zones WHERE id = ?", (zone_id,)).fetchone()
        if not row:
            raise NotFoundError("air zone not found: " + zone_id)
        return dict(row)

    @staticmethod
    def _close_loan(connection, loan, status, now=None):
        now = now or utcnow()
        connection.execute(
            "UPDATE air_loans SET status = ?, updated_at = ? WHERE loan_key = ?",
            (status, now, loan["loan_key"]),
        )

    @staticmethod
    def _recall_loan(connection, loan, donor_epoch, now=None):
        """捐赠方进入报警：立刻收回，保护区锚定新 epoch（捐赠方报警期间持续有效）。"""
        now = now or utcnow()
        connection.execute(
            "UPDATE air_loans SET status = 'recalled', donor_epoch = ?, updated_at = ? WHERE loan_key = ?",
            (donor_epoch, now, loan["loan_key"]),
        )

    def _recompute(self, connection, actor, reason, prefer=None, expect_yield=None):
        zones = [dict(row) for row in connection.execute("SELECT * FROM air_zones").fetchall()]
        requests = [dict(row) for row in connection.execute(
            "SELECT * FROM air_requests WHERE open = 1").fetchall()]
        loans = [dict(row) for row in connection.execute("SELECT * FROM air_loans").fetchall()]
        cap_row = connection.execute("SELECT value FROM air_meta WHERE key = 'capacity'").fetchone()
        total_capacity = int(json.loads(cap_row["value"])) if cap_row else 0
        zone_ids = {z["id"] for z in zones}
        zone_epoch = {z["id"]: z["epoch"] for z in zones}
        now = utcnow()

        # 1) 过期让渡账先收口（epoch 翻页 = 报警变化导致旧账作废；recalled 在捐赠方报警解除后关账）
        for loan in loans:
            if loan["status"] == "active":
                if (loan["donor_id"] not in zone_ids or loan["receiver_id"] not in zone_ids
                        or loan["donor_epoch"] != zone_epoch.get(loan["donor_id"])
                        or loan["receiver_epoch"] != zone_epoch.get(loan["receiver_id"])):
                    self._close_loan(connection, loan, "closed", now=now)
            elif loan["status"] == "recalled":
                donor = next((z for z in zones if z["id"] == loan["donor_id"]), None)
                if (not donor or donor["alarm"] == "none"
                        or donor["epoch"] != loan["donor_epoch"]):
                    self._close_loan(connection, loan, "closed", now=now)

        loans = [dict(row) for row in connection.execute("SELECT * FROM air_loans").fetchall()]
        # 本次点名让渡：同对没有可生效的账时，先构造一笔虚拟强制让渡参与分配，
        # 分配器若判定划转成立，下面第3步再真正落账。
        if expect_yield:
            expected_key, donor_id, receiver_id, want_amount, _yield_key = expect_yield
            same_pair = next((loan for loan in loans if loan["loan_key"] == expected_key), None)
            if not same_pair or same_pair["status"] not in ("active", "recalled"):
                loans.append({
                    "loan_key": expected_key,
                    "donor_id": donor_id,
                    "receiver_id": receiver_id,
                    "amount": want_amount,
                    "status": "active",
                    "donor_epoch": zone_epoch.get(donor_id, 0),
                    "receiver_epoch": zone_epoch.get(receiver_id, 0),
                    "yield_key": None,
                    "forced": 1,
                })
        directives = {}
        for loan in loans:
            if loan["status"] == "active" and loan["forced"]:
                directives[loan["loan_key"]] = loan["amount"]
        if expect_yield:
            directives[expect_yield[0]] = expect_yield[3]
        result = allocate(
            [_zone_snapshot(z) for z in zones],
            [{"id": r["id"], "zone_id": r["zone_id"], "amount": r["amount"],
              "enqueued_at": r["enqueued_at"], "seq": r["seq"]} for r in requests],
            loans,
            total_capacity,
            prefer,
            directives,
        )

        # 2) 占用写回各申请
        for req in requests:
            served = int(result.served.get(req["id"], 0))
            connection.execute("UPDATE air_requests SET served = ? WHERE id = ?", (served, req["id"]))

        # 3) 让渡账落账：本次重算实际发生的 donor -> receiver 让出
        expected_key = expect_yield[0] if expect_yield else None
        active_pairs = set()
        for (donor_id, receiver_id), amount in sorted(result.preempted.items()):
            if amount <= 0:
                continue
            loan_key = "%s:%s" % (donor_id, receiver_id)
            active_pairs.add(loan_key)
            row = connection.execute("SELECT * FROM air_loans WHERE loan_key = ?", (loan_key,)).fetchone()
            if row and row["status"] == "recalled":
                # 收回账仍有效时分配器不会再产生该对让出；出现即说明冻结失效，按防御性跳过
                continue
            forced = bool(row["forced"]) if row else False
            yield_key = row["yield_key"] if row else None
            if expect_yield and expected_key == loan_key:
                _, _, _, want_amount, want_key = expect_yield
                forced = True
                yield_key = want_key or yield_key
                amount = want_amount
            if row:
                connection.execute(
                    "UPDATE air_loans SET amount = ?, status = 'active', donor_epoch = ?, "
                    "receiver_epoch = ?, forced = ?, yield_key = COALESCE(?, yield_key), updated_at = ? "
                    "WHERE loan_key = ?",
                    (amount, zone_epoch.get(donor_id, 0), zone_epoch.get(receiver_id, 0),
                     1 if forced else 0, yield_key, now, loan_key),
                )
            else:
                connection.execute(
                    "INSERT INTO air_loans(loan_key, donor_id, receiver_id, amount, status, donor_epoch, "
                    "receiver_epoch, yield_key, forced, created_by, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, 'active', ?, ?, ?, ?, ?, ?, ?)",
                    (loan_key, donor_id, receiver_id, amount,
                     zone_epoch.get(donor_id, 0), zone_epoch.get(receiver_id, 0),
                     yield_key, 1 if forced else 0, actor.user_id if actor else "system", now, now),
                )
            if yield_key:
                connection.execute(
                    "INSERT OR IGNORE INTO air_yield_idem(yield_key, loan_key, created_at) VALUES(?, ?, ?)",
                    (yield_key, loan_key, now),
                )
            self._audit(connection, actor or _SYSTEM_ACTOR, "air.loan_book",
                        {"donor_id": donor_id, "receiver_id": receiver_id,
                         "amount": amount, "forced": forced, "reason": reason},
                        entity_id=loan_key)

        # 4) 本次不再发生让出、且未被召回的活动账关账
        for loan in connection.execute("SELECT * FROM air_loans WHERE status = 'active'").fetchall():
            if loan["loan_key"] not in active_pairs:
                self._close_loan(connection, dict(loan), "closed", now=now)
                self._audit(connection, actor or _SYSTEM_ACTOR, "air.loan_return",
                            {"donor_id": loan["donor_id"], "receiver_id": loan["receiver_id"],
                             "reason": reason}, entity_id=loan["loan_key"])


class _SystemActor:
    user_id = "system"
    role = "admin"


_SYSTEM_ACTOR = _SystemActor()
