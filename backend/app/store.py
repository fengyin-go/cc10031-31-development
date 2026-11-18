"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

跨模块数据流能力：
- DataChannel: 服务间数据发布/消费通道，支持 topic 订阅、快照读、消费确认
- DirtyDataVault: 脏数据隔离区，存储校验不通过/解析失败的脏数据记录
- FailureRecovery: 失败恢复管理器，记录失败步骤并支持重试
- CollectRecord: 采集记录存储，记录每次采集产出的结构化数据
"""
from __future__ import annotations

import threading
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS


def utc_now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

# ---------------------------------------------------------------------------
# 跨模块数据通道 (DataChannel) — 服务间发布/消费
# ---------------------------------------------------------------------------

@dataclass
class ChannelMessage:
    """数据通道中的单条消息，带 topic、来源、负载和确认状态。"""
    msg_id: int
    topic: str
    source_module: str
    payload: dict[str, Any]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    consumed_by: list[str] = field(default_factory=list)
    confirmed: bool = False

    def mark_consumed(self, consumer: str) -> None:
        if consumer not in self.consumed_by:
            self.consumed_by.append(consumer)


class DataChannel:
    """跨模块数据通道：支持 topic 发布/订阅，服务间数据流转的桥梁。

    用法::

        store.channel.publish("rpa.bot_created", "bot_definition", bot_data)
        msgs = store.channel.consume("step_editor", "rpa.bot_created")
        store.channel.confirm(msg_id, "step_editor")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._messages: list[ChannelMessage] = []

    @property
    def total_messages(self) -> int:
        with self._lock:
            return len(self._messages)

    def publish(self, topic: str, source: str, payload: dict[str, Any]) -> ChannelMessage:
        with self._lock:
            self._seq += 1
            msg = ChannelMessage(
                msg_id=self._seq, topic=topic,
                source_module=source, payload=payload,
            )
            self._messages.append(msg)
            return msg

    def consume(self, consumer: str, topic: str | None = None,
                since_msg_id: int | None = None) -> list[ChannelMessage]:
        """获取指定 consumer 尚未消费的消息。"""
        with self._lock:
            results: list[ChannelMessage] = []
            for msg in self._messages:
                if topic and msg.topic != topic:
                    continue
                if since_msg_id is not None and msg.msg_id <= since_msg_id:
                    continue
                if consumer not in msg.consumed_by:
                    results.append(msg)
                    msg.mark_consumed(consumer)
            return results

    def confirm(self, msg_id: int, consumer: str) -> bool:
        with self._lock:
            for msg in self._messages:
                if msg.msg_id == msg_id:
                    msg.confirmed = True
                    return True
            return False

    def snapshot(self, topic: str | None = None) -> list[dict[str, Any]]:
        """返回消息快照，供跨模块分析/看板消费。"""
        with self._lock:
            msgs = self._messages if topic is None else [
                m for m in self._messages if m.topic == topic
            ]
            return [{
                "msg_id": m.msg_id, "topic": m.topic,
                "source": m.source_module, "payload": m.payload,
                "created_at": m.created_at, "consumed_by": m.consumed_by,
                "confirmed": m.confirmed,
            } for m in msgs]

    def latest(self, topic: str) -> dict[str, Any] | None:
        with self._lock:
            for msg in reversed(self._messages):
                if msg.topic == topic:
                    return {
                        "msg_id": msg.msg_id, "topic": msg.topic,
                        "source": msg.source_module, "payload": msg.payload,
                        "created_at": msg.created_at, "confirmed": msg.confirmed,
                    }
            return None

    def uncomfirmed_count(self) -> int:
        with self._lock:
            return sum(1 for m in self._messages if not m.confirmed)


# ---------------------------------------------------------------------------
# 脏数据隔离区 (DirtyDataVault)
# ---------------------------------------------------------------------------

@dataclass
class DirtyRecord:
    """脏数据记录：校验失败或解析异常的数据行。"""
    record_id: int
    source_module: str
    batch_ref: str
    raw_data: dict[str, Any]
    reason: str  # 如 "字段缺失", "格式非法", "解析超时"
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    quarantined: bool = True
    recovered_to: str | None = None


class DirtyDataVault:
    """脏数据隔离区：被校验规则或解析器标记为问题数据后移入此处。

    用法::

        vault = store.dirty_vault
        vault.quarantine("data_extract", "BATC-001", row, "缺少必填字段")
        dirty = vault.list_by_source("data_extract")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._records: list[DirtyRecord] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def quarantine(self, source: str, batch_ref: str,
                   raw_data: dict[str, Any], reason: str) -> DirtyRecord:
        with self._lock:
            self._seq += 1
            rec = DirtyRecord(
                record_id=self._seq, source_module=source,
                batch_ref=batch_ref, raw_data=raw_data, reason=reason,
            )
            self._records.append(rec)
            return rec

    def list_by_source(self, source: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.source_module == source]

    def list_by_batch(self, batch_ref: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.batch_ref == batch_ref]

    def mark_recovered(self, record_id: int, recovered_to: str) -> bool:
        with self._lock:
            for r in self._records:
                if r.record_id == record_id:
                    r.quarantined = False
                    r.recovered_to = recovered_to
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            quarantined = sum(1 for r in self._records if r.quarantined)
            by_reason: dict[str, int] = {}
            for r in self._records:
                by_reason[r.reason] = by_reason.get(r.reason, 0) + 1
            return {
                "total": len(self._records),
                "quarantined": quarantined,
                "recovered": len(self._records) - quarantined,
                "by_reason": by_reason,
            }


# ---------------------------------------------------------------------------
# 采集记录 (CollectRecord) — 每次采集产出的结构化数据
# ---------------------------------------------------------------------------

@dataclass
class CollectEntry:
    entry_id: int
    batch_ref: str
    source_module: str
    fields: dict[str, Any]
    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    validated: bool = False
    quality_score: float = 1.0


class CollectRecordStore:
    """采集记录存储：每次采集流程产出的结构化数据行。

    用法::

        rec = store.collect_records.add("BATC-001", "data_extract", {"字段1": "值1", ...})
        store.collect_records.mark_validated(rec.entry_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[CollectEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def add(self, batch_ref: str, source: str,
            fields: dict[str, Any]) -> CollectEntry:
        with self._lock:
            self._seq += 1
            entry = CollectEntry(
                entry_id=self._seq, batch_ref=batch_ref,
                source_module=source, fields=dict(fields),
            )
            self._entries.append(entry)
            return entry

    def list_by_batch(self, batch_ref: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.batch_ref == batch_ref]

    def list_by_source(self, source: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.source_module == source]

    def mark_validated(self, entry_id: int, score: float = 1.0) -> bool:
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    e.validated = True
                    e.quality_score = score
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            validated = sum(1 for e in self._entries if e.validated)
            by_source: dict[str, int] = {}
            for e in self._entries:
                by_source[e.source_module] = by_source.get(e.source_module, 0) + 1
            return {
                "total": len(self._entries),
                "validated": validated,
                "unvalidated": len(self._entries) - validated,
                "by_source": by_source,
            }


# ---------------------------------------------------------------------------
# 失败恢复管理 (FailureRecovery)
# ---------------------------------------------------------------------------

@dataclass
class RecoveryEntry:
    recovery_id: int
    module: str
    batch_ref: str
    step: str
    error_detail: str
    attempt: int = 0
    max_attempts: int = 3
    resolved: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class FailureRecovery:
    """失败恢复管理器：记录失败步骤并支持自动重试。

    用法::

        rec = store.recovery.log("step_editor", "BATC-001", "元素定位",
                                  "选择器超时未匹配")
        store.recovery.retry(rec.recovery_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[RecoveryEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def log(self, module: str, batch_ref: str, step: str,
            error_detail: str, max_attempts: int = 3) -> RecoveryEntry:
        with self._lock:
            self._seq += 1
            entry = RecoveryEntry(
                recovery_id=self._seq, module=module,
                batch_ref=batch_ref, step=step,
                error_detail=error_detail, max_attempts=max_attempts,
            )
            self._entries.append(entry)
            return entry

    def retry(self, recovery_id: int) -> tuple[bool, str]:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    if e.resolved:
                        return False, "已恢复，无需重试"
                    e.attempt += 1
                    if e.attempt > e.max_attempts:
                        return False, f"已达最大重试次数 {e.max_attempts}"
                    return True, f"重试 {e.attempt}/{e.max_attempts}"
            return False, "恢复记录不存在"

    def resolve(self, recovery_id: int) -> bool:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    e.resolved = True
                    return True
            return False

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries if not e.resolved)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for e in self._entries if not e.resolved)
            return {
                "total": len(self._entries),
                "resolved": len(self._entries) - pending,
                "pending": pending,
            }


# ---------------------------------------------------------------------------
# 流程版本快照库 (FlowVersionStore)
#
# 流程发布只更新状态，步骤记录后续可被继续编辑；没有版本快照时，已生成的
# 执行计划无法说明依据哪一版步骤。这里在发布瞬间对步骤集合做深拷贝冻结，
# 流程版本与步骤集合原子落库，后续步骤编辑不能改写已发布版本的执行依据。
# ---------------------------------------------------------------------------

@dataclass
class FlowVersionSnapshot:
    """流程版本快照：一次发布冻结下来的步骤集合与参数。"""
    snapshot_id: int
    bot_id: int
    bot_name: str
    target_system: str
    version: str                 # 带版本标识，如 1.0.2
    version_seq: int             # 同流程内单调递增的版本序号
    steps: list[dict[str, Any]]  # 冻结时刻的步骤深拷贝（含输入参数）
    step_count: int
    source: str                  # plan_source：执行计划来源说明
    freeze_policy: str           # freeze_before_edit / publish_after_edit
    status: str                  # 已冻结 / 待发布
    conflict: bool = False       # 发布与编辑并发时是否发生过冲突
    created_at: str = field(default_factory=utc_now_text)
    published_at: str | None = None
    idempotency_key: str | None = None  # 与发布事件幂等绑定


class FlowVersionStore:
    """流程版本快照存储：原子冻结、按流程名称+版本定位、并发冲突登记。"""

    # 31号处理终态：发布与编辑并发时，冲突处理只会落在这两个终态之一，
    # 且每次冲突只生成一个可执行流程版本。
    STATUS_FROZEN = "已冻结"
    STATUS_PENDING_PUBLISH = "待发布"

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._seq = 0
        self._snapshots: list[FlowVersionSnapshot] = []
        # bot_id -> 当前编辑会话（编辑中标记），用于发布/编辑并发判定
        self._edit_sessions: dict[int, dict[str, Any]] = {}

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._snapshots)

    def next_seq(self, bot_id: int) -> int:
        """同一流程的版本序号单调递增。"""
        with self._lock:
            existing = [s.version_seq for s in self._snapshots if s.bot_id == bot_id]
            return (max(existing) + 1) if existing else 1

    def freeze(
        self,
        *,
        bot_id: int,
        bot_name: str,
        target_system: str,
        version: str,
        version_seq: int,
        steps: list[dict[str, Any]],
        source: str,
        freeze_policy: str,
        status: str,
        conflict: bool = False,
        idempotency_key: str | None = None,
    ) -> FlowVersionSnapshot:
        """原子冻结：步骤集合深拷贝与版本记录在同一把锁内一起落库。"""
        with self._lock:
            self._seq += 1
            snapshot = FlowVersionSnapshot(
                snapshot_id=self._seq,
                bot_id=bot_id,
                bot_name=bot_name,
                target_system=target_system,
                version=version,
                version_seq=version_seq,
                steps=deepcopy(steps),
                step_count=len(steps),
                source=source,
                freeze_policy=freeze_policy,
                status=status,
                conflict=conflict,
                published_at=utc_now_text() if status == self.STATUS_FROZEN else None,
                idempotency_key=idempotency_key,
            )
            self._snapshots.append(snapshot)
            return snapshot

    def all(self) -> list[FlowVersionSnapshot]:
        with self._lock:
            return list(self._snapshots)

    def get(self, snapshot_id: int) -> FlowVersionSnapshot | None:
        with self._lock:
            return next(
                (s for s in self._snapshots if s.snapshot_id == snapshot_id), None)

    def find_by_bot_version(self, bot_name: str, version: str) -> FlowVersionSnapshot | None:
        """按流程名称 + 版本定位执行计划来源。"""
        with self._lock:
            return next(
                (s for s in self._snapshots
                 if s.bot_name == bot_name and s.version == version), None)

    def latest_for_bot(self, bot_id: int) -> FlowVersionSnapshot | None:
        with self._lock:
            bot_snaps = [s for s in self._snapshots if s.bot_id == bot_id]
            return max(bot_snaps, key=lambda s: s.version_seq, default=None)

    def latest_executable(self, bot_id: int) -> FlowVersionSnapshot | None:
        """最新可执行版本：只取已冻结终态。"""
        with self._lock:
            frozen = [
                s for s in self._snapshots
                if s.bot_id == bot_id and s.status == self.STATUS_FROZEN
            ]
            return max(frozen, key=lambda s: s.version_seq, default=None)

    def list(
        self,
        *,
        bot_name: str | None = None,
        target_system: str | None = None,
        version: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[FlowVersionSnapshot], int]:
        """按流程名称、目标系统、版本过滤并翻页定位。"""
        with self._lock:
            rows = list(self._snapshots)
            if bot_name:
                rows = [s for s in rows if bot_name.casefold() in s.bot_name.casefold()]
            if target_system:
                rows = [
                    s for s in rows
                    if target_system.casefold() in str(s.target_system).casefold()
                ]
            if version:
                rows = [s for s in rows if version.casefold() in s.version.casefold()]
            total = len(rows)
            start = (page - 1) * size
            return rows[start:start + size], total

    # ---- 发布/编辑并发会话 ----

    def begin_edit(self, bot_id: int, scope: str = "步骤编排") -> dict[str, Any]:
        with self._lock:
            session = self._edit_sessions.get(bot_id)
            if session is None:
                session = {
                    "bot_id": bot_id,
                    "editing": True,
                    "scope": scope,
                    "started_at": utc_now_text(),
                }
                self._edit_sessions[bot_id] = session
            return dict(session)

    def end_edit(self, bot_id: int) -> dict[str, Any] | None:
        with self._lock:
            return self._edit_sessions.pop(bot_id, None)

    def is_editing(self, bot_id: int) -> bool:
        with self._lock:
            return bot_id in self._edit_sessions

    def edit_sessions(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(s) for s in self._edit_sessions.values()]

    def mark_published(self, snapshot: FlowVersionSnapshot) -> None:
        """待发布快照在编辑完成后转为已冻结终态。"""
        with self._lock:
            snapshot.status = self.STATUS_FROZEN
            snapshot.published_at = utc_now_text()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            frozen = sum(1 for s in self._snapshots if s.status == self.STATUS_FROZEN)
            pending = sum(1 for s in self._snapshots if s.status == self.STATUS_PENDING_PUBLISH)
            return {
                "total": len(self._snapshots),
                "frozen": frozen,
                "pending_publish": pending,
                "conflicts": sum(1 for s in self._snapshots if s.conflict),
                "editing_bots": list(self._edit_sessions.keys()),
            }


# ---------------------------------------------------------------------------
# 处理项库 (ProcessItemStore)
#
# 任务队列按处理项口径工作：加载处理项关联的流程版本快照，按快照中的
# 步骤集合生成执行计划任务清单。重复提交只保留一条结果（幂等键去重）。
# ---------------------------------------------------------------------------

@dataclass
class ProcessItem:
    """处理项：任务队列的处理单元，关联一个已冻结的流程版本快照。"""
    item_id: int
    item_no: str
    bot_id: int
    bot_name: str
    target_system: str
    snapshot_id: int
    version: str
    idempotency_key: str
    source: str                  # 业务入口，如 任务队列 / 外部业务入口
    status: str                  # 待处理 / 已处理
    created_at: str = field(default_factory=utc_now_text)
    task_ids: list[int] = field(default_factory=list)
    result: dict[str, Any] | None = None


class ProcessItemStore:
    """处理项存储：幂等提交、台账与结果清单联动核对。"""

    STATUS_TODO = "待处理"
    STATUS_DONE = "已处理"

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._seq = 0
        self._items: list[ProcessItem] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._items)

    def find_by_idempotency_key(self, key: str) -> ProcessItem | None:
        with self._lock:
            return next(
                (it for it in self._items if it.idempotency_key == key), None)

    def add(
        self,
        *,
        item_no: str,
        bot_id: int,
        bot_name: str,
        target_system: str,
        snapshot_id: int,
        version: str,
        idempotency_key: str,
        source: str,
    ) -> ProcessItem:
        with self._lock:
            self._seq += 1
            item = ProcessItem(
                item_id=self._seq,
                item_no=item_no,
                bot_id=bot_id,
                bot_name=bot_name,
                target_system=target_system,
                snapshot_id=snapshot_id,
                version=version,
                idempotency_key=idempotency_key,
                source=source,
                status=self.STATUS_TODO,
            )
            self._items.append(item)
            return item

    def get(self, item_id: int) -> ProcessItem | None:
        with self._lock:
            return next((it for it in self._items if it.item_id == item_id), None)

    def all(self) -> list[ProcessItem]:
        with self._lock:
            return list(self._items)

    def list(
        self,
        *,
        bot_name: str | None = None,
        version: str | None = None,
        status: str | None = None,
        source: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[ProcessItem], int]:
        with self._lock:
            rows = list(self._items)
            if bot_name:
                rows = [it for it in rows if bot_name.casefold() in it.bot_name.casefold()]
            if version:
                rows = [it for it in rows if version.casefold() in it.version.casefold()]
            if status:
                rows = [it for it in rows if it.status == status]
            if source:
                rows = [it for it in rows if source.casefold() in it.source.casefold()]
            total = len(rows)
            start = (page - 1) * size
            return rows[start:start + size], total

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            done = sum(1 for it in self._items if it.status == self.STATUS_DONE)
            return {
                "total": len(self._items),
                "todo": len(self._items) - done,
                "done": done,
            }


# ---------------------------------------------------------------------------
# 增强版 Store — 整合通道、脏数据、采集记录、恢复
# ---------------------------------------------------------------------------

class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块基础设施
        self.channel = DataChannel()
        self.dirty_vault = DirtyDataVault()
        self.collect_records = CollectRecordStore()
        self.recovery = FailureRecovery()
        # 流程版本快照库与处理项库
        self.flow_versions = FlowVersionStore()
        self.process_items = ProcessItemStore()

    # ---- 基础数据操作 ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()