"""流程版本快照领域服务 — 发布时原子冻结步骤集合，建立可检索的版本关系。

背景：
    流程发布原本只更新 bot_definition 的状态，步骤记录仍在 step_editor
    同一组数据上继续被编辑，已经生成的执行计划无法说明依据哪一版步骤。

本服务在发布瞬间读取 RPA 流程配置与当前流程版本编排的步骤数据，解析
发布采用的步骤集合与参数，深拷贝生成带版本标识的流程版本快照：
    - 流程版本与步骤集合在同一把锁内原子冻结，后续步骤编辑不可改写；
    - 支持按流程名称 + 版本定位执行计划来源；
    - 发布与步骤编辑并发时支持两种冲突策略，且每次冲突只产生一个
      可执行流程版本（31号处理终态：已冻结 / 待发布）。

数据流:
    bot_definition(发布) / step_editor(步骤)
    → FlowVersionStore 冻结快照
    → channel("rpa.flow_version_published")
    → task_queue 按处理项加载快照生成执行计划任务清单
"""
from __future__ import annotations

import re
from typing import Any

from app.store import FlowVersionSnapshot, store

# 冻结步骤时保留的步骤字段（发布采用的步骤集合与参数口径）
STEP_FIELDS = [
    "步骤序号", "步骤名称", "操作类型", "目标元素",
    "输入参数", "等待策略", "异常处理",
]


class FlowVersionService:
    """流程版本快照管理 — 纯跨模块服务，不绑定单一资源表。"""

    SERVICE_NAME = "flow_version"

    # 发布与编辑并发时的两种冲突处理策略
    POLICY_FREEZE_BEFORE_EDIT = "freeze_before_edit"   # 发布事件冻结编辑前步骤
    POLICY_PUBLISH_AFTER_EDIT = "publish_after_edit"   # 等编辑完成后再形成发布版本

    # ---- 步骤解析 ----

    def resolve_steps(self, bot: dict[str, Any]) -> list[dict[str, Any]]:
        """读取当前流程版本编排的步骤数据，解析发布采用的步骤集合与参数。

        关联优先级：步骤上的 bot_id → 步骤名称与流程名称前缀匹配 →
        同流程上下文步骤。已废弃步骤不进入发布版本。
        """
        bot_id = bot.get("id")
        bot_name = str(bot.get("流程名称", ""))
        rows = store.rows("step_editor")

        linked = [r for r in rows if r.get("bot_id") == bot_id]
        if not linked and bot_name:
            prefix = bot_name[:4]
            linked = [
                r for r in rows
                if prefix and prefix in str(r.get("步骤名称", ""))
            ]
        if not linked:
            # 兜底：取当前全部未废弃步骤，保证发布仍能冻结出一个版本
            linked = [r for r in rows]

        active = [r for r in linked if r.get("status") != "已废弃"] or linked

        def seq_key(row: dict[str, Any]) -> tuple[int, int]:
            raw = str(row.get("步骤序号", "")).strip()
            match = re.search(r"\d+", raw)
            return (int(match.group()) if match else 9999, int(row.get("id", 0)))

        ordered = sorted(active, key=seq_key)

        frozen_steps: list[dict[str, Any]] = []
        for idx, row in enumerate(ordered, start=1):
            frozen_steps.append({
                "step_id": row.get("id"),
                "seq": self._safe_seq(row.get("步骤序号"), idx),
                "name": row.get("步骤名称", f"Step-{idx}"),
                "type": row.get("操作类型", ""),
                "element": row.get("目标元素", ""),
                "params": row.get("输入参数", ""),
                "wait": row.get("等待策略", ""),
                "on_error": row.get("异常处理", ""),
                "step_status": row.get("status", ""),
                "raw": {field: row.get(field, "") for field in STEP_FIELDS},
            })
        return frozen_steps

    @staticmethod
    def _safe_seq(value: Any, fallback: int) -> int:
        match = re.search(r"\d+", str(value or ""))
        return int(match.group()) if match else fallback

    @staticmethod
    def _build_version(bot: dict[str, Any], seq: int) -> str:
        """生成带版本标识的版本号：沿用流程版本号的主次版本，补丁位随发布递增。"""
        match = re.match(r"^\s*(\d+)\.(\d+)\.", str(bot.get("版本号", "")))
        major, minor = match.groups() if match else ("1", "0")
        return f"{major}.{minor}.{seq - 1}"

    # ---- 发布冻结（含并发冲突处理） ----

    def publish_freeze(
        self,
        bot_id: int,
        conflict_policy: str = POLICY_FREEZE_BEFORE_EDIT,
    ) -> tuple[FlowVersionSnapshot | None, str]:
        """发布流程时冻结步骤集合，返回 (快照, 说明)。

        并发冲突两种处理方式都只生成一个可执行流程版本：
        - freeze_before_edit：发布事件直接冻结编辑前步骤，立即进入「已冻结」；
        - publish_after_edit：先登记「待发布」版本，等编辑完成后再转「已冻结」。
        """
        bot = store.find("bot_definition", bot_id)
        if bot is None:
            return None, "流程不存在或已归档"

        editing = store.flow_versions.is_editing(bot_id)
        steps = self.resolve_steps(bot)
        seq = store.flow_versions.next_seq(bot_id)
        version = self._build_version(bot, seq)
        source = (
            f"发布冻结:bot_definition#{bot_id}/step_editor"
            f"@step_count={len(steps)}"
        )
        idem_key = f"publish:{bot_id}:{seq}"

        if editing and conflict_policy == self.POLICY_PUBLISH_AFTER_EDIT:
            snapshot = store.flow_versions.freeze(
                bot_id=bot_id,
                bot_name=str(bot.get("流程名称", "")),
                target_system=str(bot.get("目标系统", "")),
                version=version,
                version_seq=seq,
                steps=steps,
                source=source,
                freeze_policy=self.POLICY_PUBLISH_AFTER_EDIT,
                status=store.flow_versions.STATUS_PENDING_PUBLISH,
                conflict=True,
                idempotency_key=idem_key,
            )
            return snapshot, "发布与步骤编辑并发：版本已登记待发布，编辑完成后形成可执行版本"

        snapshot = store.flow_versions.freeze(
            bot_id=bot_id,
            bot_name=str(bot.get("流程名称", "")),
            target_system=str(bot.get("目标系统", "")),
            version=version,
            version_seq=seq,
            steps=steps,
            source=source,
            freeze_policy=(
                self.POLICY_FREEZE_BEFORE_EDIT if editing else "normal_publish"
            ),
            status=store.flow_versions.STATUS_FROZEN,
            conflict=editing,
            idempotency_key=idem_key,
        )
        # 回写流程当前版本标识，保持流程定义与快照口径一致
        bot["版本号"] = version
        bot["version"] = version
        self._announce_published(snapshot)
        note = "发布与步骤编辑并发：已冻结编辑前步骤" if editing else "发布成功，步骤集合已原子冻结"
        return snapshot, note

    def finish_edit(self, bot_id: int) -> dict[str, Any]:
        """步骤编辑完成：关闭编辑会话。

        - 待发布版本（publish_after_edit 策略）在此转为已冻结终态；
        - 已冻结版本（freeze_before_edit 策略）不再产生新版本，
          编辑结果只影响后续发布，保证一次冲突只有一个可执行版本。
        """
        session = store.flow_versions.end_edit(bot_id)
        pending = next(
            (s for s in store.flow_versions.all()
             if s.bot_id == bot_id
             and s.status == store.flow_versions.STATUS_PENDING_PUBLISH),
            None,
        )
        promoted = False
        if pending is not None:
            store.flow_versions.mark_published(pending)
            bot = store.find("bot_definition", bot_id)
            if bot is not None:
                bot["版本号"] = pending.version
                bot["version"] = pending.version
            self._announce_published(pending)
            promoted = True
        return {
            "bot_id": bot_id,
            "session_closed": session is not None,
            "pending_version_promoted": promoted,
            "new_version_created": False,  # 编辑完成不额外生成版本
        }

    def begin_edit(self, bot_id: int, scope: str = "步骤编排") -> dict[str, Any]:
        """登记步骤编辑会话，用于发布事件判定并发冲突。"""
        bot = store.find("bot_definition", bot_id)
        if bot is None:
            return {"ok": False, "message": "流程不存在或已归档"}
        session = store.flow_versions.begin_edit(bot_id, scope=scope)
        store.channel.publish(
            topic="rpa.step_editing",
            source=self.SERVICE_NAME,
            payload={"bot_id": bot_id, "scope": scope, "session": session},
        )
        return {"ok": True, "session": session}

    # ---- 查询与定位 ----

    def list_snapshots(
        self,
        *,
        bot_name: str | None = None,
        target_system: str | None = None,
        version: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """按流程名称、目标系统、版本查看快照（翻页定位）。"""
        rows, total = store.flow_versions.list(
            bot_name=bot_name, target_system=target_system,
            version=version, page=page, size=size)
        return [self._summary(s) for s in rows], total

    def get_snapshot(self, snapshot_id: int) -> dict[str, Any] | None:
        snapshot = store.flow_versions.get(snapshot_id)
        if snapshot is None:
            return None
        detail = self._summary(snapshot)
        detail["steps"] = snapshot.steps
        detail["tasks"] = self.tasks_for_snapshot(snapshot_id)
        return detail

    def locate_plan_source(self, bot_name: str, version: str) -> dict[str, Any] | None:
        """按流程名称和版本定位执行计划数据来源。"""
        snapshot = store.flow_versions.find_by_bot_version(bot_name, version)
        if snapshot is None:
            return None
        return {
            "snapshot_id": snapshot.snapshot_id,
            "bot_name": snapshot.bot_name,
            "target_system": snapshot.target_system,
            "version": snapshot.version,
            "status": snapshot.status,
            "plan_source": snapshot.source,
            "step_count": snapshot.step_count,
            "freeze_policy": snapshot.freeze_policy,
            "conflict": snapshot.conflict,
            "published_at": snapshot.published_at,
            "tasks": self.tasks_for_snapshot(snapshot.snapshot_id),
        }

    def tasks_for_snapshot(self, snapshot_id: int) -> list[dict[str, Any]]:
        """核对某版本快照生成的流程任务（执行计划任务清单）。"""
        tasks = store.rows("task_queue")
        return [
            {
                "task_id": t.get("id"),
                "任务编号": t.get("任务编号"),
                "当前状态": t.get("status"),
                "步骤名称": t.get("预计执行"),
                "snapshot_id": t.get("snapshot_id"),
                "version": t.get("version"),
            }
            for t in tasks if t.get("snapshot_id") == snapshot_id
        ]

    def build_execution_plan(self, snapshot: FlowVersionSnapshot) -> dict[str, Any]:
        """按快照中的步骤集合生成执行计划（任务队列消费口径）。"""
        return {
            "snapshot_id": snapshot.snapshot_id,
            "bot_id": snapshot.bot_id,
            "bot_name": snapshot.bot_name,
            "version": snapshot.version,
            "plan_type": "snapshot_sequential",
            "plan_source": snapshot.source,
            "total_steps": snapshot.step_count,
            "steps": [
                {
                    "step_id": step.get("step_id"),
                    "seq": step.get("seq"),
                    "name": step.get("name"),
                    "type": step.get("type"),
                    "element": step.get("element"),
                    "params": step.get("params"),
                    "wait": step.get("wait"),
                    "on_error": step.get("on_error"),
                }
                for step in snapshot.steps
            ],
        }

    # ---- 统计 ----

    def domain_snapshot(self) -> dict[str, Any]:
        return {
            "module": self.SERVICE_NAME,
            "entity": "流程版本快照",
            "artifact": "流程版本快照库",
            **store.flow_versions.snapshot(),
        }

    # ---- 内部 ----

    def _announce_published(self, snapshot: FlowVersionSnapshot) -> None:
        store.channel.publish(
            topic="rpa.flow_version_published",
            source=self.SERVICE_NAME,
            payload={
                "snapshot_id": snapshot.snapshot_id,
                "bot_id": snapshot.bot_id,
                "bot_name": snapshot.bot_name,
                "version": snapshot.version,
                "step_count": snapshot.step_count,
                "plan_source": snapshot.source,
            },
        )

    @staticmethod
    def _summary(snapshot: FlowVersionSnapshot) -> dict[str, Any]:
        return {
            "id": snapshot.snapshot_id,
            "snapshot_id": snapshot.snapshot_id,
            "bot_id": snapshot.bot_id,
            "流程名称": snapshot.bot_name,
            "bot_name": snapshot.bot_name,
            "目标系统": snapshot.target_system,
            "target_system": snapshot.target_system,
            "版本号": snapshot.version,
            "version": snapshot.version,
            "version_seq": snapshot.version_seq,
            "步骤数": snapshot.step_count,
            "step_count": snapshot.step_count,
            "执行计划来源": snapshot.source,
            "plan_source": snapshot.source,
            "冻结策略": snapshot.freeze_policy,
            "freeze_policy": snapshot.freeze_policy,
            "并发冲突": snapshot.conflict,
            "conflict": snapshot.conflict,
            "status": snapshot.status,
            "created_at": snapshot.created_at,
            "published_at": snapshot.published_at,
        }


# 全局单例：与 runtime_session / callback_hook 同样的跨模块服务口径
flow_version_service = FlowVersionService()
