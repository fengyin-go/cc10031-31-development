"""任务队列领域服务 — 消费编排结果生成执行任务。

数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物

数据流:
    channel("rpa.steps_submitted") ← step_editor
    → task_queue 消费 → 生成排队任务 + execution_log 记录

处理项口径（流程版本快照）:
    process_item 提交 → 加载关联的流程版本快照
    → 按快照中的步骤集合生成执行计划任务清单
    → 重复提交幂等只保留一条结果，处理台账与结果清单联动核对
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.flow_version import FlowVersionService
from app.store import store


class TaskQueueService(BaseModuleService):
    MODULE = "task_queue"
    ENTITY = "排队任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务编号", "流程名称", "提交时间", "优先级", "预计执行", "排队时长", "当前状态"]
    REQUIRED_FIELDS = ["任务编号", "流程名称", "提交时间"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "已取消", "执行失败"]
    ACTION_RULES = {"取消任务": "已取消", "提升优先级": "排队中"}
    NEGATIVE_ACTIONS = []

    # ---- 跨模块业务方法 ----

    def consume_steps_to_queue(self) -> dict[str, Any]:
        """消费 step_editor 提交的编排结果，生成任务队列条目。

        从 channel("rpa.steps_submitted") 读取编排计划，
        为每个 bot 的每一步创建一个排队任务。

        数据流终点：这里是编排链路的末端 — 任务进入队列等待调度执行。

        Returns:
            {"generated": int, "plans_consumed": int, "task_ids": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.steps_submitted",
        )
        if not msgs:
            return {"generated": 0, "plans_consumed": 0, "task_ids": []}

        task_ids = []
        for msg in msgs:
            plan = msg.payload
            bot_id = plan.get("bot_id", 0)
            steps = plan.get("steps", [])
            if not steps:
                # 确认空计划
                store.channel.confirm(msg.msg_id, self.MODULE)
                continue

            for i, step in enumerate(steps):
                seq = i + 1
                now = datetime.now(timezone.utc)
                task_number = f"TASK-{bot_id}-{seq:03d}"

                values = {
                    "任务编号": task_number,
                    "流程名称": f"Bot-{bot_id}",
                    "提交时间": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "优先级": "高" if seq <= 3 else "中",
                    "预计执行": step.get("name", f"Step-{seq}"),
                    "排队时长": "0s",
                    "当前状态": "排队中",
                }
                entry, missing = self.create_entry(values)
                if entry:
                    entry["step_id"] = step.get("step_id")
                    entry["bot_id"] = bot_id
                    entry["step_type"] = step.get("type", "")
                    task_ids.append(entry["id"])

            store.channel.confirm(msg.msg_id, self.MODULE)

        return {
            "generated": len(task_ids),
            "plans_consumed": len(msgs),
            "task_ids": task_ids,
        }

    def update_queue_status(self, task_id: int,
                             status: str, result: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """更新任务状态并在执行完成时写 execution_log。

        Args:
            task_id: 任务 ID
            status: 目标状态（"执行中", "已完成", "执行失败"）
            result: 执行结果数据（可选）

        Returns:
            更新后的任务信息或 None
        """
        entry = store.find(self.MODULE, task_id)
        if entry is None:
            return None

        valid_statuses = {"执行中", "已完成", "已取消", "执行失败"}
        if status not in valid_statuses:
            return None

        entry["status"] = status
        entry["pending"] = status not in ("已完成", "已取消")
        entry["abnormal"] = status == "执行失败"

        if result:
            entry["result"] = result

        # 完成后写入执行日志通道
        if status in ("已完成", "执行失败"):
            exec_payload = {
                "task_id": task_id,
                "task_number": entry.get("任务编号"),
                "final_status": status,
                "processed_by": self.MODULE,
                "result": result or {},
            }
            store.channel.publish(
                topic="rpa.task_completed",
                source=self.MODULE,
                payload=exec_payload,
            )

        return {
            "task_id": task_id,
            "status": status,
            "task_number": entry.get("任务编号"),
        }

    def schedule_next_batch(self, max_tasks: int = 5) -> dict[str, Any]:
        """从排队中取下一批任务标记为执行中。

        模拟调度器的取任务逻辑 — 按优先级 + 提交时间排序。

        Returns:
            {"scheduled": int, "task_ids": [...]}
        """
        rows = store.rows(self.MODULE)
        queued = [
            r for r in rows
            if r.get("status") == "排队中" and not r.get("abnormal")
        ]
        # 按优先级排序（高 > 中 > 低）
        priority_order = {"高": 0, "中": 1, "低": 2}
        queued.sort(key=lambda r: priority_order.get(r.get("优先级", "低"), 99))

        scheduled = []
        for task in queued[:max_tasks]:
            task["status"] = "执行中"
            task["pending"] = True
            task["开始执行时间"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S")
            scheduled.append(task["id"])
            # 模拟执行记录
            store.channel.publish(
                topic="rpa.task_started",
                source=self.MODULE,
                payload={
                    "task_id": task["id"],
                    "task_number": task.get("任务编号"),
                    "start_time": task["开始执行时间"],
                },
            )

        return {"scheduled": len(scheduled), "task_ids": scheduled}

    # ---- 处理项口径：按流程版本快照生成执行计划任务清单 ----

    def submit_process_item(
        self,
        bot_id: int,
        *,
        version: str | None = None,
        snapshot_id: int | None = None,
        idempotency_key: str | None = None,
        source: str = "任务队列",
    ) -> dict[str, Any]:
        """提交处理项：加载关联的流程版本快照并生成执行计划任务清单。

        - 显式传 snapshot_id/version 时沿用指定版本口径，否则取该流程
          最新已冻结可执行版本；
        - 同一 idempotency_key 重复提交只保留一条结果（幂等）；
        - 任务清单严格按快照步骤集合生成，后续步骤编辑不影响本处理项。

        Returns:
            含 item_id、snapshot_id、task_ids 的处理结果
        """
        bot = store.find("bot_definition", bot_id)
        if bot is None:
            return {"ok": False, "message": f"RPA流程 {bot_id} 不存在或已归档"}

        snapshot = self._resolve_snapshot(bot_id, version, snapshot_id)
        if snapshot is None:
            return {"ok": False, "message": "未找到已冻结的流程版本快照，请先发布流程"}

        idem = idempotency_key or f"item:{bot_id}:{snapshot.snapshot_id}:{source}"
        existed = store.process_items.find_by_idempotency_key(idem)
        if existed is not None:
            # 重复提交只保留一条结果：返回既有处理项，不重复建任务
            return {
                "ok": True,
                "deduplicated": True,
                "item_id": existed.item_id,
                "item_no": existed.item_no,
                "snapshot_id": existed.snapshot_id,
                "version": existed.version,
                "task_ids": list(existed.task_ids),
                "message": "重复提交已命中幂等键，沿用既有处理项与版本口径",
            }

        item = store.process_items.add(
            item_no=f"PI-{bot_id}-{snapshot.snapshot_id}-{store.process_items.count + 1:04d}",
            bot_id=bot_id,
            bot_name=snapshot.bot_name,
            target_system=snapshot.target_system,
            snapshot_id=snapshot.snapshot_id,
            version=snapshot.version,
            idempotency_key=idem,
            source=source,
        )

        plan = self._flow_versions.build_execution_plan(snapshot)
        task_ids = self._materialize_tasks(item, plan)
        item.task_ids = task_ids

        store.channel.publish(
            topic="rpa.process_item_submitted",
            source=self.MODULE,
            payload={
                "item_id": item.item_id,
                "item_no": item.item_no,
                "bot_id": bot_id,
                "snapshot_id": snapshot.snapshot_id,
                "version": snapshot.version,
                "plan_source": snapshot.source,
                "task_count": len(task_ids),
                "source": source,
            },
        )
        return {
            "ok": True,
            "deduplicated": False,
            "item_id": item.item_id,
            "item_no": item.item_no,
            "snapshot_id": snapshot.snapshot_id,
            "version": snapshot.version,
            "plan_source": snapshot.source,
            "task_ids": task_ids,
            "task_count": len(task_ids),
        }

    def list_process_items(
        self,
        *,
        bot_name: str | None = None,
        version: str | None = None,
        status: str | None = None,
        source: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """处理项查询翻页定位，可按流程名称、版本、状态与业务入口过滤。"""
        rows, total = store.process_items.list(
            bot_name=bot_name, version=version, status=status,
            source=source, page=page, size=size)
        return [self._item_summary(it) for it in rows], total

    def get_process_item(self, item_id: int) -> dict[str, Any] | None:
        item = store.process_items.get(item_id)
        if item is None:
            return None
        detail = self._item_summary(item)
        detail["tasks"] = [
            store.find(self.MODULE, tid) for tid in item.task_ids
            if store.find(self.MODULE, tid) is not None
        ]
        detail["result"] = item.result
        return detail

    def complete_process_item(self, item_id: int,
                              result: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """处理完成：联动更新待办状态，并写处理台账与结果清单。

        同一处理项重复完成不会产生第二条结果（幂等口径）。
        """
        item = store.process_items.get(item_id)
        if item is None:
            return None
        if item.status == store.process_items.STATUS_DONE:
            return {
                "ok": True,
                "deduplicated": True,
                "item_id": item_id,
                "message": "处理项已完成，重复提交只保留一条结果",
                "result": item.result,
            }

        tasks = [
            store.find(self.MODULE, tid) for tid in item.task_ids
            if store.find(self.MODULE, tid) is not None
        ]
        for task in tasks:
            task["status"] = "已完成"
            task["pending"] = False
            task["当前状态"] = "已完成"
            task["完成时间"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            store.channel.publish(
                topic="rpa.task_completed",
                source=self.MODULE,
                payload={
                    "task_id": task["id"],
                    "task_number": task.get("任务编号"),
                    "final_status": "已完成",
                    "processed_by": self.MODULE,
                    "item_id": item_id,
                    "snapshot_id": item.snapshot_id,
                },
            )

        item.status = store.process_items.STATUS_DONE
        item.result = result or {"outcome": "success", "note": "按快照步骤集合执行完成"}
        store.channel.publish(
            topic="rpa.process_item_completed",
            source=self.MODULE,
            payload={
                "item_id": item_id,
                "item_no": item.item_no,
                "snapshot_id": item.snapshot_id,
                "version": item.version,
                "result": item.result,
            },
        )
        return {
            "ok": True,
            "deduplicated": False,
            "item_id": item_id,
            "status": item.status,
            "task_count": len(tasks),
            "result": item.result,
        }

    def replay_process_item(self, item_id: int) -> dict[str, Any] | None:
        """沿用既有版本口径重放处理项：不重新选版本，按原快照重建任务清单。"""
        item = store.process_items.get(item_id)
        if item is None:
            return None
        snapshot = store.flow_versions.get(item.snapshot_id)
        if snapshot is None:
            return {"ok": False, "message": "关联快照缺失，无法沿用既有版本口径"}
        plan = self._flow_versions.build_execution_plan(snapshot)
        task_ids = self._materialize_tasks(item, plan, suffix="R")
        item.task_ids.extend(task_ids)
        item.status = store.process_items.STATUS_TODO
        return {
            "ok": True,
            "item_id": item_id,
            "snapshot_id": snapshot.snapshot_id,
            "version": snapshot.version,
            "new_task_ids": task_ids,
        }

    def reconcile_ledger(self) -> dict[str, Any]:
        """处理台账与结果清单联动核对：任务清单、处理项状态与结果一一对应。"""
        items = store.process_items.all()
        task_rows = store.rows(self.MODULE)
        details = []
        mismatches = 0
        for item in items:
            linked_tasks = [
                t for t in task_rows if t.get("item_id") == item.item_id
            ]
            done_tasks = [t for t in linked_tasks if t.get("status") == "已完成"]
            has_result = item.result is not None
            consistent = (
                len(linked_tasks) == len(item.task_ids)
                and (item.status != store.process_items.STATUS_DONE
                     or (len(done_tasks) == len(linked_tasks) and has_result))
            )
            if not consistent:
                mismatches += 1
            details.append({
                "item_id": item.item_id,
                "item_no": item.item_no,
                "version": item.version,
                "snapshot_id": item.snapshot_id,
                "status": item.status,
                "planned_tasks": len(item.task_ids),
                "linked_tasks": len(linked_tasks),
                "done_tasks": len(done_tasks),
                "has_result": has_result,
                "consistent": consistent,
            })
        return {
            "total_items": len(items),
            "mismatches": mismatches,
            "consistent": mismatches == 0,
            "details": details,
        }

    # ---- 处理项内部辅助 ----

    _flow_versions = FlowVersionService()

    def _resolve_snapshot(self, bot_id: int, version: str | None,
                          snapshot_id: int | None):
        if snapshot_id is not None:
            snapshot = store.flow_versions.get(snapshot_id)
            if snapshot is not None and snapshot.bot_id == bot_id:
                return snapshot
        bot = store.find("bot_definition", bot_id)
        if version is not None and bot is not None:
            snapshot = store.flow_versions.find_by_bot_version(
                str(bot.get("流程名称", "")), version)
            if snapshot is not None:
                return snapshot
        return store.flow_versions.latest_executable(bot_id)

    def _materialize_tasks(self, item, plan: dict[str, Any],
                           suffix: str = "") -> list[int]:
        """按快照执行计划逐步骤落任务清单，任务与版本快照绑定。"""
        task_ids: list[int] = []
        for step in plan["steps"]:
            seq = int(step.get("seq", len(task_ids) + 1))
            task_number = (
                f"TASK-{item.bot_id}-V{plan['snapshot_id']}-{seq:03d}{suffix}"
            )
            values = {
                "任务编号": task_number,
                "流程名称": item.bot_name,
                "提交时间": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "优先级": "高" if seq <= 3 else "中",
                "预计执行": step.get("name", f"Step-{seq}"),
                "排队时长": "0s",
                "当前状态": "排队中",
            }
            entry, missing = self.create_entry(values)
            if entry:
                entry["status"] = "排队中"
                entry["bot_id"] = item.bot_id
                entry["item_id"] = item.item_id
                entry["snapshot_id"] = item.snapshot_id
                entry["version"] = item.version
                entry["plan_source"] = plan["plan_source"]
                entry["step_id"] = step.get("step_id")
                entry["step_type"] = step.get("type", "")
                entry["step_seq"] = seq
                task_ids.append(entry["id"])
        return task_ids

    @staticmethod
    def _item_summary(item) -> dict[str, Any]:
        return {
            "id": item.item_id,
            "item_id": item.item_id,
            "item_no": item.item_no,
            "bot_id": item.bot_id,
            "流程名称": item.bot_name,
            "bot_name": item.bot_name,
            "目标系统": item.target_system,
            "snapshot_id": item.snapshot_id,
            "版本号": item.version,
            "version": item.version,
            "source": item.source,
            "status": item.status,
            "created_at": item.created_at,
            "task_count": len(item.task_ids),
            "task_ids": list(item.task_ids),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 通道消费统计
        consumed = store.channel.snapshot(topic="rpa.steps_submitted")
        completed = store.channel.snapshot(topic="rpa.task_completed")
        return {
            **base,
            "steps_plans_consumed": len(consumed),
            "tasks_completed": len(completed),
            "process_items": store.process_items.snapshot(),
        }