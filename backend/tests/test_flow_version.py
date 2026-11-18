"""流程版本快照测试 — 覆盖原子冻结、并发冲突、处理项口径与定位查询。

核心断言：
1. 发布即冻结：流程版本与步骤集合原子落库，后续步骤编辑不改写已发布版本；
2. 可按流程名称 + 版本定位执行计划来源；
3. 任务队列按处理项加载快照生成执行计划任务清单；
4. 重复提交幂等只保留一条结果；台账与结果清单联动核对；
5. 发布与编辑并发的两种策略都只生成一个可执行流程版本（31号处理终态）；
6. 快照/处理项查询支持翻页定位。
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.routers import versioning  # noqa: E402
from app.services.bot_definition import BotDefinitionService  # noqa: E402
from app.services.flow_version import FlowVersionService  # noqa: E402
from app.services.task_queue import TaskQueueService  # noqa: E402
from app.store import store  # noqa: E402


# ---------------------------------------------------------------------------
# 测试夹具：构造带专属步骤的流程，并在结束后清理本测试产生的数据
# ---------------------------------------------------------------------------

def _make_bot(name: str, target: str = "ERP测试系统") -> dict:
    rows = store.rows("bot_definition")
    bot_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    bot = {
        "id": bot_id,
        "status": "测试中",
        "pending": True,
        "abnormal": False,
        "流程名称": name,
        "目标系统": target,
        "触发方式": "手动触发",
        "执行环境": "沙箱Chrome",
        "超时上限": "300s",
        "失败策略": "重试3次",
        "版本号": "2.1.0",
    }
    rows.append(bot)
    return bot


def _make_steps(bot: dict, count: int = 4, mark: str = "") -> list[dict]:
    rows = store.rows("step_editor")
    next_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    created = []
    for i in range(1, count + 1):
        step = {
            "id": next_id + i - 1,
            "status": "已验证",
            "pending": False,
            "abnormal": False,
            "bot_id": bot["id"],
            "步骤序号": str(i),
            "步骤名称": f"{bot['流程名称'][:4]}-{mark}步骤{i}",
            "操作类型": ["navigate", "input", "click", "extract", "click"][i % 5],
            "目标元素": f"#elem-{i}",
            "输入参数": f"param-{i}=v1",
            "等待策略": "visible",
            "异常处理": "retry",
        }
        rows.append(step)
        created.append(step)
    return created


@pytest.fixture()
def bot_with_steps():
    bot = _make_bot(f"快照测试流程{os.getpid()}-{id(object())}")
    steps = _make_steps(bot, count=4, mark="初版")
    created_bot_id = bot["id"]
    created_step_ids = {s["id"] for s in steps}
    try:
        yield bot, steps
    finally:
        store._tables["bot_definition"] = [
            r for r in store.rows("bot_definition") if r.get("id") != created_bot_id
        ]
        store._tables["step_editor"] = [
            r for r in store.rows("step_editor") if r.get("id") not in created_step_ids
        ]
        store._tables["task_queue"] = [
            r for r in store.rows("task_queue") if r.get("bot_id") != created_bot_id
        ]
        fv = store.flow_versions
        with fv._lock:
            fv._snapshots = [s for s in fv._snapshots if s.bot_id != created_bot_id]
            fv._edit_sessions.pop(created_bot_id, None)
        pi = store.process_items
        with pi._lock:
            pi._items = [it for it in pi._items if it.bot_id != created_bot_id]


# ---------------------------------------------------------------------------
# 1. 发布即冻结：原子快照与版本标识
# ---------------------------------------------------------------------------

class TestPublishFreeze:

    def test_publish_creates_frozen_snapshot(self, bot_with_steps):
        bot, steps = bot_with_steps
        result = BotDefinitionService().publish_bot(bot["id"])

        assert result is not None
        assert result["status"] == "已发布"
        assert result["snapshot_status"] == "已冻结"
        assert result["step_count"] == len(steps)

        snapshot = store.flow_versions.get(result["snapshot_id"])
        assert snapshot is not None
        assert snapshot.version == result["version"]
        assert snapshot.version_seq == 1
        assert snapshot.step_count == 4
        # 步骤集合与参数完整冻结
        assert [s["seq"] for s in snapshot.steps] == [1, 2, 3, 4]
        assert all(s["params"].startswith("param-") for s in snapshot.steps)
        assert snapshot.source.startswith("发布冻结:bot_definition#")

    def test_republish_generates_new_version(self, bot_with_steps):
        bot, _ = bot_with_steps
        svc = BotDefinitionService()
        first = svc.publish_bot(bot["id"])
        # 回到测试中后再次发布，应产生新版本号且版本序号递增
        bot["status"] = "测试中"
        second = svc.publish_bot(bot["id"])

        assert second["version_seq"] == 2
        assert second["version"] != first["version"]
        assert second["snapshot_id"] != first["snapshot_id"]

    def test_snapshot_immutable_after_step_editing(self, bot_with_steps):
        """原子冻结：后续步骤编辑不能改写已发布版本的执行计划依据。"""
        bot, steps = bot_with_steps
        svc = BotDefinitionService()
        published = svc.publish_bot(bot["id"])
        snapshot_before = store.flow_versions.get(published["snapshot_id"])
        frozen_params = [s["params"] for s in snapshot_before.steps]
        frozen_names = [s["name"] for s in snapshot_before.steps]

        # 发布后继续编辑同一组步骤记录（模拟现状：直接改写 step_editor 行）
        for step in steps:
            step["输入参数"] = "param-has-been-changed"
            step["步骤名称"] = step["步骤名称"] + "-被改写"

        snapshot_after = store.flow_versions.get(published["snapshot_id"])
        assert [s["params"] for s in snapshot_after.steps] == frozen_params
        assert [s["name"] for s in snapshot_after.steps] == frozen_names

    def test_locate_plan_source_by_name_and_version(self, bot_with_steps):
        bot, _ = bot_with_steps
        published = BotDefinitionService().publish_bot(bot["id"])

        located = FlowVersionService().locate_plan_source(
            bot["流程名称"], published["version"])
        assert located is not None
        assert located["snapshot_id"] == published["snapshot_id"]
        assert located["version"] == published["version"]
        assert located["step_count"] == 4
        assert "plan_source" in located

        # 不存在的版本返回 None
        assert FlowVersionService().locate_plan_source(bot["流程名称"], "9.9.9") is None


# ---------------------------------------------------------------------------
# 2. 发布与步骤编辑并发：两种冲突策略都只产生一个可执行版本
# ---------------------------------------------------------------------------

class TestEditConflict:

    def test_freeze_before_edit_policy(self, bot_with_steps):
        bot, steps = bot_with_steps
        fv = FlowVersionService()
        fv.begin_edit(bot["id"])
        assert store.flow_versions.is_editing(bot["id"]) is True

        result = BotDefinitionService().publish_bot(
            bot["id"], conflict_policy=FlowVersionService.POLICY_FREEZE_BEFORE_EDIT)

        snapshot = store.flow_versions.get(result["snapshot_id"])
        # 终态：直接冻结编辑前步骤
        assert snapshot.status == "已冻结"
        assert snapshot.conflict is True
        assert snapshot.freeze_policy == "freeze_before_edit"

        # 编辑完成不再产生新版本
        finish = fv.finish_edit(bot["id"])
        assert finish["new_version_created"] is False
        assert finish["pending_version_promoted"] is False
        bot_snaps = [s for s in store.flow_versions.all() if s.bot_id == bot["id"]]
        assert len(bot_snaps) == 1
        assert bot_snaps[0].status == "已冻结"

    def test_publish_after_edit_policy(self, bot_with_steps):
        bot, _ = bot_with_steps
        fv = FlowVersionService()
        fv.begin_edit(bot["id"])

        result = BotDefinitionService().publish_bot(
            bot["id"], conflict_policy=FlowVersionService.POLICY_PUBLISH_AFTER_EDIT)
        snapshot = store.flow_versions.get(result["snapshot_id"])
        # 31号处理终态之一：先待发布
        assert snapshot.status == "待发布"
        assert snapshot.conflict is True
        # 待发布版本尚不可执行
        assert store.flow_versions.latest_executable(bot["id"]) is None

        finish = fv.finish_edit(bot["id"])
        assert finish["pending_version_promoted"] is True
        assert finish["new_version_created"] is False
        # 编辑完成后形成唯一一个可执行版本
        assert snapshot.status == "已冻结"
        executable = store.flow_versions.latest_executable(bot["id"])
        assert executable is not None
        assert executable.snapshot_id == snapshot.snapshot_id
        bot_snaps = [s for s in store.flow_versions.all() if s.bot_id == bot["id"]]
        assert len(bot_snaps) == 1

    def test_normal_publish_without_conflict(self, bot_with_steps):
        bot, _ = bot_with_steps
        result = BotDefinitionService().publish_bot(bot["id"])
        snapshot = store.flow_versions.get(result["snapshot_id"])
        assert snapshot.conflict is False
        assert snapshot.freeze_policy == "normal_publish"


# ---------------------------------------------------------------------------
# 3. 处理项口径：任务队列按快照生成执行计划任务清单
# ---------------------------------------------------------------------------

class TestProcessItem:

    def test_submit_generates_tasks_from_snapshot(self, bot_with_steps):
        bot, steps = bot_with_steps
        BotDefinitionService().publish_bot(bot["id"])
        task_svc = TaskQueueService()

        outcome = task_svc.submit_process_item(bot["id"], source="外部业务入口")
        assert outcome["ok"] is True
        assert outcome["deduplicated"] is False
        assert outcome["task_count"] == len(steps)

        item = store.process_items.get(outcome["item_id"])
        assert item is not None
        assert item.status == "待处理"
        # 每个任务都绑定同一快照与版本口径
        for tid in item.task_ids:
            task = store.find("task_queue", tid)
            assert task["snapshot_id"] == item.snapshot_id
            assert task["version"] == item.version
            assert task["item_id"] == item.item_id

    def test_duplicate_submit_keeps_single_result(self, bot_with_steps):
        bot, _ = bot_with_steps
        BotDefinitionService().publish_bot(bot["id"])
        task_svc = TaskQueueService()

        first = task_svc.submit_process_item(
            bot["id"], idempotency_key="idem-001", source="任务队列")
        second = task_svc.submit_process_item(
            bot["id"], idempotency_key="idem-001", source="任务队列")

        assert second["deduplicated"] is True
        assert second["item_id"] == first["item_id"]
        items = [it for it in store.process_items.all()
                 if it.idempotency_key == "idem-001"]
        assert len(items) == 1

    def test_process_item_uses_pinned_version_after_republish(self, bot_with_steps):
        """沿用既有版本口径：新发布版本不影响已提交处理项的任务依据。"""
        bot, _ = bot_with_steps
        svc = BotDefinitionService()
        first = svc.publish_bot(bot["id"])
        task_svc = TaskQueueService()
        outcome = task_svc.submit_process_item(
            bot["id"], version=first["version"])
        item = store.process_items.get(outcome["item_id"])
        assert item.snapshot_id == first["snapshot_id"]

        # 发布新版本
        bot["status"] = "测试中"
        second = svc.publish_bot(bot["id"])
        # 不指定版本时取最新可执行版本
        outcome_latest = task_svc.submit_process_item(bot["id"], source="调度器")
        assert outcome_latest["snapshot_id"] == second["snapshot_id"]
        # 旧处理项仍锁定旧快照
        item_again = store.process_items.get(outcome["item_id"])
        assert item_again.snapshot_id == first["snapshot_id"]

    def test_complete_updates_tasks_and_ledger(self, bot_with_steps):
        bot, _ = bot_with_steps
        BotDefinitionService().publish_bot(bot["id"])
        task_svc = TaskQueueService()
        outcome = task_svc.submit_process_item(bot["id"])

        complete = task_svc.complete_process_item(
            outcome["item_id"], result={"rows": 100})
        assert complete["ok"] is True
        item = store.process_items.get(outcome["item_id"])
        assert item.status == "已处理"
        assert item.result == {"rows": 100}
        for tid in item.task_ids:
            assert store.find("task_queue", tid)["status"] == "已完成"

        # 重复完成只保留一条结果
        again = task_svc.complete_process_item(outcome["item_id"])
        assert again["deduplicated"] is True

        ledger = task_svc.reconcile_ledger()
        assert ledger["consistent"] is True
        detail = next(d for d in ledger["details"]
                      if d["item_id"] == outcome["item_id"])
        assert detail["has_result"] is True
        assert detail["done_tasks"] == detail["planned_tasks"]

    def test_replay_keeps_same_version(self, bot_with_steps):
        bot, _ = bot_with_steps
        published = BotDefinitionService().publish_bot(bot["id"])
        task_svc = TaskQueueService()
        outcome = task_svc.submit_process_item(bot["id"])
        replay = task_svc.replay_process_item(outcome["item_id"])

        assert replay["ok"] is True
        assert replay["snapshot_id"] == published["snapshot_id"]
        assert len(replay["new_task_ids"]) == published["step_count"]
        item = store.process_items.get(outcome["item_id"])
        # 重放任务仍绑定原快照
        for tid in replay["new_task_ids"]:
            assert store.find("task_queue", tid)["snapshot_id"] == item.snapshot_id

    def test_submit_without_snapshot_returns_error(self, bot_with_steps):
        bot, _ = bot_with_steps
        # 未发布直接提交处理项
        outcome = TaskQueueService().submit_process_item(bot["id"])
        assert outcome["ok"] is False
        assert "快照" in outcome["message"]


# ---------------------------------------------------------------------------
# 4. 查询翻页与定位
# ---------------------------------------------------------------------------

class TestQueryPaging:

    def test_snapshot_paging_and_filters(self, bot_with_steps):
        bot, _ = bot_with_steps
        svc = BotDefinitionService()
        svc.publish_bot(bot["id"])
        bot["status"] = "测试中"
        svc.publish_bot(bot["id"])
        bot["status"] = "测试中"
        svc.publish_bot(bot["id"])

        fv = FlowVersionService()
        page1, total = fv.list_snapshots(bot_name=bot["流程名称"], page=1, size=2)
        page2, _ = fv.list_snapshots(bot_name=bot["流程名称"], page=2, size=2)
        assert total == 3
        assert len(page1) == 2
        assert len(page2) == 1
        ids = {row["snapshot_id"] for row in page1 + page2}
        assert len(ids) == 3

        # 目标系统 + 版本过滤
        rows, filtered_total = fv.list_snapshots(
            target_system=bot["目标系统"], version="2.1.")
        assert filtered_total == 3
        assert all(r["目标系统"] == bot["目标系统"] for r in rows)

    def test_process_item_paging(self, bot_with_steps):
        bot, _ = bot_with_steps
        BotDefinitionService().publish_bot(bot["id"])
        task_svc = TaskQueueService()
        for i in range(3):
            task_svc.submit_process_item(
                bot["id"], idempotency_key=f"page-key-{i}")

        page1, total = task_svc.list_process_items(
            bot_name=bot["流程名称"], page=1, size=2)
        page2, _ = task_svc.list_process_items(
            bot_name=bot["流程名称"], page=2, size=2)
        assert total == 3
        assert len(page1) == 2 and len(page2) == 1


# ---------------------------------------------------------------------------
# 5. HTTP 接口：发布冻结、快照查询、处理项与台账核对
# ---------------------------------------------------------------------------

class TestHttpEndpoints:
    """直接调用路由处理函数，覆盖与 HTTP 接口一致的入参与返回契约。"""

    def test_publish_freeze_and_locate_api(self, bot_with_steps):
        bot, _ = bot_with_steps
        body = versioning.publish_freeze(
            bot["id"], versioning.PublishPayload(
                conflict_policy="freeze_before_edit"))
        assert body["snapshot_status"] == "已冻结"

        payload = versioning.list_flow_versions(
            bot_name=bot["流程名称"], target_system=None,
            version=None, page=1, size=10)
        assert payload.total >= 1
        snapshot_id = payload.items[0]["snapshot_id"]

        detail_body = versioning.get_flow_version(snapshot_id)
        assert len(detail_body["steps"]) == 4
        assert "plan_source" in detail_body

        located = versioning.locate_plan_source(
            bot_name=bot["流程名称"], version=body["version"])
        assert located["snapshot_id"] == body["snapshot_id"]

    def test_process_item_flow_api(self, bot_with_steps):
        bot, _ = bot_with_steps
        versioning.publish_freeze(bot["id"], versioning.PublishPayload())

        item = versioning.submit_process_item(versioning.ProcessItemPayload(
            bot_id=bot["id"], source="外部业务入口", idempotency_key="http-idem-1"))
        assert item["task_count"] == 4

        again = versioning.submit_process_item(versioning.ProcessItemPayload(
            bot_id=bot["id"], idempotency_key="http-idem-1"))
        assert again["deduplicated"] is True

        detail = versioning.get_process_item(item["item_id"])
        assert len(detail["tasks"]) == 4

        complete = versioning.complete_process_item(
            item["item_id"], versioning.CompletePayload(result={"rows": 8}))
        assert complete["status"] == "已处理"

        reconcile = versioning.reconcile_ledger()
        assert reconcile["consistent"] is True

        paged = versioning.list_process_items(
            bot_name=bot["流程名称"], version=None, status=None,
            source=None, page=1, size=1)
        assert paged.size == 1
        assert paged.total >= 1

    def test_conflict_publish_after_edit_api(self, bot_with_steps):
        bot, _ = bot_with_steps
        versioning.begin_editing(bot["id"])
        resp = versioning.publish_freeze(
            bot["id"],
            versioning.PublishPayload(conflict_policy="publish_after_edit"))
        assert resp["snapshot_status"] == "待发布"
        finish = versioning.finish_editing(bot["id"])
        assert finish["pending_version_promoted"] is True

    def test_locate_missing_version_raises_404(self, bot_with_steps):
        from fastapi import HTTPException

        bot, _ = bot_with_steps
        with pytest.raises(HTTPException) as exc:
            versioning.locate_plan_source(
                bot_name=bot["流程名称"], version="9.9.9")
        assert exc.value.status_code == 404
