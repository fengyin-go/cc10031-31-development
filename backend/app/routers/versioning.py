"""流程版本快照与处理项路由。

查询口径：
- 流程版本快照可按流程名称、目标系统、版本过滤并翻页；
- 处理项可按流程名称、版本、状态、业务入口过滤并翻页；
- 按流程名称 + 版本可定位执行计划数据来源与当前版本的流程任务。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.schemas import PageResult
from app.services.flow_version import FlowVersionService, flow_version_service
from app.services.task_queue import TaskQueueService

router = APIRouter(prefix="/api", tags=["流程版本快照"])

task_service = TaskQueueService()


class PublishPayload(BaseModel):
    conflict_policy: str = FlowVersionService.POLICY_FREEZE_BEFORE_EDIT


class ProcessItemPayload(BaseModel):
    bot_id: int
    version: str | None = None
    snapshot_id: int | None = None
    idempotency_key: str | None = None
    source: str = "任务队列"


class CompletePayload(BaseModel):
    result: dict[str, Any] | None = None


# ---- 流程版本快照 ----

@router.get("/flow-versions", response_model=PageResult[dict])
def list_flow_versions(
    bot_name: str | None = Query(default=None),
    target_system: str | None = Query(default=None),
    version: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按流程名称、目标系统和版本查看快照，支持翻页定位。"""
    items, total = flow_version_service.list_snapshots(
        bot_name=bot_name, target_system=target_system,
        version=version, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/flow-versions/{snapshot_id}")
def get_flow_version(snapshot_id: int) -> dict[str, Any]:
    """快照详情：冻结的步骤集合、执行计划来源和当前版本的流程任务。"""
    detail = flow_version_service.get_snapshot(snapshot_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"流程版本快照 {snapshot_id} 不存在")
    return detail


@router.get("/flow-versions/locate/plan-source")
def locate_plan_source(
    bot_name: str = Query(...),
    version: str = Query(...),
) -> dict[str, Any]:
    """按流程名称和版本定位执行计划数据来源。"""
    result = flow_version_service.locate_plan_source(bot_name, version)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"未找到流程「{bot_name}」版本「{version}」的快照",
        )
    return result


# ---- 发布冻结与编辑冲突入口 ----

@router.post("/bot-definition/{bot_id}/publish-freeze")
def publish_freeze(bot_id: int, payload: PublishPayload) -> dict[str, Any]:
    """发布流程并原子冻结步骤集合（支持两种并发冲突策略）。"""
    from app.services.bot_definition import BotDefinitionService

    result = BotDefinitionService().publish_bot(
        bot_id, conflict_policy=payload.conflict_policy)
    if result is None:
        raise HTTPException(status_code=409, detail="流程当前状态不允许发布或流程不存在")
    return result


@router.post("/bot-definition/{bot_id}/editing")
def begin_editing(bot_id: int) -> dict[str, Any]:
    """登记步骤编辑会话（模拟另一个业务入口开始编辑步骤）。"""
    return flow_version_service.begin_edit(bot_id)


@router.post("/bot-definition/{bot_id}/editing/finish")
def finish_editing(bot_id: int) -> dict[str, Any]:
    """步骤编辑完成：待发布版本转已冻结，已冻结版本不再产生新版本。"""
    return flow_version_service.finish_edit(bot_id)


# ---- 处理项（任务队列口径） ----

@router.post("/process-items")
def submit_process_item(payload: ProcessItemPayload) -> dict[str, Any]:
    """提交处理项：加载关联快照，按快照步骤集合生成执行计划任务清单。"""
    return task_service.submit_process_item(
        payload.bot_id,
        version=payload.version,
        snapshot_id=payload.snapshot_id,
        idempotency_key=payload.idempotency_key,
        source=payload.source,
    )


@router.get("/process-items", response_model=PageResult[dict])
def list_process_items(
    bot_name: str | None = Query(default=None),
    version: str | None = Query(default=None),
    status: str | None = Query(default=None),
    source: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """处理项查询翻页定位。"""
    items, total = task_service.list_process_items(
        bot_name=bot_name, version=version, status=status,
        source=source, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/process-items/ledger/reconcile")
def reconcile_ledger() -> dict[str, Any]:
    """处理台账与结果清单联动核对。"""
    return task_service.reconcile_ledger()


@router.get("/process-items/{item_id}")
def get_process_item(item_id: int) -> dict[str, Any]:
    detail = task_service.get_process_item(item_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"处理项 {item_id} 不存在")
    return detail


@router.post("/process-items/{item_id}/complete")
def complete_process_item(item_id: int, payload: CompletePayload) -> dict[str, Any]:
    """处理完成：联动更新待办，台账与结果清单核对；重复完成只保留一条结果。"""
    result = task_service.complete_process_item(item_id, result=payload.result)
    if result is None:
        raise HTTPException(status_code=404, detail=f"处理项 {item_id} 不存在")
    return result


@router.post("/process-items/{item_id}/replay")
def replay_process_item(item_id: int) -> dict[str, Any]:
    """沿用既有版本口径重放处理项。"""
    result = task_service.replay_process_item(item_id)
    if result is None:
        raise HTTPException(status_code=404, detail=f"处理项 {item_id} 不存在")
    return result
