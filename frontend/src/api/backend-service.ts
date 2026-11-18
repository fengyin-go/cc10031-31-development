import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

// ---- 流程版本快照 ----

export type FlowVersionRow = {
  snapshot_id: number
  bot_id: number
  流程名称: string
  目标系统: string
  版本号: string
  version_seq: number
  步骤数: number
  执行计划来源: string
  冻结策略: string
  并发冲突: boolean
  status: string
  created_at: string
  published_at: string | null
  [field: string]: unknown
}

export type FlowVersionDetail = FlowVersionRow & {
  steps: Array<{
    step_id: number
    seq: number
    name: string
    type: string
    element: string
    params: string
    wait: string
    on_error: string
  }>
  tasks: Array<Record<string, unknown>>
}

async function readJson<T>(response: Response, fallback: string): Promise<T> {
  if (!response.ok) {
    let message = fallback
    try {
      const body = await response.json()
      if (body?.detail) message = String(body.detail)
    } catch {
      /* 沿用默认错误提示 */
    }
    throw new Error(message)
  }
  return (await response.json()) as T
}

/** 按流程名称、目标系统、版本分页查询流程版本快照 */
export async function listFlowVersions(
  filters: { bot_name?: string; target_system?: string; version?: string },
  page = 1,
  size = 10,
): Promise<{ items: FlowVersionRow[]; total: number; page: number; size: number }> {
  const query = new URLSearchParams({ page: String(page), size: String(size) })
  Object.entries(filters).forEach(([key, value]) => {
    if (value) query.set(key, value)
  })
  const response = await request(`/api/flow-versions?${query.toString()}`)
  return readJson(response, '流程版本快照查询失败')
}

/** 快照详情：冻结步骤集合、执行计划来源与当前版本的流程任务 */
export async function loadFlowVersionDetail(snapshotId: number): Promise<FlowVersionDetail> {
  const response = await request(`/api/flow-versions/${snapshotId}`)
  return readJson(response, '快照详情读取失败')
}

/** 按流程名称和版本定位执行计划数据来源 */
export async function locatePlanSource(
  botName: string,
  version: string,
): Promise<Record<string, unknown>> {
  const query = new URLSearchParams({ bot_name: botName, version })
  const response = await request(`/api/flow-versions/locate/plan-source?${query.toString()}`)
  return readJson(response, '执行计划来源定位失败')
}

/** 发布流程并原子冻结步骤集合 */
export async function publishFreeze(
  botId: number,
  conflictPolicy: 'freeze_before_edit' | 'publish_after_edit',
): Promise<Record<string, unknown>> {
  const response = await request(`/api/bot-definition/${botId}/publish-freeze`, {
    method: 'POST',
    body: JSON.stringify({ conflict_policy: conflictPolicy }),
  })
  return readJson(response, '发布冻结失败')
}

/** 登记步骤编辑会话（模拟另一个业务入口开始编辑） */
export async function beginEditing(botId: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/bot-definition/${botId}/editing`, { method: 'POST' })
  return readJson(response, '编辑会话登记失败')
}

/** 步骤编辑完成：待发布版本转已冻结 */
export async function finishEditing(botId: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/bot-definition/${botId}/editing/finish`, {
    method: 'POST',
  })
  return readJson(response, '编辑完成处理失败')
}

// ---- 处理项（任务队列口径） ----

export type ProcessItemRow = {
  item_id: number
  item_no: string
  bot_id: number
  流程名称: string
  目标系统: string
  snapshot_id: number
  版本号: string
  source: string
  status: string
  created_at: string
  task_count: number
  [field: string]: unknown
}

/** 提交处理项：加载关联快照，按快照步骤集合生成执行计划任务清单 */
export async function submitProcessItem(payload: {
  bot_id: number
  version?: string
  snapshot_id?: number
  idempotency_key?: string
  source?: string
}): Promise<Record<string, unknown>> {
  const response = await request('/api/process-items', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  return readJson(response, '处理项提交失败')
}

/** 处理项查询翻页定位 */
export async function listProcessItems(
  filters: { bot_name?: string; version?: string; status?: string; source?: string },
  page = 1,
  size = 10,
): Promise<{ items: ProcessItemRow[]; total: number; page: number; size: number }> {
  const query = new URLSearchParams({ page: String(page), size: String(size) })
  Object.entries(filters).forEach(([key, value]) => {
    if (value) query.set(key, value)
  })
  const response = await request(`/api/process-items?${query.toString()}`)
  return readJson(response, '处理项查询失败')
}

/** 处理项详情：任务清单与结果 */
export async function loadProcessItem(itemId: number): Promise<Record<string, unknown>> {
  const response = await request(`/api/process-items/${itemId}`)
  return readJson(response, '处理项详情读取失败')
}

/** 处理完成：联动更新待办，重复完成只保留一条结果 */
export async function completeProcessItem(
  itemId: number,
  result?: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const response = await request(`/api/process-items/${itemId}/complete`, {
    method: 'POST',
    body: JSON.stringify({ result: result ?? null }),
  })
  return readJson(response, '处理项完成失败')
}

/** 处理台账与结果清单联动核对 */
export async function reconcileLedger(): Promise<Record<string, unknown>> {
  const response = await request('/api/process-items/ledger/reconcile')
  return readJson(response, '台账核对失败')
}
