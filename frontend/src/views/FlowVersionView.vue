<template>
  <section class="page" data-module="flow_version">
    <header class="page-head">
      <div>
        <h2>流程版本快照</h2>
        <p class="page-desc">
          发布时原子冻结步骤集合与参数，按流程名称、目标系统和版本核对执行计划来源；
          任务队列按处理项口径加载快照生成执行计划任务清单。
        </p>
      </div>
    </header>

    <div class="tab-row">
      <button
        class="btn"
        :class="{ primary: tab === 'snapshots' }"
        type="button"
        data-testid="tab-snapshots"
        @click="switchTab('snapshots')"
      >
        版本快照
      </button>
      <button
        class="btn"
        :class="{ primary: tab === 'items' }"
        type="button"
        data-testid="tab-items"
        @click="switchTab('items')"
      >
        处理项与执行计划
      </button>
    </div>

    <!-- ============ 版本快照 ============ -->
    <template v-if="tab === 'snapshots'">
      <form class="filter-bar" @submit.prevent="reloadSnapshots(1)">
        <label class="filter-item">
          <span>流程名称</span>
          <input v-model="snapshotFilters.bot_name" data-testid="filter-bot-name" placeholder="按流程名称检索" />
        </label>
        <label class="filter-item">
          <span>目标系统</span>
          <input v-model="snapshotFilters.target_system" data-testid="filter-target" placeholder="按目标系统检索" />
        </label>
        <label class="filter-item">
          <span>版本</span>
          <input v-model="snapshotFilters.version" data-testid="filter-version" placeholder="如 1.0.0" />
        </label>
        <button class="btn" data-testid="query-snapshot" type="submit">查询</button>
        <button class="btn ghost" type="button" @click="resetSnapshotFilters">重置条件</button>
      </form>

      <form class="filter-bar" @submit.prevent="locateSource">
        <label class="filter-item">
          <span>来源定位-流程名称</span>
          <input v-model="locateName" data-testid="locate-name" placeholder="精确流程名称" />
        </label>
        <label class="filter-item">
          <span>来源定位-版本</span>
          <input v-model="locateVersion" data-testid="locate-version" placeholder="精确版本号" />
        </label>
        <button class="btn" data-testid="locate-btn" type="submit">按名称+版本定位执行计划来源</button>
      </form>

      <div v-if="located" class="stat-card" data-testid="located-box">
        <span class="stat-label">执行计划来源（{{ located.bot_name }} @ {{ located.version }}）</span>
        <strong class="stat-value">快照 #{{ located.snapshot_id }} · {{ located.status }}</strong>
        <p class="page-desc">{{ located.plan_source }}</p>
        <p class="page-desc">步骤数 {{ located.step_count }} · 冻结策略 {{ located.freeze_policy }} · 关联任务 {{ located.tasks.length }} 条</p>
      </div>

      <table class="data-table">
        <thead>
          <tr>
            <th>快照ID</th>
            <th>流程名称</th>
            <th>目标系统</th>
            <th>版本号</th>
            <th>步骤数</th>
            <th>冻结策略</th>
            <th>并发冲突</th>
            <th>终态</th>
            <th>发布时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in snapshotRows" :key="row.snapshot_id" :data-testid="`snapshot-row-${row.snapshot_id}`">
            <td>{{ row.snapshot_id }}</td>
            <td>{{ row['流程名称'] }}</td>
            <td>{{ row['目标系统'] }}</td>
            <td>{{ row['版本号'] }}</td>
            <td>{{ row['步骤数'] }}</td>
            <td>{{ policyText(row['冻结策略']) }}</td>
            <td>{{ row['并发冲突'] ? '是' : '否' }}</td>
            <td>{{ row.status }}</td>
            <td>{{ row.published_at ?? '—' }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="openDetail(row.snapshot_id)">步骤集合/任务</button>
            </td>
          </tr>
          <tr v-if="!snapshotRows.length">
            <td colspan="10" class="empty-state">暂无流程版本快照，发布流程后自动冻结生成</td>
          </tr>
        </tbody>
      </table>

      <footer class="page-foot">
        <span>共 {{ snapshotTotal }} 条快照</span>
        <button class="btn ghost" type="button" :disabled="snapshotPage <= 1" @click="reloadSnapshots(snapshotPage - 1)">上一页</button>
        <span>第 {{ snapshotPage }} 页 / 每页 {{ snapshotSize }} 条</span>
        <button
          class="btn ghost"
          type="button"
          :disabled="snapshotPage * snapshotSize >= snapshotTotal"
          @click="reloadSnapshots(snapshotPage + 1)"
        >
          下一页
        </button>
      </footer>
    </template>

    <!-- ============ 处理项与执行计划 ============ -->
    <template v-if="tab === 'items'">
      <form class="filter-bar" @submit.prevent="submitItem">
        <label class="filter-item">
          <span>流程ID</span>
          <input v-model.number="submitBotId" data-testid="submit-bot-id" placeholder="bot_definition 主键" />
        </label>
        <label class="filter-item">
          <span>指定版本(可选)</span>
          <input v-model="submitVersion" data-testid="submit-version" placeholder="留空取最新已冻结版本" />
        </label>
        <label class="filter-item">
          <span>幂等键(可选)</span>
          <input v-model="submitIdem" data-testid="submit-idem" placeholder="重复提交只保留一条结果" />
        </label>
        <label class="filter-item">
          <span>业务入口</span>
          <input v-model="submitSource" data-testid="submit-source" placeholder="任务队列 / 外部业务入口" />
        </label>
        <button class="btn primary" data-testid="submit-item" type="submit">提交处理项生成任务清单</button>
        <button class="btn" type="button" @click="runReconcile">台账核对</button>
      </form>

      <div v-if="reconcile" class="stat-card" data-testid="reconcile-box">
        <span class="stat-label">处理台账与结果清单联动核对</span>
        <strong class="stat-value">{{ reconcile.consistent ? '一致' : '存在不一致' }}</strong>
        <p class="page-desc">处理项 {{ reconcile.total_items }} · 不一致 {{ reconcile.mismatches }}</p>
      </div>

      <form class="filter-bar" @submit.prevent="reloadItems(1)">
        <label class="filter-item">
          <span>流程名称</span>
          <input v-model="itemFilters.bot_name" data-testid="item-filter-name" placeholder="按流程名称检索" />
        </label>
        <label class="filter-item">
          <span>版本</span>
          <input v-model="itemFilters.version" data-testid="item-filter-version" placeholder="按版本检索" />
        </label>
        <label class="filter-item">
          <span>状态</span>
          <input v-model="itemFilters.status" data-testid="item-filter-status" placeholder="待处理 / 已处理" />
        </label>
        <button class="btn" type="submit">查询</button>
        <button class="btn ghost" type="button" @click="resetItemFilters">重置条件</button>
      </form>

      <table class="data-table">
        <thead>
          <tr>
            <th>处理项编号</th>
            <th>流程名称</th>
            <th>版本</th>
            <th>快照ID</th>
            <th>业务入口</th>
            <th>任务数</th>
            <th>状态</th>
            <th>提交时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in itemRows" :key="row.item_id" :data-testid="`item-row-${row.item_id}`">
            <td>{{ row.item_no }}</td>
            <td>{{ row['流程名称'] }}</td>
            <td>{{ row['版本号'] }}</td>
            <td>{{ row.snapshot_id }}</td>
            <td>{{ row.source }}</td>
            <td>{{ row.task_count }}</td>
            <td>{{ row.status }}</td>
            <td>{{ row.created_at }}</td>
            <td class="row-actions">
              <button class="link" type="button" @click="openItemDetail(row.item_id)">任务清单</button>
              <button
                v-if="row.status === '待处理'"
                class="link"
                type="button"
                @click="finishItem(row.item_id)"
              >
                处理完成
              </button>
            </td>
          </tr>
          <tr v-if="!itemRows.length">
            <td colspan="9" class="empty-state">暂无处理项，提交后按快照步骤集合生成执行计划任务清单</td>
          </tr>
        </tbody>
      </table>

      <footer class="page-foot">
        <span>共 {{ itemTotal }} 条处理项</span>
        <button class="btn ghost" type="button" :disabled="itemPage <= 1" @click="reloadItems(itemPage - 1)">上一页</button>
        <span>第 {{ itemPage }} 页 / 每页 {{ itemSize }} 条</span>
        <button
          class="btn ghost"
          type="button"
          :disabled="itemPage * itemSize >= itemTotal"
          @click="reloadItems(itemPage + 1)"
        >
          下一页
        </button>
      </footer>
    </template>

    <!-- 快照详情：冻结步骤集合 + 当前版本流程任务 -->
    <div v-if="detail" class="modal-backdrop" data-testid="detail-modal" @click.self="detail = null">
      <div class="modal modal-wide">
        <h3>快照 #{{ detail.snapshot_id }} · {{ detail['版本号'] }}（{{ detail.status }}）</h3>
        <p class="page-desc">执行计划来源：{{ detail['执行计划来源'] }}</p>
        <table class="data-table">
          <thead>
            <tr><th>序号</th><th>步骤名称</th><th>操作类型</th><th>目标元素</th><th>输入参数</th><th>等待策略</th><th>异常处理</th></tr>
          </thead>
          <tbody>
            <tr v-for="step in detail.steps" :key="`${step.step_id}-${step.seq}`">
              <td>{{ step.seq }}</td>
              <td>{{ step.name }}</td>
              <td>{{ step.type }}</td>
              <td>{{ step.element }}</td>
              <td>{{ step.params }}</td>
              <td>{{ step.wait }}</td>
              <td>{{ step.on_error }}</td>
            </tr>
          </tbody>
        </table>
        <h3>当前版本的流程任务（{{ detail.tasks.length }}）</h3>
        <table class="data-table">
          <thead>
            <tr><th>任务编号</th><th>步骤</th><th>状态</th></tr>
          </thead>
          <tbody>
            <tr v-for="task in detail.tasks" :key="String(task.task_id)">
              <td>{{ String(task['任务编号'] ?? '') }}</td>
              <td>{{ String(task['步骤名称'] ?? '') }}</td>
              <td>{{ String(task['当前状态'] ?? '') }}</td>
            </tr>
            <tr v-if="!detail.tasks.length">
              <td colspan="3" class="empty-state">该版本尚未生成流程任务</td>
            </tr>
          </tbody>
        </table>
        <div class="page-actions">
          <button class="btn primary" type="button" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>

    <!-- 处理项详情：执行计划任务清单 -->
    <div v-if="itemDetail" class="modal-backdrop" data-testid="item-modal" @click.self="itemDetail = null">
      <div class="modal modal-wide">
        <h3>处理项 {{ itemDetail.item_no }} · {{ itemDetail['版本号'] }}</h3>
        <p class="page-desc">
          快照 #{{ itemDetail.snapshot_id }} · 业务入口 {{ itemDetail.source }} · 状态 {{ itemDetail.status }}
        </p>
        <table class="data-table">
          <thead>
            <tr><th>任务编号</th><th>流程名称</th><th>步骤</th><th>优先级</th><th>状态</th></tr>
          </thead>
          <tbody>
            <tr v-for="task in itemDetail.tasks" :key="String(task.id)">
              <td>{{ String(task['任务编号'] ?? '') }}</td>
              <td>{{ String(task['流程名称'] ?? '') }}</td>
              <td>{{ String(task['预计执行'] ?? '') }}</td>
              <td>{{ String(task['优先级'] ?? '') }}</td>
              <td>{{ String(task.status ?? '') }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="itemDetail.result" class="page-desc">结果清单：{{ JSON.stringify(itemDetail.result) }}</p>
        <div class="page-actions">
          <button class="btn primary" type="button" @click="itemDetail = null">关闭</button>
        </div>
      </div>
    </div>

    <footer class="page-foot">
      <span v-if="message" class="error-text">{{ message }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  completeProcessItem,
  listFlowVersions,
  listProcessItems,
  loadFlowVersionDetail,
  loadProcessItem,
  locatePlanSource,
  reconcileLedger,
  submitProcessItem,
  type FlowVersionDetail,
  type FlowVersionRow,
  type ProcessItemRow,
} from '@/api/backend-service'

type LocatedSource = {
  snapshot_id: number
  bot_name: string
  target_system: string
  version: string
  status: string
  plan_source: string
  step_count: number
  freeze_policy: string
  conflict: boolean
  published_at: string | null
  tasks: Array<Record<string, unknown>>
}

type Reconcile = {
  consistent: boolean
  total_items: number
  mismatches: number
  details: unknown[]
}

const tab = ref<'snapshots' | 'items'>('snapshots')
const message = ref('')

// ---- 版本快照 ----
const snapshotRows = ref<FlowVersionRow[]>([])
const snapshotTotal = ref(0)
const snapshotPage = ref(1)
const snapshotSize = 10
const snapshotFilters = ref({ bot_name: '', target_system: '', version: '' })
const locateName = ref('')
const locateVersion = ref('')
const located = ref<LocatedSource | null>(null)
const detail = ref<FlowVersionDetail | null>(null)

// ---- 处理项 ----
const itemRows = ref<ProcessItemRow[]>([])
const itemTotal = ref(0)
const itemPage = ref(1)
const itemSize = 10
const itemFilters = ref({ bot_name: '', version: '', status: '' })
const submitBotId = ref<number | null>(null)
const submitVersion = ref('')
const submitIdem = ref('')
const submitSource = ref('任务队列')
const reconcile = ref<Reconcile | null>(null)
const itemDetail = ref<{
  item_no: string
  snapshot_id: number
  source: string
  status: string
  版本号: string
  result: Record<string, unknown> | null
  tasks: Array<Record<string, unknown>>
} | null>(null)

function policyText(policy: unknown): string {
  const map: Record<string, string> = {
    normal_publish: '常规发布',
    freeze_before_edit: '冻结编辑前步骤',
    publish_after_edit: '编辑完成后发布',
  }
  return map[String(policy)] ?? String(policy)
}

async function reloadSnapshots(page = 1) {
  message.value = ''
  snapshotPage.value = page
  try {
    const data = await listFlowVersions(snapshotFilters.value, page, snapshotSize)
    snapshotRows.value = data.items
    snapshotTotal.value = data.total
  } catch (error) {
    message.value = error instanceof Error ? error.message : '快照查询失败'
  }
}

function resetSnapshotFilters() {
  snapshotFilters.value = { bot_name: '', target_system: '', version: '' }
  located.value = null
  void reloadSnapshots(1)
}

async function locateSource() {
  message.value = ''
  if (!locateName.value || !locateVersion.value) {
    message.value = '请同时填写流程名称与版本号'
    return
  }
  try {
    located.value = (await locatePlanSource(locateName.value, locateVersion.value)) as LocatedSource
  } catch (error) {
    located.value = null
    message.value = error instanceof Error ? error.message : '定位失败'
  }
}

async function openDetail(snapshotId: number) {
  message.value = ''
  try {
    detail.value = await loadFlowVersionDetail(snapshotId)
  } catch (error) {
    message.value = error instanceof Error ? error.message : '详情读取失败'
  }
}

async function submitItem() {
  message.value = ''
  if (!submitBotId.value) {
    message.value = '请填写流程ID'
    return
  }
  try {
    const result = await submitProcessItem({
      bot_id: submitBotId.value,
      version: submitVersion.value || undefined,
      idempotency_key: submitIdem.value || undefined,
      source: submitSource.value || '任务队列',
    })
    message.value = result.deduplicated
      ? `幂等命中：沿用既有处理项 #${result.item_id}`
      : `已生成 ${result.task_count} 条执行计划任务（处理项 #${result.item_id}，版本 ${result.version}）`
    await reloadItems(1)
  } catch (error) {
    message.value = error instanceof Error ? error.message : '处理项提交失败'
  }
}

async function reloadItems(page = 1) {
  message.value = ''
  itemPage.value = page
  try {
    const data = await listProcessItems(itemFilters.value, page, itemSize)
    itemRows.value = data.items
    itemTotal.value = data.total
  } catch (error) {
    message.value = error instanceof Error ? error.message : '处理项查询失败'
  }
}

function resetItemFilters() {
  itemFilters.value = { bot_name: '', version: '', status: '' }
  void reloadItems(1)
}

async function openItemDetail(itemId: number) {
  message.value = ''
  try {
    itemDetail.value = (await loadProcessItem(itemId)) as typeof itemDetail.value
  } catch (error) {
    message.value = error instanceof Error ? error.message : '处理项详情读取失败'
  }
}

async function finishItem(itemId: number) {
  message.value = ''
  try {
    const result = await completeProcessItem(itemId)
    message.value = result.deduplicated ? '该处理项已完成，重复提交只保留一条结果' : '处理完成，台账与任务状态已联动更新'
    await reloadItems(itemPage.value)
  } catch (error) {
    message.value = error instanceof Error ? error.message : '处理失败'
  }
}

async function runReconcile() {
  message.value = ''
  try {
    reconcile.value = (await reconcileLedger()) as Reconcile
  } catch (error) {
    message.value = error instanceof Error ? error.message : '台账核对失败'
  }
}

function switchTab(target: 'snapshots' | 'items') {
  tab.value = target
  message.value = ''
}

onMounted(() => {
  void reloadSnapshots(1)
  void reloadItems(1)
})
</script>
