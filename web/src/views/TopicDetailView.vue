<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import { toHourlyConsumption, type Point } from '@/lib/consumption'
import TopicEditModal from '@/components/TopicEditModal.vue'
import TopicDeleteModal from '@/components/TopicDeleteModal.vue'
import TrendChart from '@/components/TrendChart.vue'

interface PartitionInfo { partition: number; leader: number; replicas: number[]; isr: number[] }
interface TopicDetail { name: string; partitions: PartitionInfo[]; configs: Record<string, string> }
interface PartitionThroughput { partition: number; endOffset: number; count: number; ratePerMin: number }
interface SamplePoint { sampledAt: string; value: number }
interface MessageRecord {
  partition: number
  offset: number
  timestamp: string
  key: string | null
  value: string | null
}

const route = useRoute()
const router = useRouter()
const { isAdmin } = useSession()
const detail = ref<TopicDetail | null>(null)
const error = ref('')
const messages = ref<MessageRecord[]>([])
const messagesError = ref('')
const messagesLoading = ref(false)
const showEdit = ref(false)
const showDelete = ref(false)
const throughput = ref<Map<number, PartitionThroughput>>(new Map())
const producedTrend = ref<Point[]>([])
const throughputLoaded = ref(false)

// 유입 지표는 부가 정보 — 실패해도 파티션 표 자체는 그대로 보여준다
async function loadThroughput() {
  try {
    const list = await api<PartitionThroughput[]>(`/topics/${route.params.name}/throughput`)
    throughput.value = new Map(list.map((t) => [t.partition, t]))
    const samples = await api<SamplePoint[]>(
      `/metrics?type=PRODUCED_TOPIC&subject=${encodeURIComponent(String(route.params.name))}&hours=24`,
    )
    producedTrend.value = toHourlyConsumption(samples.map((s) => ({ t: s.sampledAt, v: s.value })))
  } catch {
    throughput.value = new Map()
    producedTrend.value = []
  } finally {
    throughputLoaded.value = true
  }
}

const hasThroughput = computed(() => throughput.value.size > 0)
// 수집 샘플이 아직 없을 때만 예시(데모) 데이터를 보여준다 — 로드 중 깜빡임 방지로 loaded 이후에만
const isDemo = computed(() => throughputLoaded.value && !hasThroughput.value)

// 미리보기용 예시 값 — 고정 수식이라 렌더링이 결정적이다 (랜덤 없음)
function demoThroughput(partition: number): PartitionThroughput {
  const count = 90 + ((partition * 37) % 60)
  return { partition, endOffset: 1_000 * (partition + 1) + count, count, ratePerMin: count / 60 }
}

function rowThroughput(partition: number): PartitionThroughput | null {
  if (isDemo.value) return demoThroughput(partition)
  return throughput.value.get(partition) ?? null
}

// 추이 차트 대상: 전체(토픽 합) 또는 특정 파티션
const selectedPartition = ref('all')

// 파티션 선택에 따라 다른 예시 곡선을 보여준다 ('all'이면 토픽 합 느낌의 큰 값)
const demoTrend = computed<Point[]>(() => {
  const hour = 3_600_000
  const base = Math.floor(Date.now() / hour) * hour
  const seed = selectedPartition.value === 'all' ? 0 : Number(selectedPartition.value) + 1
  return Array.from({ length: 24 }, (_, i) => ({
    t: new Date(base - (23 - i) * hour).toISOString(),
    v: seed === 0 ? 60 + ((i * 7919) % 90) : 15 + ((i * 7919 + seed * 131) % 45),
  }))
})
// 파티션별 조회 결과는 캐시한다
const partitionTrends = ref<Map<number, Point[]>>(new Map())

watch(selectedPartition, async (sel) => {
  if (sel === 'all' || isDemo.value) return
  const p = Number(sel)
  if (partitionTrends.value.has(p)) return
  try {
    const samples = await api<SamplePoint[]>(
      `/metrics?type=PRODUCED_PARTITION&subject=${encodeURIComponent(`${route.params.name}|${p}`)}&hours=24`,
    )
    partitionTrends.value.set(
      p,
      toHourlyConsumption(samples.map((s) => ({ t: s.sampledAt, v: s.value }))),
    )
  } catch {
    partitionTrends.value.set(p, [])
  }
})

const chartPoints = computed<Point[]>(() => {
  if (isDemo.value) return demoTrend.value
  if (selectedPartition.value === 'all') return producedTrend.value
  return partitionTrends.value.get(Number(selectedPartition.value)) ?? []
})

async function loadMessages() {
  messagesLoading.value = true
  messagesError.value = ''
  try {
    messages.value = await api<MessageRecord[]>(`/topics/${route.params.name}/messages?limit=50`)
  } catch (e) {
    messagesError.value = e instanceof Error ? e.message : '조회 실패'
  } finally {
    messagesLoading.value = false
  }
}

onMounted(async () => {
  try {
    detail.value = await api<TopicDetail>(`/topics/${route.params.name}`)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
  await Promise.all([loadMessages(), loadThroughput()])
})

async function reload() {
  showEdit.value = false
  detail.value = await api<TopicDetail>(`/topics/${route.params.name}`)
}

function onDeleted() {
  router.push('/topics')
}
</script>

<template>
  <main>
    <div class="head-row">
      <h1>토픽: {{ route.params.name }}</h1>
      <div v-if="isAdmin && detail" class="actions">
        <button type="button" class="btn" @click="showEdit = true">설정 수정</button>
        <button type="button" class="btn danger-outline" @click="showDelete = true">삭제</button>
      </div>
    </div>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-else-if="detail">
      <h2>설정</h2>
      <ul>
        <li v-for="(v, k) in detail.configs" :key="k">{{ k }} = {{ v }}</li>
      </ul>
      <h2>파티션</h2>
      <table>
        <thead>
          <tr>
            <th>파티션</th><th>리더</th><th>복제본</th><th>ISR</th><th>상태</th>
            <th>endOffset<span v-if="isDemo" class="demo-badge">예시</span></th>
            <th>최근 1시간 유입<span v-if="isDemo" class="demo-badge">예시</span></th>
            <th>분당 속도<span v-if="isDemo" class="demo-badge">예시</span></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="p in detail.partitions" :key="p.partition">
            <td>{{ p.partition }}</td>
            <td>{{ p.leader }}</td>
            <td>{{ p.replicas.join(', ') }}</td>
            <td>{{ p.isr.join(', ') }}</td>
            <td>
              <span v-if="p.isr.length < p.replicas.length" class="warn">복제 부족</span>
              <span v-else>정상</span>
            </td>
            <td class="num" :class="{ demo: isDemo }">{{ rowThroughput(p.partition)?.endOffset ?? '—' }}</td>
            <td class="num" :class="{ demo: isDemo }">{{ rowThroughput(p.partition)?.count ?? '—' }}</td>
            <td class="num" :class="{ demo: isDemo }">
              {{ rowThroughput(p.partition)?.ratePerMin.toFixed(1) ?? '—' }}
            </td>
          </tr>
        </tbody>
      </table>
      <p v-if="isDemo" class="hint">
        아직 수집된 샘플이 없어 예시 데이터를 표시 중입니다. 수집이 시작되면 실제 값으로 바뀝니다.
      </p>
      <template v-if="isDemo || producedTrend.length > 0">
        <div class="trend-head">
          <h2>유입 추이 (시간대별)<span v-if="isDemo" class="demo-badge">예시</span></h2>
          <select
            v-model="selectedPartition"
            class="partition-select"
            aria-label="추이 대상 파티션"
          >
            <option value="all">전체</option>
            <option v-for="p in detail.partitions" :key="p.partition" :value="String(p.partition)">
              파티션 {{ p.partition }}
            </option>
          </select>
        </div>
        <div v-if="chartPoints.length > 0" :class="{ demo: isDemo }">
          <TrendChart :points="chartPoints" label="토픽 유입 추이 차트" />
        </div>
        <p v-else class="hint">선택한 파티션의 추이를 그리기엔 샘플이 아직 부족합니다.</p>
      </template>
      <div class="messages-head">
        <h2>최근 메시지</h2>
        <button type="button" class="refresh-btn" :disabled="messagesLoading" @click="loadMessages">
          {{ messagesLoading ? '조회 중…' : '새로고침' }}
        </button>
      </div>
      <p v-if="messagesError" class="error">{{ messagesError }}</p>
      <p v-else-if="messages.length === 0" class="hint">메시지가 없습니다.</p>
      <table v-else class="messages-table">
        <thead>
          <tr><th>시각</th><th>파티션</th><th>오프셋</th><th>키</th><th>값</th></tr>
        </thead>
        <tbody>
          <tr v-for="m in messages" :key="`${m.partition}-${m.offset}`">
            <td class="nowrap">{{ new Date(m.timestamp).toLocaleString() }}</td>
            <td>{{ m.partition }}</td>
            <td>{{ m.offset }}</td>
            <td class="mono">{{ m.key ?? '—' }}</td>
            <td class="mono value-cell">{{ m.value ?? '—' }}</td>
          </tr>
        </tbody>
      </table>
      <p class="hint">최신순 최대 50건, 값은 1,000자까지 표시됩니다.</p>
    </template>
    <TopicEditModal
      v-if="showEdit && detail"
      :name="detail.name"
      :current-partitions="detail.partitions.length"
      :configs="detail.configs"
      @close="showEdit = false"
      @updated="reload"
    />
    <TopicDeleteModal
      v-if="showDelete && detail"
      :name="detail.name"
      @close="showDelete = false"
      @deleted="onDeleted"
    />
  </main>
</template>

<style scoped>
.head-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.actions {
  display: flex;
  gap: 0.5rem;
}
.warn { color: var(--crit); font-weight: bold; }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.messages-head { display: flex; align-items: center; gap: 0.75rem; }
.messages-head h2 { margin-right: 0; }
.refresh-btn {
  margin-top: 1.6rem;
  padding: 0.25rem 0.75rem;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  color: var(--ink);
  font-size: 0.85rem;
}
.refresh-btn:hover:not(:disabled) { border-color: var(--accent); color: var(--accent); }
.refresh-btn:disabled { opacity: 0.5; cursor: default; }
.mono { font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 0.85rem; }
.nowrap { white-space: nowrap; }
.num { text-align: right; font-variant-numeric: tabular-nums; }
.demo { opacity: 0.55; }
.trend-head { display: flex; align-items: center; gap: 0.75rem; }
.partition-select {
  margin-top: 1.6rem;
  padding: 0.25rem 0.5rem;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  color: var(--ink);
  font-size: 0.85rem;
}
.demo-badge {
  margin-left: 0.35rem;
  padding: 0.05rem 0.4rem;
  border-radius: 999px;
  font-size: 0.7rem;
  font-weight: normal;
  background: var(--line);
  color: var(--ink-soft);
  vertical-align: middle;
}
.value-cell { word-break: break-all; max-width: 480px; }
</style>
