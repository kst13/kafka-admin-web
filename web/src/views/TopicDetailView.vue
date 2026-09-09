<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import { usePrometheus } from '@/composables/usePrometheus'
import { toHourlyConsumption, type Point } from '@/lib/consumption'
import { SERIES_RANGES, formatBytes, formatCount, type Series, type SeriesResponse, type SeriesRange } from '@/lib/metrics'
import TopicEditModal from '@/components/TopicEditModal.vue'
import TopicDeleteModal from '@/components/TopicDeleteModal.vue'
import TrendChart from '@/components/TrendChart.vue'
import MetricChart from '@/components/MetricChart.vue'
import TopicSchemaSection from '@/components/TopicSchemaSection.vue'

interface PartitionInfo { partition: number; leader: number; replicas: number[]; isr: number[] }
interface TopicDetail { name: string; partitions: PartitionInfo[]; configs: Record<string, string> }
interface PartitionThroughput { partition: number; endOffset: number; count: number; ratePerMin: number }
interface SamplePoint { sampledAt: string; value: number }
interface MessageRecord { partition: number; offset: number; timestamp: string; key: string | null; value: string | null }

const route = useRoute()
const router = useRouter()
const { isAdmin } = useSession()
const { configured: prometheusConfigured, ready: prometheusReady } = usePrometheus()
const topicName = computed(() => String(route.params.name))
const detail = ref<TopicDetail | null>(null)
const error = ref('')
const messages = ref<MessageRecord[]>([])
const messagesError = ref('')
const messagesLoading = ref(false)
const showEdit = ref(false)
const showDelete = ref(false)

// --- 폴백(수집기) 모드: 저장된 PRODUCED_* 샘플 ---
const throughput = ref<Map<number, PartitionThroughput>>(new Map())
const producedTrend = ref<Point[]>([])

async function loadThroughput() {
  try {
    const list = await api<PartitionThroughput[]>(`/topics/${topicName.value}/throughput`)
    throughput.value = new Map(list.map((t) => [t.partition, t]))
    const samples = await api<SamplePoint[]>(
      `/metrics?type=PRODUCED_TOPIC&subject=${encodeURIComponent(topicName.value)}&hours=24`,
    )
    producedTrend.value = toHourlyConsumption(samples.map((s) => ({ t: s.sampledAt, v: s.value })))
  } catch {
    throughput.value = new Map()
    producedTrend.value = []
  }
}

// --- Prometheus 모드: 파티션 현재값 + 유입 추이 ---
const retained = ref<Map<number, number>>(new Map())
const logSize = ref<Map<number, number>>(new Map())
const intakeMetric = ref<'TOPIC_MESSAGES_IN' | 'TOPIC_BYTES_IN'>('TOPIC_MESSAGES_IN')
const intakeRange = ref<SeriesRange>('1h')
const intake = ref<SeriesResponse | null>(null)
const intakeError = ref('')

// 파티션별 시리즈의 마지막 포인트를 현재값으로 쓴다
function lastByPartition(series: Series[]): Map<number, number> {
  const m = new Map<number, number>()
  for (const s of series) {
    const p = s.points[s.points.length - 1]
    if (p) m.set(Number(s.name), p.v)
  }
  return m
}

function retainedText(partition: number): string {
  const v = retained.value.get(partition)
  return v === undefined ? '—' : formatCount(v)
}
function logSizeText(partition: number): string {
  const v = logSize.value.get(partition)
  return v === undefined ? '—' : formatBytes(v)
}

async function loadPartitionMetrics() {
  try {
    const [r, l] = await Promise.all([
      api<SeriesResponse>(`/topics/${topicName.value}/series?key=TOPIC_RETAINED_BY_PARTITION&range=1h`),
      api<SeriesResponse>(`/topics/${topicName.value}/series?key=TOPIC_LOG_SIZE_BY_PARTITION&range=1h`),
    ])
    retained.value = lastByPartition(r.series)
    logSize.value = lastByPartition(l.series)
  } catch {
    retained.value = new Map()
    logSize.value = new Map()
  }
}

// loadIntake() 는 지표/범위 전환마다 다시 실행된다 — 응답이 늦게 도착한 오래된 요청이 최신 선택을
// 덮어쓰지 않도록 세대 번호로 막는다.
let intakeGeneration = 0

async function loadIntake() {
  const gen = ++intakeGeneration
  intakeError.value = ''
  try {
    const res = await api<SeriesResponse>(
      `/topics/${topicName.value}/series?key=${intakeMetric.value}&range=${intakeRange.value}`,
    )
    if (gen !== intakeGeneration) return
    intake.value = res
  } catch (e) {
    if (gen !== intakeGeneration) return
    intake.value = null
    intakeError.value = e instanceof Error ? e.message : '조회 실패'
  }
}

watch([intakeMetric, intakeRange], () => { if (prometheusConfigured.value) loadIntake() })

async function loadMessages() {
  messagesLoading.value = true
  messagesError.value = ''
  try {
    messages.value = await api<MessageRecord[]>(`/topics/${topicName.value}/messages?limit=50`)
  } catch (e) {
    messagesError.value = e instanceof Error ? e.message : '조회 실패'
  } finally {
    messagesLoading.value = false
  }
}

onMounted(async () => {
  try {
    detail.value = await api<TopicDetail>(`/topics/${topicName.value}`)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
  // App.vue 의 Prometheus 상태 로드가 아직 끝나지 않았을 수 있으므로, 모드를 정하기 전에 기다린다.
  await prometheusReady()
  const extra = prometheusConfigured.value ? [loadPartitionMetrics(), loadIntake()] : [loadThroughput()]
  await Promise.all([loadMessages(), ...extra])
})

async function reload() {
  showEdit.value = false
  detail.value = await api<TopicDetail>(`/topics/${topicName.value}`)
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
      <TopicSchemaSection :topic="String(route.params.name)" />
      <h2>파티션</h2>
      <table>
        <thead>
          <tr>
            <th>파티션</th><th>리더</th><th>복제본</th><th>ISR</th><th>상태</th>
            <template v-if="prometheusConfigured"><th>보유 메시지</th><th>로그 크기</th></template>
            <template v-else><th>endOffset</th><th>최근 1시간 유입</th><th>분당 속도</th></template>
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
            <template v-if="prometheusConfigured">
              <td class="num">{{ retainedText(p.partition) }}</td>
              <td class="num">{{ logSizeText(p.partition) }}</td>
            </template>
            <template v-else>
              <td class="num">{{ throughput.get(p.partition)?.endOffset ?? '—' }}</td>
              <td class="num">{{ throughput.get(p.partition)?.count ?? '—' }}</td>
              <td class="num">{{ throughput.get(p.partition)?.ratePerMin.toFixed(1) ?? '—' }}</td>
            </template>
          </tr>
        </tbody>
      </table>
      <template v-if="prometheusConfigured">
        <div class="trend-head">
          <h2>유입 추이</h2>
          <div class="metric-tabs">
            <button type="button" :class="{ on: intakeMetric === 'TOPIC_MESSAGES_IN' }" @click="intakeMetric = 'TOPIC_MESSAGES_IN'">메시지</button>
            <button type="button" :class="{ on: intakeMetric === 'TOPIC_BYTES_IN' }" @click="intakeMetric = 'TOPIC_BYTES_IN'">바이트</button>
          </div>
          <div class="range-tabs">
            <button v-for="r in SERIES_RANGES" :key="r.value" type="button" :class="{ on: intakeRange === r.value }" @click="intakeRange = r.value">
              {{ r.label }}
            </button>
          </div>
        </div>
        <p v-if="intakeError" class="error">{{ intakeError }}</p>
        <MetricChart v-else-if="intake" :series="intake.series" :unit="intake.unit" title="유입 추이" />
      </template>
      <template v-else>
        <h2>유입 추이 (시간대별)</h2>
        <TrendChart v-if="producedTrend.length > 0" :points="producedTrend" label="토픽 유입 추이 차트" />
        <p v-else class="hint">수집된 유입 샘플이 아직 없습니다. 수집이 시작되면 표시됩니다.</p>
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
.trend-head { display: flex; align-items: center; gap: 0.75rem; flex-wrap: wrap; }
.metric-tabs, .range-tabs { display: flex; gap: 0.4rem; margin-top: 1.6rem; }
.metric-tabs button, .range-tabs button {
  padding: 0.25rem 0.75rem; border: 1px solid var(--line); border-radius: 6px;
  background: var(--surface); color: var(--ink); font-size: 0.85rem;
}
.metric-tabs button.on, .range-tabs button.on { border-color: var(--accent); color: var(--accent); font-weight: bold; }
.value-cell { word-break: break-all; max-width: 480px; }
</style>
