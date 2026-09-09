<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import { api } from '@/api/client'
import MetricChart from '@/components/MetricChart.vue'
import {
  SERIES_RANGES, ALERT_RULE_LABELS, formatBytesPerSec, formatMs, formatPct, formatCount, formatValue,
  type ClusterHealth, type BrokerSnapshot, type SeriesResponse, type Series, type SeriesRange,
} from '@/lib/metrics'

interface AlertEvent { ruleType: string; subjectKey: string; message: string; value: number; threshold: number; occurredAt: string }

const route = useRoute()
const id = computed(() => Number(route.params.id))
const snapshot = ref<BrokerSnapshot | null>(null)
const error = ref('')
const range = ref<SeriesRange>('1h')
const alerts = ref<AlertEvent[]>([])
const seriesByKey = ref<Map<string, Series[]>>(new Map())
const requestQueue = ref<number | null>(null)

const CHARTS = [
  { title: '처리량', unit: 'bytes/s', keys: ['BROKER_BYTES_IN', 'BROKER_BYTES_OUT'] },
  { title: '지연', unit: 'ms', keys: ['BROKER_P99_PRODUCE_MS', 'BROKER_P99_FETCH_MS'] },
  { title: '부하', unit: '%', keys: ['BROKER_HANDLER_IDLE_PCT', 'BROKER_NETWORK_IDLE_PCT'] },
  { title: 'JVM', unit: '%', keys: ['BROKER_HEAP_USED_PCT', 'BROKER_GC_TIME_PCT'] },
]
const ALL_KEYS = [...CHARTS.flatMap((c) => c.keys), 'BROKER_REQUEST_QUEUE']
const BROKER_RULES = new Set(['LATENCY_HIGH', 'HANDLER_SATURATED', 'HEAP_HIGH', 'DISK_HIGH'])

function chartSeries(keys: string[]): Series[] {
  return keys.flatMap((k) => seriesByKey.value.get(k) ?? [])
}

const brokerAlerts = computed(() =>
  alerts.value.filter((a) => a.subjectKey === String(id.value) && BROKER_RULES.has(a.ruleType)),
)

async function loadSeries() {
  const results = await Promise.all(ALL_KEYS.map(async (key) => {
    try {
      const r = await api<SeriesResponse>(`/brokers/${id.value}/series?key=${key}&range=${range.value}`)
      return [key, r.series] as const
    } catch (e) {
      if (!error.value) error.value = e instanceof Error ? e.message : '조회 실패'
      return [key, [] as Series[]] as const
    }
  }))
  seriesByKey.value = new Map(results)
  const q = seriesByKey.value.get('BROKER_REQUEST_QUEUE')?.[0]?.points
  requestQueue.value = q && q.length > 0 ? q[q.length - 1]!.v : null
}

onMounted(async () => {
  try {
    const health = await api<ClusterHealth>('/cluster/health')
    snapshot.value = health.brokers.find((b) => b.id === id.value) ?? null
    if (!snapshot.value) error.value = `존재하지 않는 브로커입니다: ${id.value}`
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
  try {
    alerts.value = await api<AlertEvent[]>('/alerts')
  } catch {
    // 알림 표만 생략
  }
  if (snapshot.value) await loadSeries()
})

watch(range, () => { if (snapshot.value) loadSeries() })
</script>

<template>
  <main>
    <h1>브로커 {{ id }}<span v-if="snapshot" class="host">{{ snapshot.host }}</span></h1>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-if="snapshot">
      <p v-if="!snapshot.scraped" class="hint">Prometheus 에 이 브로커의 시리즈가 없습니다 (스크랩 대상 확인).</p>
      <div class="cards">
        <div class="card"><span class="label">유입 / 유출</span><span class="value">{{ formatBytesPerSec(snapshot.bytesInPerSec) }} / {{ formatBytesPerSec(snapshot.bytesOutPerSec) }}</span></div>
        <div class="card"><span class="label">메시지/초</span><span class="value">{{ formatValue(snapshot.messagesInPerSec, 'msg/s') }}</span></div>
        <div class="card"><span class="label">Produce p99</span><span class="value">{{ formatMs(snapshot.p99ProduceMs) }}</span></div>
        <div class="card"><span class="label">Fetch p99</span><span class="value">{{ formatMs(snapshot.p99FetchMs) }}</span></div>
        <div class="card"><span class="label">핸들러 유휴율</span><span class="value">{{ formatPct(snapshot.handlerIdlePct) }}</span></div>
        <div class="card"><span class="label">네트워크 유휴율</span><span class="value">{{ formatPct(snapshot.networkIdlePct) }}</span></div>
        <div class="card"><span class="label">요청 큐</span><span class="value">{{ requestQueue === null ? '—' : formatCount(requestQueue) }}</span></div>
        <div class="card"><span class="label">힙</span><span class="value">{{ formatPct(snapshot.heapUsedPct) }}</span></div>
        <div class="card"><span class="label">CPU</span><span class="value">{{ formatPct(snapshot.cpuPct) }}</span></div>
        <div class="card"><span class="label">리더 / 파티션</span><span class="value">{{ snapshot.leaderCount }} / {{ snapshot.partitionCount }}</span></div>
        <div class="card"><span class="label">미복제 파티션</span><span class="value" :class="{ warn: snapshot.underReplicated > 0 }">{{ snapshot.underReplicated }}</span></div>
      </div>
      <div class="range-tabs">
        <button v-for="r in SERIES_RANGES" :key="r.value" type="button" :class="{ on: range === r.value }" @click="range = r.value">
          {{ r.label }}
        </button>
      </div>
      <MetricChart v-for="c in CHARTS" :key="c.title" :series="chartSeries(c.keys)" :unit="c.unit" :title="c.title" />
      <h2>이 브로커의 알림</h2>
      <p v-if="brokerAlerts.length === 0" class="hint">알림이 없습니다.</p>
      <table v-else class="broker-alerts">
        <thead><tr><th>시각</th><th>유형</th><th>내용</th></tr></thead>
        <tbody>
          <tr v-for="a in brokerAlerts" :key="a.occurredAt + a.ruleType">
            <td>{{ new Date(a.occurredAt).toLocaleString() }}</td>
            <td>{{ ALERT_RULE_LABELS[a.ruleType]?.label ?? a.ruleType }}</td>
            <td>{{ a.message }}</td>
          </tr>
        </tbody>
      </table>
    </template>
  </main>
</template>

<style scoped>
.host { margin-left: 0.75rem; font-size: 0.9rem; font-weight: normal; color: var(--ink-soft); }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.warn { color: var(--crit); font-weight: bold; }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 0.6rem; margin: 0.75rem 0 1rem; }
.card { display: flex; flex-direction: column; gap: 0.2rem; padding: 0.6rem 0.75rem; background: var(--surface); border: 1px solid var(--line); border-radius: 8px; }
.card .label { font-size: 0.75rem; color: var(--ink-soft); }
.card .value { font-size: 1.05rem; font-weight: 600; font-variant-numeric: tabular-nums; }
.range-tabs { display: flex; gap: 0.5rem; margin-bottom: 0.75rem; }
.range-tabs button { padding: 0.25rem 0.75rem; border: 1px solid var(--line); border-radius: 6px; background: var(--surface); color: var(--ink); font-size: 0.85rem; }
.range-tabs button.on { border-color: var(--accent); color: var(--accent); font-weight: bold; }
</style>
