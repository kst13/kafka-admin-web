<script setup lang="ts">
import { computed, ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { api } from '@/api/client'
import { sumLag } from '@/lib/lag'
import { toHourlyConsumption, toConsumptionLog } from '@/lib/consumption'
import type { ConsumptionEntry, TopicSeries } from '@/lib/consumption'
import TrendChart from '@/components/TrendChart.vue'
import LagHelp from '@/components/LagHelp.vue'

interface PartitionLag { topic: string; partition: number; committed: number; end: number; lag: number }
interface GroupMember { memberId: string; clientId: string; host: string; assignedPartitions: string[] }
interface GroupDetail {
  groupId: string
  state: string
  lags: PartitionLag[]
  totalLag: number
  members: GroupMember[]
}
interface SamplePoint { sampledAt: string; value: number }

const route = useRoute()
const detail = ref<GroupDetail | null>(null)
const error = ref('')
const clientTotal = computed(() => (detail.value ? sumLag(detail.value.lags) : 0))
const trend = ref<{ t: string; v: number }[]>([])
const consumption = ref<{ t: string; v: number }[]>([])
const consumptionLog = ref<ConsumptionEntry[]>([])
// 표는 최신 시간대부터 보여준다 (차트는 시간 순)
const consumptionRows = computed(() => [...consumption.value].reverse())

function hourLabel(t: string): string {
  const d = new Date(t)
  return `${d.getMonth() + 1}/${d.getDate()} ${String(d.getHours()).padStart(2, '0')}:00~`
}

onMounted(async () => {
  try {
    detail.value = await api<GroupDetail>(`/groups/${route.params.groupId}`)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }

  try {
    const pts = await api<SamplePoint[]>(
      `/metrics?type=LAG&subject=${route.params.groupId}&hours=24`,
    )
    trend.value = pts.map((p) => ({ t: p.sampledAt, v: p.value }))
  } catch {
    // 차트만 생략하고 그룹 상세는 그대로 둔다 (TrendChart가 '데이터 없음' 표시)
  }

  try {
    const pts = await api<SamplePoint[]>(
      `/metrics?type=CONSUMED_TOTAL&subject=${route.params.groupId}&hours=24`,
    )
    consumption.value = toHourlyConsumption(pts.map((p) => ({ t: p.sampledAt, v: p.value })))
  } catch {
    // 차트만 생략
  }

  // 토픽별 소비 내역: 그룹이 소비 중인 토픽마다 CONSUMED_TOPIC 이력을 모아 증가분만 남긴다
  if (detail.value) {
    const topics = [...new Set(detail.value.lags.map((l) => l.topic))]
    const series: TopicSeries[] = []
    for (const topic of topics) {
      try {
        const subject = encodeURIComponent(`${route.params.groupId}|${topic}`)
        const pts = await api<SamplePoint[]>(
          `/metrics?type=CONSUMED_TOPIC&subject=${subject}&hours=24`,
        )
        series.push({ topic, points: pts.map((p) => ({ t: p.sampledAt, v: p.value })) })
      } catch {
        // 해당 토픽 내역만 생략
      }
    }
    consumptionLog.value = toConsumptionLog(series)
  }
})
</script>

<template>
  <main>
    <div class="page-head">
      <h1>그룹: {{ route.params.groupId }}</h1>
      <LagHelp />
    </div>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-else-if="detail">
      <p>상태: {{ detail.state }} / 총 랙: <strong>{{ clientTotal }}</strong></p>
      <h2>멤버 ({{ detail.members.length }})</h2>
      <p v-if="detail.members.length === 0" class="hint">
        활성 멤버가 없습니다 — 컨슈머가 모두 내려간 상태(Empty)로, 커밋 오프셋은 그룹에 남아 있습니다.
      </p>
      <table v-else>
        <thead><tr><th>클라이언트 ID</th><th>호스트</th><th>담당 파티션</th><th>멤버 ID</th></tr></thead>
        <tbody>
          <tr v-for="m in detail.members" :key="m.memberId">
            <td>{{ m.clientId }}</td>
            <td class="mono">{{ m.host }}</td>
            <td class="mono">{{ m.assignedPartitions.join(', ') }}</td>
            <td class="mono member-id">{{ m.memberId }}</td>
          </tr>
        </tbody>
      </table>
      <h2>최근 24시간 랙 추이</h2>
      <TrendChart :points="trend" />
      <h2>시간대별 소비량</h2>
      <TrendChart :points="consumption" label="시간대별 소비량 차트" />
      <table v-if="consumptionRows.length > 0" class="consumption-table">
        <thead><tr><th>시간대</th><th class="num">소비 건수</th></tr></thead>
        <tbody>
          <tr v-for="p in consumptionRows" :key="p.t">
            <td>{{ hourLabel(p.t) }}</td>
            <td class="num">{{ p.v.toLocaleString() }}</td>
          </tr>
        </tbody>
      </table>
      <p class="hint">
        각 시간대에 커밋 오프셋이 얼마나 증가했는지(=소비한 메시지 수)입니다.
        60초 수집 이력 기반이라 수집이 시작된 뒤부터 쌓입니다.
      </p>
      <h2>최근 소비 내역</h2>
      <p v-if="consumptionLog.length === 0" class="hint">
        최근 24시간 내 소비 기록이 없습니다 (60초 수집 주기 단위로 잡힙니다).
      </p>
      <table v-else class="consumption-table">
        <thead><tr><th>시각</th><th>토픽</th><th class="num">가져온 건수</th></tr></thead>
        <tbody>
          <tr v-for="e in consumptionLog" :key="`${e.t}-${e.topic}`">
            <td>{{ new Date(e.t).toLocaleTimeString() }}</td>
            <td>{{ e.topic }}</td>
            <td class="num">{{ e.count.toLocaleString() }}</td>
          </tr>
        </tbody>
      </table>
      <table>
        <thead>
          <tr><th>토픽</th><th>파티션</th><th>커밋 오프셋</th><th>최신 오프셋</th><th>랙</th></tr>
        </thead>
        <tbody>
          <tr v-for="p in detail.lags" :key="`${p.topic}-${p.partition}`">
            <td>{{ p.topic }}</td>
            <td>{{ p.partition }}</td>
            <td>{{ p.committed }}</td>
            <td>{{ p.end }}</td>
            <td :class="{ warn: p.lag > 0 }">{{ p.lag }}</td>
          </tr>
        </tbody>
      </table>
    </template>
  </main>
</template>

<style scoped>
.warn { color: var(--crit); font-weight: bold; }
.page-head { display: flex; justify-content: space-between; align-items: center; gap: 1rem; }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.consumption-table { max-width: 480px; }
.consumption-table .num { text-align: right; font-variant-numeric: tabular-nums; }
.mono { font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 0.85rem; }
.member-id { word-break: break-all; color: var(--ink-soft); }
</style>
