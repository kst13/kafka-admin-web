<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'
import TrendChart from '@/components/TrendChart.vue'

interface Broker { id: number; host: string; port: number }
interface Cluster { clusterId: string; controllerId: number; brokers: Broker[] }
interface CertStatus { broker: string; notAfter: string; daysRemaining: number }
interface MonitorStatus {
  lastCollectedAt: string | null
  consecutiveFailures: number
  certs: CertStatus[]
}
interface BrokerDisk { brokerId: number; usedPercent: number }
interface DiskStatus { thresholdPct: number; brokers: BrokerDisk[] }
interface SamplePoint { sampledAt: string; value: number }
type TrendPoint = { t: string; v: number }

const cluster = ref<Cluster | null>(null)
const error = ref('')
const monitor = ref<MonitorStatus | null>(null)
const disk = ref<DiskStatus | null>(null)
const urpTrend = ref<TrendPoint[]>([])
const diskTrend = ref<TrendPoint[]>([])
const diskTrendBroker = ref<number | null>(null)

async function fetchTrend(type: string, subject: string): Promise<TrendPoint[]> {
  const pts = await api<SamplePoint[]>(
    `/metrics?type=${type}&subject=${encodeURIComponent(subject)}&hours=24`,
  )
  return pts.map((p) => ({ t: p.sampledAt, v: p.value }))
}

async function loadDiskTrend(brokerId: number) {
  diskTrendBroker.value = brokerId
  try {
    diskTrend.value = await fetchTrend('DISK_USED_PCT', String(brokerId))
  } catch {
    diskTrend.value = [] // TrendChart가 '데이터 없음' 표시
  }
}

onMounted(async () => {
  try {
    cluster.value = await api<Cluster>('/cluster')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
  try {
    monitor.value = await api<MonitorStatus>('/monitor/status')
  } catch {
    // 감시 상태 카드만 생략하고 클러스터 화면은 그대로 둔다
  }
  try {
    urpTrend.value = await fetchTrend('URP', 'cluster')
  } catch {
    // 차트만 생략
  }
  try {
    disk.value = await api<DiskStatus>('/monitor/disk')
  } catch {
    // 디스크 카드만 생략 (브로커 무응답 등)
  }
  // 사용률이 가장 높은 브로커의 추이를 기본으로 보여준다
  let worst: BrokerDisk | null = null
  for (const b of disk.value?.brokers ?? []) {
    if (!worst || b.usedPercent > worst.usedPercent) worst = b
  }
  if (worst) await loadDiskTrend(worst.brokerId)
})
</script>

<template>
  <main>
    <h1>클러스터</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-else-if="cluster">
      <p>Cluster ID: {{ cluster.clusterId }}</p>
      <table>
        <thead><tr><th>브로커 ID</th><th>주소</th><th>역할</th></tr></thead>
        <tbody>
          <tr v-for="b in cluster.brokers" :key="b.id">
            <td>{{ b.id }}</td>
            <td>{{ b.host }}:{{ b.port }}</td>
            <td>{{ b.id === cluster.controllerId ? '컨트롤러' : '' }}</td>
          </tr>
        </tbody>
      </table>
      <template v-if="monitor">
        <h2>감시 상태</h2>
        <p>
          마지막 수집:
          {{ monitor.lastCollectedAt ? new Date(monitor.lastCollectedAt).toLocaleString() : '없음' }}
          <span v-if="monitor.consecutiveFailures > 0" class="warn">
            (연속 실패 {{ monitor.consecutiveFailures }}회)
          </span>
        </p>
        <table v-if="monitor.certs.length > 0">
          <thead><tr><th>브로커</th><th>인증서 만료</th><th>남은 일수</th></tr></thead>
          <tbody>
            <tr v-for="c in monitor.certs" :key="c.broker">
              <td>{{ c.broker }}</td>
              <td>{{ new Date(c.notAfter).toLocaleDateString() }}</td>
              <td :class="{ warn: c.daysRemaining <= 30 }">D-{{ c.daysRemaining }}</td>
            </tr>
          </tbody>
        </table>
        <p v-else>인증서 정보 없음 (PLAINTEXT 구성이거나 아직 점검 전)</p>
        <h2>미복제 파티션(URP) 추이 — 24시간</h2>
        <TrendChart :points="urpTrend" label="URP 추이 차트" />
        <p class="hint">0이 정상입니다. 0보다 크면 어느 브로커의 복제가 뒤처져 있다는 신호입니다.</p>
      </template>
      <template v-if="disk">
        <h2>디스크 사용량</h2>
        <p v-if="disk.brokers.length === 0">디스크 정보 없음 (브로커가 totalBytes를 제공하지 않음)</p>
        <table v-else>
          <thead><tr><th>브로커</th><th>사용률</th><th></th></tr></thead>
          <tbody>
            <tr v-for="b in disk.brokers" :key="b.brokerId">
              <td>{{ b.brokerId }}</td>
              <td :class="{ warn: b.usedPercent >= disk.thresholdPct }">
                {{ b.usedPercent.toFixed(1) }}%
              </td>
              <td class="bar-cell">
                <div class="bar">
                  <div
                    class="bar-fill"
                    :class="{ 'bar-warn': b.usedPercent >= disk.thresholdPct }"
                    :style="{ width: Math.min(b.usedPercent, 100) + '%' }"
                  ></div>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
        <p class="hint">임계치 {{ disk.thresholdPct }}% 초과 시 알림이 발생합니다.</p>
        <template v-if="diskTrendBroker !== null">
          <h2>디스크 사용률 추이 — 24시간</h2>
          <div class="broker-tabs">
            <button
              v-for="b in disk.brokers"
              :key="b.brokerId"
              type="button"
              :class="{ on: diskTrendBroker === b.brokerId }"
              @click="loadDiskTrend(b.brokerId)"
            >
              브로커 {{ b.brokerId }}
            </button>
          </div>
          <TrendChart :points="diskTrend" label="디스크 사용률 추이 차트" />
        </template>
      </template>
    </template>
  </main>
</template>

<style scoped>
.warn { color: var(--crit); font-weight: bold; }
.bar-cell { width: 40%; min-width: 160px; }
.bar { height: 10px; background: var(--surface-2); border-radius: 5px; overflow: hidden; }
.bar-fill { height: 100%; background: var(--accent); border-radius: 5px; }
.bar-warn { background: var(--crit); }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.broker-tabs { display: flex; gap: 0.5rem; margin-bottom: 0.5rem; }
.broker-tabs button {
  padding: 0.25rem 0.75rem;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  color: var(--ink);
  font-size: 0.85rem;
}
.broker-tabs button.on {
  border-color: var(--accent);
  color: var(--accent);
  font-weight: bold;
}
</style>
