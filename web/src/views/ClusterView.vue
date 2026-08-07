<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'

interface Broker { id: number; host: string; port: number }
interface Cluster { clusterId: string; controllerId: number; brokers: Broker[] }
interface CertStatus { broker: string; notAfter: string; daysRemaining: number }
interface MonitorStatus {
  lastCollectedAt: string | null
  consecutiveFailures: number
  certs: CertStatus[]
}

const cluster = ref<Cluster | null>(null)
const error = ref('')
const monitor = ref<MonitorStatus | null>(null)

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
      </template>
    </template>
  </main>
</template>

<style scoped>
.warn { color: #c00; font-weight: bold; }
</style>
