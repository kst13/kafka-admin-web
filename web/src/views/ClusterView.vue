<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'

interface Broker { id: number; host: string; port: number }
interface Cluster { clusterId: string; controllerId: number; brokers: Broker[] }

const cluster = ref<Cluster | null>(null)
const error = ref('')

onMounted(async () => {
  try {
    cluster.value = await api<Cluster>('/cluster')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
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
    </template>
  </main>
</template>
