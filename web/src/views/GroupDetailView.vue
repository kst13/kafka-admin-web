<script setup lang="ts">
import { computed, ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { api } from '@/api/client'
import { sumLag } from '@/lib/lag'
import TrendChart from '@/components/TrendChart.vue'

interface PartitionLag { topic: string; partition: number; committed: number; end: number; lag: number }
interface GroupDetail { groupId: string; state: string; lags: PartitionLag[]; totalLag: number }
interface SamplePoint { sampledAt: string; value: number }

const route = useRoute()
const detail = ref<GroupDetail | null>(null)
const error = ref('')
const clientTotal = computed(() => (detail.value ? sumLag(detail.value.lags) : 0))
const trend = ref<{ t: string; v: number }[]>([])

onMounted(async () => {
  try {
    detail.value = await api<GroupDetail>(`/groups/${route.params.groupId}`)
    const pts = await api<SamplePoint[]>(
      `/metrics?type=LAG&subject=${route.params.groupId}&hours=24`,
    )
    trend.value = pts.map((p) => ({ t: p.sampledAt, v: p.value }))
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})
</script>

<template>
  <main>
    <h1>그룹: {{ route.params.groupId }}</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-else-if="detail">
      <p>상태: {{ detail.state }} / 총 랙: <strong>{{ clientTotal }}</strong></p>
      <h2>최근 24시간 랙 추이</h2>
      <TrendChart :points="trend" />
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
.warn { color: #c00; font-weight: bold; }
</style>
