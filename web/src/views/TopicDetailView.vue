<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { api } from '@/api/client'

interface PartitionInfo { partition: number; leader: number; replicas: number[]; isr: number[] }
interface TopicDetail { name: string; partitions: PartitionInfo[]; configs: Record<string, string> }

const route = useRoute()
const detail = ref<TopicDetail | null>(null)
const error = ref('')

onMounted(async () => {
  try {
    detail.value = await api<TopicDetail>(`/topics/${route.params.name}`)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})
</script>

<template>
  <main>
    <h1>토픽: {{ route.params.name }}</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-else-if="detail">
      <h2>설정</h2>
      <ul>
        <li v-for="(v, k) in detail.configs" :key="k">{{ k }} = {{ v }}</li>
      </ul>
      <h2>파티션</h2>
      <table>
        <thead><tr><th>파티션</th><th>리더</th><th>복제본</th><th>ISR</th><th>상태</th></tr></thead>
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
          </tr>
        </tbody>
      </table>
    </template>
  </main>
</template>

<style scoped>
.warn { color: #c00; font-weight: bold; }
</style>
