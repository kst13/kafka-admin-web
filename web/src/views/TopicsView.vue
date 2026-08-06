<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'

interface TopicSummary { name: string; partitionCount: number; replicationFactor: number }

const topics = ref<TopicSummary[]>([])
const error = ref('')

onMounted(async () => {
  try {
    topics.value = await api<TopicSummary[]>('/topics')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})
</script>

<template>
  <main>
    <h1>토픽</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <table v-else>
      <thead><tr><th>이름</th><th>파티션</th><th>복제 팩터</th></tr></thead>
      <tbody>
        <tr v-for="t in topics" :key="t.name">
          <td><RouterLink :to="`/topics/${t.name}`">{{ t.name }}</RouterLink></td>
          <td>{{ t.partitionCount }}</td>
          <td>{{ t.replicationFactor }}</td>
        </tr>
      </tbody>
    </table>
  </main>
</template>
