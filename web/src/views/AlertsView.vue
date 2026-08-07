<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'

interface AlertEvent {
  ruleType: string
  subjectKey: string
  message: string
  value: number
  threshold: number
  occurredAt: string
}

const alerts = ref<AlertEvent[]>([])
const error = ref('')

onMounted(async () => {
  try {
    alerts.value = await api<AlertEvent[]>('/alerts')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})
</script>

<template>
  <main>
    <h1>알림 이력</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <p v-else-if="alerts.length === 0">알림이 없습니다.</p>
    <table v-else>
      <thead>
        <tr><th>시각</th><th>유형</th><th>대상</th><th>내용</th></tr>
      </thead>
      <tbody>
        <tr v-for="a in alerts" :key="a.occurredAt + a.ruleType + a.subjectKey">
          <td>{{ new Date(a.occurredAt).toLocaleString() }}</td>
          <td>{{ a.ruleType }}</td>
          <td>{{ a.subjectKey }}</td>
          <td>{{ a.message }}</td>
        </tr>
      </tbody>
    </table>
  </main>
</template>
