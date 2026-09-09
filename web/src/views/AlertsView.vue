<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { api } from '@/api/client'
import { ALERT_RULE_LABELS, alertLink } from '@/lib/metrics'

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

// 템플릿에서 non-null 단언을 쓰지 않도록 라벨·링크를 미리 계산한다
const rows = computed(() =>
  alerts.value.map((a) => ({
    ...a,
    label: ALERT_RULE_LABELS[a.ruleType]?.label ?? a.ruleType,
    description: ALERT_RULE_LABELS[a.ruleType]?.description ?? '',
    link: alertLink(a.ruleType, a.subjectKey),
  })),
)

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
        <tr v-for="a in rows" :key="a.occurredAt + a.ruleType + a.subjectKey">
          <td>{{ new Date(a.occurredAt).toLocaleString() }}</td>
          <td :title="a.description">{{ a.label }}</td>
          <td>
            <RouterLink v-if="a.link" :to="a.link">{{ a.subjectKey }}</RouterLink>
            <template v-else>{{ a.subjectKey }}</template>
          </td>
          <td>{{ a.message }}</td>
        </tr>
      </tbody>
    </table>
  </main>
</template>
