<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'

interface GroupSummary { groupId: string; state: string; memberCount: number }

const groups = ref<GroupSummary[]>([])
const error = ref('')

onMounted(async () => {
  try {
    groups.value = await api<GroupSummary[]>('/groups')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})
</script>

<template>
  <main>
    <h1>컨슈머 그룹</h1>
    <p v-if="error" class="error">{{ error }}</p>
    <table v-else>
      <thead><tr><th>그룹</th><th>상태</th><th>멤버 수</th></tr></thead>
      <tbody>
        <tr v-for="g in groups" :key="g.groupId">
          <td><RouterLink :to="`/groups/${g.groupId}`">{{ g.groupId }}</RouterLink></td>
          <td>{{ g.state }}</td>
          <td>{{ g.memberCount }}</td>
        </tr>
      </tbody>
    </table>
  </main>
</template>
