<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import GroupRegisterModal, { type RegisteredGroup } from '@/components/GroupRegisterModal.vue'

interface GroupSummary { groupId: string; state: string; memberCount: number }

const groups = ref<GroupSummary[]>([])
const error = ref('')
const showRegister = ref(false)
const { isAdmin } = useSession()

const byId = (a: GroupSummary, b: GroupSummary) => a.groupId.localeCompare(b.groupId)

async function loadGroups() {
  try {
    groups.value = await api<GroupSummary[]>('/groups')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(loadGroups)

// 등록 직후: 코디네이터 반영까지 잠깐 걸릴 수 있어 먼저 표에 넣고, 서버 목록에 나타날 때까지 재조회한다.
const REFRESH_ATTEMPTS = 6
const REFRESH_INTERVAL_MS = 500
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

async function onRegistered(created: RegisteredGroup) {
  showRegister.value = false
  if (!groups.value.some((g) => g.groupId === created.groupId)) {
    groups.value = [...groups.value, created].sort(byId)
  }
  for (let i = 0; i < REFRESH_ATTEMPTS; i++) {
    try {
      const list = await api<GroupSummary[]>('/groups')
      if (list.some((g) => g.groupId === created.groupId)) {
        groups.value = list
        return
      }
      groups.value = [...list, created].sort(byId)
    } catch {
      // 일시적 조회 실패는 무시 (낙관적 표 유지)
    }
    await sleep(REFRESH_INTERVAL_MS)
  }
}
</script>

<template>
  <main>
    <div class="head-row">
      <h1>컨슈머 그룹</h1>
      <button v-if="isAdmin" type="button" class="btn primary" @click="showRegister = true">그룹 등록</button>
    </div>
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
    <GroupRegisterModal v-if="showRegister" @close="showRegister = false" @registered="onRegistered" />
  </main>
</template>

<style scoped>
.head-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
