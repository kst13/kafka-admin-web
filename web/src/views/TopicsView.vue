<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import TopicCreateModal, { type CreatedTopic } from '@/components/TopicCreateModal.vue'

interface TopicSummary { name: string; partitionCount: number; replicationFactor: number }

const topics = ref<TopicSummary[]>([])
const error = ref('')
const showCreate = ref(false)
const { isAdmin } = useSession()

const byName = (a: TopicSummary, b: TopicSummary) => a.name.localeCompare(b.name)

async function loadTopics() {
  try {
    topics.value = await api<TopicSummary[]>('/topics')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(loadTopics)

// 생성 직후: 브로커별 메타데이터 전파 시차 때문에 곧바로 조회한 목록에 새 토픽이 빠질 수 있다.
// 먼저 낙관적으로 표에 넣고, 서버 목록에 나타날 때까지 잠깐(최대 6회 x 500ms) 재조회한다.
const REFRESH_ATTEMPTS = 6
const REFRESH_INTERVAL_MS = 500
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

async function onCreated(created: CreatedTopic) {
  showCreate.value = false
  if (!topics.value.some((t) => t.name === created.name)) {
    topics.value = [...topics.value, created].sort(byName)
  }
  for (let i = 0; i < REFRESH_ATTEMPTS; i++) {
    try {
      const list = await api<TopicSummary[]>('/topics')
      if (list.some((t) => t.name === created.name)) {
        topics.value = list
        return
      }
      topics.value = [...list, created].sort(byName)
    } catch {
      // 일시적 조회 실패는 무시하고 다음 시도에서 다시 본다 (낙관적 표는 유지)
    }
    await sleep(REFRESH_INTERVAL_MS)
  }
}
</script>

<template>
  <main>
    <div class="head-row">
      <h1>토픽</h1>
      <button v-if="isAdmin" type="button" class="btn primary" @click="showCreate = true">토픽 생성</button>
    </div>
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
    <TopicCreateModal v-if="showCreate" @close="showCreate = false" @created="onCreated" />
  </main>
</template>

<style scoped>
.head-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
