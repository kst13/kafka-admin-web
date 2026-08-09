<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import TopicCreateModal from '@/components/TopicCreateModal.vue'

interface TopicSummary { name: string; partitionCount: number; replicationFactor: number }

const topics = ref<TopicSummary[]>([])
const error = ref('')
const showCreate = ref(false)
const { isAdmin } = useSession()

async function loadTopics() {
  try {
    topics.value = await api<TopicSummary[]>('/topics')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(loadTopics)

function onCreated() {
  showCreate.value = false
  loadTopics()
}
</script>

<template>
  <main>
    <div class="head-row">
      <h1>토픽</h1>
      <button v-if="isAdmin" type="button" @click="showCreate = true">토픽 생성</button>
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
