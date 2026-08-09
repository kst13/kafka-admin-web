<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import TopicEditModal from '@/components/TopicEditModal.vue'
import TopicDeleteModal from '@/components/TopicDeleteModal.vue'

interface PartitionInfo { partition: number; leader: number; replicas: number[]; isr: number[] }
interface TopicDetail { name: string; partitions: PartitionInfo[]; configs: Record<string, string> }

const route = useRoute()
const router = useRouter()
const { isAdmin } = useSession()
const detail = ref<TopicDetail | null>(null)
const error = ref('')
const showEdit = ref(false)
const showDelete = ref(false)

onMounted(async () => {
  try {
    detail.value = await api<TopicDetail>(`/topics/${route.params.name}`)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
})

async function reload() {
  showEdit.value = false
  detail.value = await api<TopicDetail>(`/topics/${route.params.name}`)
}

function onDeleted() {
  router.push('/topics')
}
</script>

<template>
  <main>
    <div class="head-row">
      <h1>토픽: {{ route.params.name }}</h1>
      <div v-if="isAdmin && detail" class="actions">
        <button type="button" @click="showEdit = true">설정 수정</button>
        <button type="button" class="danger-outline" @click="showDelete = true">삭제</button>
      </div>
    </div>
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
    <TopicEditModal
      v-if="showEdit && detail"
      :name="detail.name"
      :current-partitions="detail.partitions.length"
      :configs="detail.configs"
      @close="showEdit = false"
      @updated="reload"
    />
    <TopicDeleteModal
      v-if="showDelete && detail"
      :name="detail.name"
      @close="showDelete = false"
      @deleted="onDeleted"
    />
  </main>
</template>

<style scoped>
.head-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.actions {
  display: flex;
  gap: 0.5rem;
}
.danger-outline {
  border: 1px solid var(--crit);
  color: var(--crit);
  background: none;
  border-radius: 6px;
  padding: 0.35rem 0.9rem;
}
.warn { color: #c00; font-weight: bold; }
</style>
