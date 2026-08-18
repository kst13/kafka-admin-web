<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import TopicEditModal from '@/components/TopicEditModal.vue'
import TopicDeleteModal from '@/components/TopicDeleteModal.vue'

interface PartitionInfo { partition: number; leader: number; replicas: number[]; isr: number[] }
interface TopicDetail { name: string; partitions: PartitionInfo[]; configs: Record<string, string> }
interface MessageRecord {
  partition: number
  offset: number
  timestamp: string
  key: string | null
  value: string | null
}

const route = useRoute()
const router = useRouter()
const { isAdmin } = useSession()
const detail = ref<TopicDetail | null>(null)
const error = ref('')
const messages = ref<MessageRecord[]>([])
const messagesError = ref('')
const messagesLoading = ref(false)
const showEdit = ref(false)
const showDelete = ref(false)

async function loadMessages() {
  messagesLoading.value = true
  messagesError.value = ''
  try {
    messages.value = await api<MessageRecord[]>(`/topics/${route.params.name}/messages?limit=50`)
  } catch (e) {
    messagesError.value = e instanceof Error ? e.message : '조회 실패'
  } finally {
    messagesLoading.value = false
  }
}

onMounted(async () => {
  try {
    detail.value = await api<TopicDetail>(`/topics/${route.params.name}`)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
  await loadMessages()
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
        <button type="button" class="btn" @click="showEdit = true">설정 수정</button>
        <button type="button" class="btn danger-outline" @click="showDelete = true">삭제</button>
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
      <div class="messages-head">
        <h2>최근 메시지</h2>
        <button type="button" class="refresh-btn" :disabled="messagesLoading" @click="loadMessages">
          {{ messagesLoading ? '조회 중…' : '새로고침' }}
        </button>
      </div>
      <p v-if="messagesError" class="error">{{ messagesError }}</p>
      <p v-else-if="messages.length === 0" class="hint">메시지가 없습니다.</p>
      <table v-else class="messages-table">
        <thead>
          <tr><th>시각</th><th>파티션</th><th>오프셋</th><th>키</th><th>값</th></tr>
        </thead>
        <tbody>
          <tr v-for="m in messages" :key="`${m.partition}-${m.offset}`">
            <td class="nowrap">{{ new Date(m.timestamp).toLocaleString() }}</td>
            <td>{{ m.partition }}</td>
            <td>{{ m.offset }}</td>
            <td class="mono">{{ m.key ?? '—' }}</td>
            <td class="mono value-cell">{{ m.value ?? '—' }}</td>
          </tr>
        </tbody>
      </table>
      <p class="hint">최신순 최대 50건, 값은 1,000자까지 표시됩니다.</p>
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
.warn { color: var(--crit); font-weight: bold; }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.messages-head { display: flex; align-items: center; gap: 0.75rem; }
.messages-head h2 { margin-right: 0; }
.refresh-btn {
  margin-top: 1.6rem;
  padding: 0.25rem 0.75rem;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  color: var(--ink);
  font-size: 0.85rem;
}
.refresh-btn:hover:not(:disabled) { border-color: var(--accent); color: var(--accent); }
.refresh-btn:disabled { opacity: 0.5; cursor: default; }
.mono { font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 0.85rem; }
.nowrap { white-space: nowrap; }
.value-cell { word-break: break-all; max-width: 480px; }
</style>
