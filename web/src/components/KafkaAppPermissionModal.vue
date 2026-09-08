<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { api } from '@/api/client'
import { describePermission, MODE_LABELS, type KafkaAppDetail, type PermissionMode } from '@/lib/kafkaApps'
import ModalDialog from './ModalDialog.vue'

// topic 이 주어지면 "변경" 모드(토픽 고정), 없으면 "추가" 모드(토픽 선택)
const props = defineProps<{ app: string; topic?: string; mode?: PermissionMode }>()
const emit = defineEmits<{ close: []; saved: [detail: KafkaAppDetail] }>()

interface TopicSummary { name: string; partitionCount: number; replicationFactor: number }

const topics = ref<TopicSummary[]>([])
const topic = ref(props.topic ?? '')
const mode = ref<PermissionMode>(props.mode ?? 'produce')
const error = ref('')
const submitting = ref(false)
const isEdit = computed(() => !!props.topic)
const canSubmit = computed(() => !!topic.value && !submitting.value)
const summary = computed(() => (topic.value ? describePermission(props.app, topic.value, mode.value) : ''))

onMounted(async () => {
  try {
    topics.value = await api<TopicSummary[]>('/topics')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '토픽 목록 조회 실패'
  }
})

async function submit() {
  error.value = ''
  submitting.value = true
  try {
    const detail = await api<KafkaAppDetail>(`/ops/kafka-apps/${props.app}/topics/${topic.value}`, {
      method: 'PUT',
      body: JSON.stringify({ mode: mode.value }),
    })
    emit('saved', detail)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '권한 설정 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog :title="isEdit ? '권한 변경' : '권한 추가'" @close="emit('close')">
    <div class="form">
      <label v-if="!isEdit">
        토픽
        <select name="topic" v-model="topic">
          <option value="">— 선택 —</option>
          <option v-for="t in topics" :key="t.name" :value="t.name">{{ t.name }}</option>
        </select>
      </label>
      <p v-else>토픽 <strong>{{ topic }}</strong></p>
      <label>
        모드
        <select name="mode" v-model="mode">
          <option v-for="(label, value) in MODE_LABELS" :key="value" :value="value">{{ label }}</option>
        </select>
      </label>
      <p v-if="summary" class="summary">{{ summary }}</p>
      <p v-if="error" class="error">{{ error }}</p>
    </div>
    <template #footer>
      <button type="button" class="btn" @click="emit('close')">취소</button>
      <button type="button" class="btn primary" :disabled="!canSubmit" @click="submit">
        {{ submitting ? '적용 중…' : '적용' }}
      </button>
    </template>
  </ModalDialog>
</template>

<style scoped>
.form { display: flex; flex-direction: column; gap: 0.75rem; }
label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
.summary { margin: 0; padding: 0.5rem 0.75rem; background: var(--surface-2); border-radius: 6px; }
.error { color: var(--crit, #c0392b); margin: 0; }
</style>
