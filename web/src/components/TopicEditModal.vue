<script setup lang="ts">
import { ref } from 'vue'
import { api } from '@/api/client'
import ModalDialog from './ModalDialog.vue'

const props = defineProps<{
  name: string
  currentPartitions: number
  configs: Record<string, string>
}>()
const emit = defineEmits<{ close: []; updated: [] }>()

const partitions = ref(String(props.currentPartitions))
const retentionMs = ref(props.configs['retention.ms'] ?? '')
const error = ref('')
const submitting = ref(false)

async function submit() {
  error.value = ''
  const body: { partitions?: number; configs?: Record<string, string> } = {}
  const nextPartitions = Number(partitions.value)
  if (nextPartitions !== props.currentPartitions) body.partitions = nextPartitions
  const currentRetention = props.configs['retention.ms'] ?? ''
  if (retentionMs.value !== currentRetention && retentionMs.value !== '') {
    body.configs = { 'retention.ms': String(retentionMs.value) }
  }
  if (body.partitions === undefined && body.configs === undefined) {
    error.value = '변경된 내용이 없습니다'
    return
  }
  submitting.value = true
  try {
    await api(`/ops/topics/${props.name}`, { method: 'PATCH', body: JSON.stringify(body) })
    emit('updated')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '수정 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog :title="`토픽 설정 수정: ${name}`" @close="emit('close')">
    <form id="topic-edit-form" @submit.prevent="submit">
      <label>
        파티션 수 (현재 {{ currentPartitions }})
        <input name="partitions" v-model="partitions" type="number" :min="currentPartitions" />
      </label>
      <p class="note">파티션은 늘릴 수만 있고 줄일 수 없습니다.</p>
      <label>retention.ms <input name="retentionMs" v-model="retentionMs" type="number" min="1" /></label>
      <p v-if="error" class="error">{{ error }}</p>
    </form>
    <template #footer>
      <button type="button" class="btn" @click="emit('close')">취소</button>
      <button type="submit" form="topic-edit-form" :disabled="submitting">적용</button>
    </template>
  </ModalDialog>
</template>

<style scoped>
form {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}
label {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  font-size: 0.85rem;
}
.note {
  margin: 0;
  padding: 0.5rem 0.75rem;
  background: var(--warn-soft, #fdf3d7);
  border-radius: 6px;
  color: var(--warn, #8a6d1a);
  font-size: 0.85rem;
}
.error {
  color: var(--crit, #c0392b);
  margin: 0;
}
</style>
