<script setup lang="ts">
import { ref, computed } from 'vue'
import { api } from '@/api/client'
import ModalDialog from './ModalDialog.vue'

const emit = defineEmits<{ close: []; created: [] }>()

const name = ref('')
const partitions = ref('3')
const replicationFactor = ref('3')
const retentionMs = ref('604800000') // 7일 (브로커 기본값과 동일)
const error = ref('')
const submitting = ref(false)

const summary = computed(() =>
  name.value
    ? `'${name.value}' 토픽을 파티션 ${partitions.value}, 복제 팩터 ${replicationFactor.value}으로 생성합니다`
    : '',
)

async function submit() {
  error.value = ''
  submitting.value = true
  try {
    await api('/ops/topics', {
      method: 'POST',
      body: JSON.stringify({
        name: name.value,
        partitions: Number(partitions.value),
        replicationFactor: Number(replicationFactor.value),
        configs: retentionMs.value ? { 'retention.ms': String(retentionMs.value) } : undefined,
      }),
    })
    emit('created')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '생성 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog title="토픽 생성" @close="emit('close')">
    <form id="topic-create-form" @submit.prevent="submit">
      <label>토픽명 <input name="name" v-model="name" required /></label>
      <label>파티션 수 <input name="partitions" v-model="partitions" type="number" min="1" required /></label>
      <label>복제 팩터 <input name="replicationFactor" v-model="replicationFactor" type="number" min="1" required /></label>
      <label>retention.ms (기본 604800000 = 7일) <input name="retentionMs" v-model="retentionMs" type="number" min="1" /></label>
      <p v-if="summary" class="summary">{{ summary }}</p>
      <p v-if="error" class="error">{{ error }}</p>
    </form>
    <template #footer>
      <button type="button" class="btn" @click="emit('close')">취소</button>
      <button type="submit" form="topic-create-form" :disabled="submitting">생성</button>
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
.summary {
  background: var(--surface-2, #f4f6f8);
  border-radius: 6px;
  padding: 0.5rem 0.75rem;
  margin: 0;
  font-size: 0.85rem;
}
.error {
  color: var(--crit, #c0392b);
  margin: 0;
}
</style>
