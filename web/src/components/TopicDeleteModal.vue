<script setup lang="ts">
import { ref, computed } from 'vue'
import { api } from '@/api/client'
import ModalDialog from './ModalDialog.vue'

const props = defineProps<{ name: string }>()
const emit = defineEmits<{ close: []; deleted: [] }>()

const confirmText = ref('')
const error = ref('')
const submitting = ref(false)
const canDelete = computed(() => confirmText.value === props.name)

async function remove() {
  error.value = ''
  submitting.value = true
  try {
    await api(`/ops/topics/${props.name}`, { method: 'DELETE' })
    emit('deleted')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '삭제 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog title="토픽 삭제" @close="emit('close')">
    <p>
      <strong>'{{ name }}'</strong> 토픽과 모든 메시지가 삭제됩니다. 되돌릴 수 없습니다.
    </p>
    <label>
      계속하려면 토픽명을 입력하세요
      <input v-model="confirmText" :placeholder="name" />
    </label>
    <p v-if="error" class="error">{{ error }}</p>
    <template #footer>
      <button type="button" @click="emit('close')">취소</button>
      <button type="button" class="danger" :disabled="!canDelete || submitting" @click="remove">
        삭제
      </button>
    </template>
  </ModalDialog>
</template>

<style scoped>
label {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  font-size: 0.85rem;
}
.danger {
  background: var(--crit, #c0392b);
  color: white;
  border: none;
  border-radius: 6px;
  padding: 0.35rem 0.9rem;
}
.danger:disabled {
  opacity: 0.4;
}
.error {
  color: var(--crit, #c0392b);
  margin: 0.5rem 0 0;
}
</style>
