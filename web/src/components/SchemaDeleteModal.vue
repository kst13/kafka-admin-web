<script setup lang="ts">
import { ref, computed } from 'vue'
import { api } from '@/api/client'
import ModalDialog from './ModalDialog.vue'

const props = defineProps<{ subject: string }>()
const emit = defineEmits<{ close: []; deleted: [] }>()

const confirmText = ref('')
const error = ref('')
const submitting = ref(false)
const canDelete = computed(() => confirmText.value === props.subject)

async function remove() {
  error.value = ''
  submitting.value = true
  try {
    await api(`/ops/schemas/subjects/${props.subject}`, { method: 'DELETE' })
    emit('deleted')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '삭제 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog title="서브젝트 삭제" @close="emit('close')">
    <p>
      <strong>'{{ subject }}'</strong> 의 모든 버전을 삭제합니다. soft delete 라 Schema Registry 에서 복구할 수 있지만,
      이 서브젝트를 쓰는 프로듀서·컨슈머는 즉시 영향을 받습니다.
    </p>
    <label>
      계속하려면 서브젝트 이름을 입력하세요
      <input v-model="confirmText" :placeholder="subject" />
    </label>
    <p v-if="error" class="error">{{ error }}</p>
    <template #footer>
      <button type="button" class="btn" @click="emit('close')">취소</button>
      <button type="button" class="btn danger" :disabled="!canDelete || submitting" @click="remove">삭제</button>
    </template>
  </ModalDialog>
</template>

<style scoped>
label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
.error { color: var(--crit); margin: 0.5rem 0 0; }
</style>
