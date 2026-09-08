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
    await api(`/ops/kafka-apps/${props.name}`, { method: 'DELETE' })
    emit('deleted')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '삭제 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog title="Kafka 계정 삭제" @close="emit('close')">
    <p>
      <strong>'{{ name }}'</strong> 계정과 이 계정의 모든 토픽 권한(ACL)이 함께 삭제됩니다.
      이 계정으로 접속 중인 앱은 즉시 끊깁니다. 되돌릴 수 없습니다.
    </p>
    <label>
      계속하려면 앱 이름을 입력하세요
      <input v-model="confirmText" :placeholder="name" />
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
.error { color: var(--crit, #c0392b); margin: 0.5rem 0 0; }
</style>
