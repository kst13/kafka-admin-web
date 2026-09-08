<script setup lang="ts">
import { ref } from 'vue'
import { api } from '@/api/client'
import type { PasswordResponse } from '@/lib/kafkaApps'
import ModalDialog from './ModalDialog.vue'
import PasswordReveal from './PasswordReveal.vue'

const props = defineProps<{ name: string }>()
const emit = defineEmits<{ close: [] }>()

const result = ref<PasswordResponse | null>(null)
const error = ref('')
const submitting = ref(false)

async function reset() {
  error.value = ''
  submitting.value = true
  try {
    result.value = await api<PasswordResponse>(`/ops/kafka-apps/${props.name}/password`, { method: 'POST' })
  } catch (e) {
    error.value = e instanceof Error ? e.message : '재발급 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog title="비밀번호 재발급" @close="emit('close')">
    <PasswordReveal v-if="result" :name="result.name" :password="result.password" />
    <template v-else>
      <p>
        <strong>'{{ name }}'</strong> 의 비밀번호를 새로 만듭니다. 기존 비밀번호로 접속 중인 앱은
        재접속 시 인증에 실패하므로, 새 비밀번호를 앱 설정에 반영한 뒤 재기동해야 합니다.
      </p>
      <p v-if="error" class="error">{{ error }}</p>
    </template>
    <template #footer>
      <button v-if="result" type="button" class="btn primary" @click="emit('close')">닫기</button>
      <template v-else>
        <button type="button" class="btn" @click="emit('close')">취소</button>
        <button type="button" class="btn danger" :disabled="submitting" @click="reset">재발급</button>
      </template>
    </template>
  </ModalDialog>
</template>

<style scoped>
.error { color: var(--crit, #c0392b); margin: 0.5rem 0 0; }
</style>
