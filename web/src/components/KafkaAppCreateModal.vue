<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { api } from '@/api/client'
import { isValidAppName, type KafkaAppDetail, type PasswordResponse } from '@/lib/kafkaApps'
import ModalDialog from './ModalDialog.vue'
import PasswordReveal from './PasswordReveal.vue'

// registerName 이 있으면 "미등록 계정 등록" 모드: 이름 고정, 비밀번호 단계 없음.
const props = defineProps<{ registerName?: string }>()
const emit = defineEmits<{ close: []; created: [name: string]; registered: [name: string] }>()

interface SiteUser { id: number; username: string; role: string }

const isRegister = computed(() => !!props.registerName)
const name = ref(props.registerName ?? '')
const owner = ref('')
const description = ref('')
const siteUsers = ref<SiteUser[]>([])
const error = ref('')
const submitting = ref(false)
const result = ref<PasswordResponse | null>(null)

const canSubmit = computed(() => isValidAppName(name.value) && !submitting.value)

onMounted(async () => {
  try {
    siteUsers.value = await api<SiteUser[]>('/ops/users')
  } catch {
    siteUsers.value = [] // 담당자 목록은 부가 정보 — 실패해도 생성은 가능
  }
})

async function submit() {
  error.value = ''
  submitting.value = true
  try {
    if (isRegister.value) {
      await api<KafkaAppDetail>(`/ops/kafka-apps/${name.value}/register`, {
        method: 'POST',
        body: JSON.stringify({ owner: owner.value, description: description.value }),
      })
      emit('registered', name.value)
    } else {
      result.value = await api<PasswordResponse>('/ops/kafka-apps', {
        method: 'POST',
        body: JSON.stringify({ name: name.value, owner: owner.value, description: description.value }),
      })
      emit('created', name.value)
    }
  } catch (e) {
    error.value = e instanceof Error ? e.message : '요청 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog :title="isRegister ? '미등록 계정 등록' : '앱 계정 추가'" @close="emit('close')">
    <PasswordReveal v-if="result" :name="result.name" :password="result.password" />
    <form v-else class="form" @submit.prevent="submit">
      <label v-if="!isRegister">
        앱 이름 (SCRAM 사용자명)
        <input name="name" v-model="name" placeholder="order-api" autocomplete="off" />
        <small>영문·숫자·'.', '_', '-' 만, 64자 이하</small>
      </label>
      <p v-else>브로커에 있는 <strong>{{ name }}</strong> 계정에 담당자·설명을 붙입니다. 비밀번호는 바뀌지 않습니다.</p>
      <label>
        담당 개발자 (선택)
        <select name="owner" v-model="owner">
          <option value="">— 없음 —</option>
          <option v-for="u in siteUsers" :key="u.id" :value="u.username">{{ u.username }}</option>
        </select>
      </label>
      <label>
        설명 (선택)
        <input name="description" v-model="description" placeholder="주문 서비스" />
      </label>
      <p v-if="error" class="error">{{ error }}</p>
    </form>
    <template #footer>
      <button v-if="result" type="button" class="btn primary" @click="emit('close')">닫기</button>
      <template v-else>
        <button type="button" class="btn" @click="emit('close')">취소</button>
        <button type="button" class="btn primary" :disabled="!canSubmit" @click="submit">
          {{ submitting ? '처리 중…' : isRegister ? '등록' : '생성' }}
        </button>
      </template>
    </template>
  </ModalDialog>
</template>

<style scoped>
.form { display: flex; flex-direction: column; gap: 0.75rem; }
label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
small { color: var(--ink-soft); }
.error { color: var(--crit, #c0392b); margin: 0; }
</style>
