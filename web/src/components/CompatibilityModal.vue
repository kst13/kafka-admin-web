<script setup lang="ts">
import { ref, computed } from 'vue'
import { api } from '@/api/client'
import { COMPATIBILITY_LEVELS, type SchemaRegistryStatus, type SubjectDetail } from '@/lib/schemas'
import ModalDialog from './ModalDialog.vue'

// subject 가 없으면 전역 호환성, 있으면 서브젝트 호환성. 서브젝트 모드는 "전역 설정 따르기"(빈 값 → null 전송)를 추가로 제공한다.
const props = defineProps<{ subject?: string; current: string; source?: 'SUBJECT' | 'GLOBAL' }>()
const emit = defineEmits<{ close: []; saved: [payload: SubjectDetail | SchemaRegistryStatus] }>()

const isSubject = computed(() => !!props.subject)
const level = ref<string>(isSubject.value && props.source === 'GLOBAL' ? '' : props.current)
const error = ref('')
const submitting = ref(false)
const selected = computed(() => COMPATIBILITY_LEVELS.find((l) => l.value === level.value))

async function save() {
  error.value = ''
  submitting.value = true
  try {
    const payload = isSubject.value
      ? await api<SubjectDetail>(`/ops/schemas/subjects/${props.subject}/config`, {
          method: 'PUT', body: JSON.stringify({ compatibility: level.value || null }),
        })
      : await api<SchemaRegistryStatus>('/ops/schemas/config', {
          method: 'PUT', body: JSON.stringify({ compatibility: level.value }),
        })
    emit('saved', payload)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '변경 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog :title="isSubject ? `호환성 변경 — ${subject}` : '전역 호환성 변경'" @close="emit('close')">
    <div class="form">
      <label>
        호환성 모드
        <select name="compatibility" v-model="level">
          <option v-if="isSubject" value="">전역 설정 따르기</option>
          <option v-for="l in COMPATIBILITY_LEVELS" :key="l.value" :value="l.value">{{ l.label }}</option>
        </select>
      </label>
      <p class="desc">{{ selected ? selected.description : '서브젝트 단위 설정을 지우고 전역 호환성을 상속합니다.' }}</p>
      <p v-if="level === 'NONE'" class="warn-none">
        NONE 은 호환성 검사를 하지 않습니다. 기존 데이터를 읽는 컨슈머가 깨질 수 있습니다.
      </p>
      <p v-if="error" class="error">{{ error }}</p>
    </div>
    <template #footer>
      <button type="button" class="btn" @click="emit('close')">취소</button>
      <button type="button" class="btn primary" :disabled="submitting" @click="save">{{ submitting ? '적용 중…' : '적용' }}</button>
    </template>
  </ModalDialog>
</template>

<style scoped>
.form { display: flex; flex-direction: column; gap: 0.6rem; }
label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
.desc { margin: 0; color: var(--ink-soft); font-size: 0.85rem; }
.warn-none { margin: 0; color: var(--crit); }
.error { color: var(--crit); margin: 0; }
</style>
