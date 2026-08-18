<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { api } from '@/api/client'
import ModalDialog from './ModalDialog.vue'

// Kafka 에는 "그룹 생성" API 가 없어, 선택한 토픽의 전 파티션에 시작 위치 오프셋을 그룹 이름으로
// 미리 커밋하는 방식으로 등록한다. 등록되면 그룹은 Empty(멤버 0) 상태로 목록에 나타난다.
export interface RegisteredGroup { groupId: string; state: string; memberCount: number }
interface TopicSummary { name: string; partitionCount: number; replicationFactor: number }

const emit = defineEmits<{ close: []; registered: [group: RegisteredGroup] }>()

const groupId = ref('')
const topics = ref<TopicSummary[]>([])
const selected = ref<string[]>([])
const startFrom = ref<'earliest' | 'latest'>('latest')
const error = ref('')
const submitting = ref(false)

onMounted(async () => {
  try {
    topics.value = await api<TopicSummary[]>('/topics')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '토픽 목록 조회 실패'
  }
})

const summary = computed(() => {
  if (!groupId.value || selected.value.length === 0) return ''
  const where = startFrom.value === 'earliest' ? '처음(earliest)부터' : '지금 이후(latest)부터'
  return `'${groupId.value}' 그룹을 ${selected.value.join(', ')} 의 ${where} 읽도록 등록합니다`
})

async function submit() {
  error.value = ''
  if (selected.value.length === 0) {
    error.value = '토픽을 1개 이상 선택하세요'
    return
  }
  submitting.value = true
  try {
    await api('/ops/groups', {
      method: 'POST',
      body: JSON.stringify({
        groupId: groupId.value,
        topics: selected.value,
        startFrom: startFrom.value,
      }),
    })
    emit('registered', { groupId: groupId.value, state: 'Empty', memberCount: 0 })
  } catch (e) {
    error.value = e instanceof Error ? e.message : '등록 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog title="컨슈머 그룹 등록" @close="emit('close')">
    <form id="group-register-form" @submit.prevent="submit">
      <label>그룹명 (group.id) <input name="groupId" v-model="groupId" required /></label>
      <fieldset>
        <legend>구독할 토픽</legend>
        <p v-if="topics.length === 0" class="hint">토픽이 없습니다</p>
        <label v-for="t in topics" :key="t.name" class="check">
          <input type="checkbox" :value="t.name" v-model="selected" />
          {{ t.name }} <span class="hint">(파티션 {{ t.partitionCount }})</span>
        </label>
      </fieldset>
      <fieldset>
        <legend>시작 위치</legend>
        <label class="check">
          <input type="radio" name="startFrom" value="latest" v-model="startFrom" />
          latest — 지금 이후 메시지부터 (컨슈머 기본값과 동일)
        </label>
        <label class="check">
          <input type="radio" name="startFrom" value="earliest" v-model="startFrom" />
          earliest — 보관 중인 처음 메시지부터
        </label>
      </fieldset>
      <p v-if="summary" class="summary">{{ summary }}</p>
      <p class="hint">
        등록 후 그룹은 멤버 0명(Empty)으로 보이며, 앱이 같은 group.id 로 붙으면 이 위치부터 읽습니다.
      </p>
      <p v-if="error" class="error">{{ error }}</p>
    </form>
    <template #footer>
      <button type="button" class="btn" @click="emit('close')">취소</button>
      <button type="submit" form="group-register-form" :disabled="submitting">등록</button>
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
fieldset {
  border: 1px solid var(--line, #d8dee4);
  border-radius: 6px;
  padding: 0.5rem 0.75rem;
  display: flex;
  flex-direction: column;
  gap: 0.35rem;
  max-height: 200px;
  overflow-y: auto;
}
legend {
  font-size: 0.8rem;
  color: var(--ink-soft, #5c6b7a);
  padding: 0 0.25rem;
}
label.check {
  flex-direction: row;
  align-items: center;
  gap: 0.5rem;
}
.summary {
  background: var(--surface-2, #f4f6f8);
  border-radius: 6px;
  padding: 0.5rem 0.75rem;
  margin: 0;
  font-size: 0.85rem;
}
.hint {
  margin: 0;
  font-size: 0.8rem;
  color: var(--ink-soft, #5c6b7a);
}
.error {
  color: var(--crit, #c0392b);
  margin: 0;
}
</style>
