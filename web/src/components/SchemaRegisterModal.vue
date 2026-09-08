<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { api } from '@/api/client'
import { SCHEMA_TYPES, type CompatibilityResult, type RegisteredSchema, type SchemaType } from '@/lib/schemas'
import ModalDialog from './ModalDialog.vue'

// topic·kind 가 모두 주어지면 고정(상세 화면·토픽 섹션에서 "새 버전 등록"). 등록은 호환성 검사를 통과해야 활성화된다.
const props = defineProps<{ topic?: string; kind?: 'key' | 'value' }>()
const emit = defineEmits<{ close: []; registered: [r: RegisteredSchema] }>()

interface TopicSummary { name: string; partitionCount: number; replicationFactor: number }

const fixed = computed(() => !!props.topic && !!props.kind)
const topics = ref<TopicSummary[]>([])
const topic = ref(props.topic ?? '')
const kind = ref<'key' | 'value'>(props.kind ?? 'value')
const schemaType = ref<SchemaType>('AVRO')
const schema = ref('')
const check = ref<CompatibilityResult | null>(null)
const checking = ref(false)
const submitting = ref(false)
const error = ref('')
const result = ref<RegisteredSchema | null>(null)

const subject = computed(() => (topic.value ? `${topic.value}-${kind.value}` : ''))
const canCheck = computed(() => !!topic.value && schema.value.trim().length > 0 && !checking.value)
const canRegister = computed(() => check.value?.compatible === true && !submitting.value && !result.value)

// 입력이 바뀌면 이전 검사 결과는 무효
watch([topic, kind, schemaType, schema], () => { check.value = null; error.value = '' })

onMounted(async () => {
  if (fixed.value) return
  try {
    topics.value = await api<TopicSummary[]>('/topics')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '토픽 목록 조회 실패'
  }
})

function body() {
  return JSON.stringify({ topic: topic.value, kind: kind.value, schemaType: schemaType.value, schema: schema.value })
}

async function runCheck() {
  if (!canCheck.value) return
  error.value = ''
  checking.value = true
  const requested = body()
  try {
    const res = await api<CompatibilityResult>('/schemas/compatibility', { method: 'POST', body: requested })
    // 응답이 오는 사이 입력이 바뀌었으면(검사 대상이 달라졌으므로) 결과를 반영하지 않는다
    if (requested === body()) check.value = res
  } catch (e) {
    if (requested === body()) error.value = e instanceof Error ? e.message : '호환성 검사 실패'
  } finally {
    checking.value = false
  }
}

async function register() {
  if (!canRegister.value) return
  error.value = ''
  submitting.value = true
  try {
    result.value = await api<RegisteredSchema>('/schemas/register', { method: 'POST', body: body() })
    emit('registered', result.value)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '등록 실패'
    check.value = null // 등록 시점 재검사 실패 등 — 다시 검사하도록
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog :title="fixed ? '새 버전 등록' : '스키마 등록'" @close="emit('close')">
    <div class="form">
      <template v-if="!fixed">
        <label>
          토픽
          <select name="topic" v-model="topic" :disabled="checking || submitting">
            <option value="">— 선택 —</option>
            <option v-for="t in topics" :key="t.name" :value="t.name">{{ t.name }}</option>
          </select>
        </label>
        <fieldset class="kind">
          <legend>종류</legend>
          <label><input type="radio" name="kind" value="key" v-model="kind" :disabled="checking || submitting" /> key</label>
          <label><input type="radio" name="kind" value="value" v-model="kind" :disabled="checking || submitting" /> value</label>
        </fieldset>
      </template>
      <p v-else>서브젝트 <strong>{{ subject }}</strong> 에 새 버전을 등록합니다.</p>
      <label>
        형식
        <select name="schemaType" v-model="schemaType" :disabled="checking || submitting">
          <option v-for="t in SCHEMA_TYPES" :key="t" :value="t">{{ t }}</option>
        </select>
      </label>
      <label>
        스키마 본문 (최대 1 MB)
        <textarea name="schema" v-model="schema" rows="12" spellcheck="false" :disabled="checking || submitting" />
      </label>
      <div class="check-row">
        <button type="button" class="btn check" :disabled="!canCheck" @click="runCheck">
          {{ checking ? '검사 중…' : '호환성 검사' }}
        </button>
        <span v-if="subject" class="subject-hint">→ {{ subject }}</span>
      </div>
      <p v-if="check && check.compatible" class="check-ok">호환성 검사 통과. 등록할 수 있습니다.</p>
      <div v-else-if="check" class="check-fail">
        <p>호환성 검사에 실패했습니다.</p>
        <ul><li v-for="(m, i) in check.messages" :key="i">{{ m }}</li></ul>
      </div>
      <p v-if="result" class="registered">등록됨: {{ result.subject }} v{{ result.version }} (id {{ result.id }})</p>
      <p v-if="error" class="error">{{ error }}</p>
    </div>
    <template #footer>
      <button v-if="result" type="button" class="btn primary" @click="emit('close')">닫기</button>
      <template v-else>
        <button type="button" class="btn" @click="emit('close')">취소</button>
        <button type="button" class="btn primary register" :disabled="!canRegister" @click="register">
          {{ submitting ? '등록 중…' : '등록' }}
        </button>
      </template>
    </template>
  </ModalDialog>
</template>

<style scoped>
.form { display: flex; flex-direction: column; gap: 0.75rem; }
label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
.kind { display: flex; gap: 1rem; border: 1px solid var(--line); border-radius: 6px; padding: 0.4rem 0.75rem; }
.kind label { flex-direction: row; align-items: center; gap: 0.35rem; }
textarea { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.82rem; }
.check-row { display: flex; align-items: center; gap: 0.75rem; }
.subject-hint { color: var(--ink-soft); font-size: 0.85rem; }
.check-ok { color: var(--accent); margin: 0; }
.check-fail { color: var(--crit); margin: 0; }
.check-fail ul { margin: 0.25rem 0 0 1.2rem; font-family: ui-monospace, monospace; font-size: 0.8rem; }
.registered { margin: 0; padding: 0.5rem 0.75rem; background: var(--surface-2); border-radius: 6px; }
.error { color: var(--crit); margin: 0; }
</style>
