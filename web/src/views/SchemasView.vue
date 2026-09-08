<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import { useSchemaRegistry } from '@/composables/useSchemaRegistry'
import { groupByTopic, compatibilityLabel, type SubjectSummary, type SchemaRegistryStatus } from '@/lib/schemas'
import SchemaRegisterModal from '@/components/SchemaRegisterModal.vue'
import CompatibilityModal from '@/components/CompatibilityModal.vue'

const subjects = ref<SubjectSummary[]>([])
const error = ref('')
const showRegister = ref(false)
const showGlobal = ref(false)
const { isAdmin } = useSession()
const { globalCompatibility, setStatus } = useSchemaRegistry()

function onGlobalSaved(s: unknown) { setStatus(s as SchemaRegistryStatus); showGlobal.value = false }

const grouped = computed(() => groupByTopic(subjects.value))

async function load() {
  try {
    subjects.value = await api<SubjectSummary[]>('/schemas/subjects')
    error.value = ''
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(load)

function cell(s: SubjectSummary | null) {
  return s ? `${s.schemaType} · v${s.latestVersion}` : '—'
}
function rowCompat(row: { key: SubjectSummary | null; value: SubjectSummary | null }) {
  const s = row.value ?? row.key
  return s ? compatibilityLabel(s) : '—'
}
</script>

<template>
  <main>
    <div class="head-row">
      <h1>스키마</h1>
      <div class="actions">
        <span class="global-compat">전역 호환성: <strong>{{ globalCompatibility ?? '—' }}</strong></span>
        <button v-if="isAdmin" type="button" class="btn change-global" @click="showGlobal = true">전역 호환성 변경</button>
        <button type="button" class="btn primary register-schema" @click="showRegister = true">스키마 등록</button>
      </div>
    </div>
    <p class="hint">
      Schema Registry 의 서브젝트를 토픽 기준(<code>&lt;토픽&gt;-key</code> / <code>&lt;토픽&gt;-value</code>)으로 보여줍니다.
      새 버전 등록은 호환성 검사를 통과해야 합니다.
      <span v-if="!isAdmin">호환성 변경과 삭제는 ADMIN 만 할 수 있습니다.</span>
    </p>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-else>
      <p v-if="subjects.length === 0" class="hint">등록된 스키마가 없습니다.</p>
      <template v-else>
        <table class="topics">
          <thead><tr><th>토픽</th><th>key 스키마</th><th>value 스키마</th><th>호환성</th></tr></thead>
          <tbody>
            <tr v-for="row in grouped.topics" :key="row.topic">
              <td><RouterLink :to="`/topics/${row.topic}`">{{ row.topic }}</RouterLink></td>
              <td>
                <RouterLink v-if="row.key" :to="`/schemas/${row.key.subject}`">{{ cell(row.key) }}</RouterLink>
                <span v-else>—</span>
              </td>
              <td>
                <RouterLink v-if="row.value" :to="`/schemas/${row.value.subject}`">{{ cell(row.value) }}</RouterLink>
                <span v-else>—</span>
              </td>
              <td>{{ rowCompat(row) }}</td>
            </tr>
          </tbody>
        </table>
        <template v-if="grouped.others.length > 0">
          <h2>기타 서브젝트</h2>
          <p class="hint">토픽 규칙 밖 이름입니다. 조회만 가능하며 등록은 앱/CLI 에서 합니다.</p>
          <table class="others">
            <thead><tr><th>서브젝트</th><th>형식</th><th>최신 버전</th></tr></thead>
            <tbody>
              <tr v-for="s in grouped.others" :key="s.subject">
                <td><RouterLink :to="`/schemas/${s.subject}`">{{ s.subject }}</RouterLink></td>
                <td>{{ s.schemaType }}</td>
                <td>v{{ s.latestVersion }}</td>
              </tr>
            </tbody>
          </table>
        </template>
      </template>
    </template>
    <SchemaRegisterModal v-if="showRegister" @close="showRegister = false" @registered="load" />
    <CompatibilityModal v-if="showGlobal" :current="globalCompatibility ?? 'BACKWARD'" @close="showGlobal = false" @saved="onGlobalSaved" />
  </main>
</template>

<style scoped>
.head-row { display: flex; justify-content: space-between; align-items: center; }
.actions { display: flex; align-items: center; gap: 0.75rem; }
.global-compat { font-size: 0.9rem; color: var(--ink-soft); }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.error { color: var(--crit); }
</style>
