<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import { formatSchema, compatibilityLabel, type SchemaVersion, type SubjectDetail } from '@/lib/schemas'
import SchemaRegisterModal from '@/components/SchemaRegisterModal.vue'
import CompatibilityModal from '@/components/CompatibilityModal.vue'
import SchemaDeleteModal from '@/components/SchemaDeleteModal.vue'

const route = useRoute()
const router = useRouter()
const { isAdmin } = useSession()
const subject = String(route.params.subject)

const detail = ref<SubjectDetail | null>(null)
const error = ref('')
const selected = ref('')          // 선택한 버전 (문자열, select 바인딩)
const current = ref<SchemaVersion | null>(null)
const compare = ref(false)
const compareSelected = ref('')
const compareVersion = ref<SchemaVersion | null>(null)
const showRegister = ref(false)
const showCompat = ref(false)
const showDelete = ref(false)

const isTopicBound = computed(() => detail.value?.kind === 'key' || detail.value?.kind === 'value')
const latestType = computed(() => detail.value?.versions[0]?.schemaType ?? '')

async function loadVersion(version: string): Promise<SchemaVersion> {
  return api<SchemaVersion>(`/schemas/subjects/${subject}/versions/${version}`)
}

async function load() {
  try {
    detail.value = await api<SubjectDetail>(`/schemas/subjects/${subject}`)
    error.value = ''
    const latest = detail.value.versions[0]
    if (latest) {
      selected.value = String(latest.version)
      current.value = await loadVersion(selected.value)
    }
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(load)

watch(selected, async (v) => {
  if (!v || !detail.value) return
  try { current.value = await loadVersion(v) } catch (e) { error.value = e instanceof Error ? e.message : '조회 실패' }
})
watch(compareSelected, async (v) => {
  if (!v) { compareVersion.value = null; return }
  try { compareVersion.value = await loadVersion(v) } catch (e) { error.value = e instanceof Error ? e.message : '조회 실패' }
})
watch(compare, (on) => { if (!on) { compareSelected.value = ''; compareVersion.value = null } })

function onCompatSaved(payload: unknown) {
  detail.value = payload as SubjectDetail
  showCompat.value = false
}
function onRegistered() { showRegister.value = false; load() }
function onDeleted() { router.push('/schemas') }
</script>

<template>
  <main>
    <p v-if="error && !detail" class="error">{{ error }}</p>
    <template v-if="detail">
      <div class="head-row">
        <h1>
          {{ detail.subject }}
          <span v-if="!isTopicBound" class="badge">기타</span>
        </h1>
        <div class="actions">
          <button v-if="isTopicBound" type="button" class="btn primary register-version" @click="showRegister = true">새 버전 등록</button>
          <button v-if="isAdmin" type="button" class="btn change-compat" @click="showCompat = true">호환성 변경</button>
          <button v-if="isAdmin" type="button" class="btn danger-outline delete-subject" @click="showDelete = true">삭제</button>
        </div>
      </div>
      <dl class="meta">
        <dt>토픽</dt>
        <dd><RouterLink v-if="detail.topic" :to="`/topics/${detail.topic}`">{{ detail.topic }}</RouterLink><span v-else>—</span></dd>
        <dt>종류</dt><dd>{{ detail.kind }}</dd>
        <dt>형식</dt><dd>{{ latestType || '—' }}</dd>
        <dt>호환성</dt><dd>{{ compatibilityLabel(detail) }}</dd>
      </dl>
      <p v-if="error" class="error">{{ error }}</p>

      <div class="version-row">
        <label>
          버전
          <select name="version" v-model="selected">
            <option v-for="v in detail.versions" :key="v.version" :value="String(v.version)">v{{ v.version }} (id {{ v.id }}, {{ v.schemaType }})</option>
          </select>
        </label>
        <label class="compare-toggle">
          <input type="checkbox" name="compare" v-model="compare" /> 다른 버전과 비교
        </label>
        <label v-if="compare">
          비교 대상
          <select name="compare-version" v-model="compareSelected">
            <option value="">— 선택 —</option>
            <option v-for="v in detail.versions" :key="v.version" :value="String(v.version)">v{{ v.version }}</option>
          </select>
        </label>
      </div>
      <div class="panes" :class="{ split: compare && compareVersion }">
        <pre v-if="current" class="schema">{{ formatSchema(current.schemaType, current.schema) }}</pre>
        <pre v-if="compare && compareVersion" class="schema schema-compare">{{ formatSchema(compareVersion.schemaType, compareVersion.schema) }}</pre>
      </div>
      <p v-if="current && current.references.length" class="hint">
        참조: <span v-for="r in current.references" :key="r.name">{{ r.name }} → {{ r.subject }} v{{ r.version }}; </span>
      </p>

      <SchemaRegisterModal v-if="showRegister && detail.topic && isTopicBound" :topic="detail.topic"
                           :kind="detail.kind === 'key' ? 'key' : 'value'" @close="showRegister = false" @registered="onRegistered" />
      <CompatibilityModal v-if="showCompat" :subject="detail.subject" :current="detail.compatibility"
                          :source="detail.compatibilitySource" @close="showCompat = false" @saved="onCompatSaved" />
      <SchemaDeleteModal v-if="showDelete" :subject="detail.subject" @close="showDelete = false" @deleted="onDeleted" />
    </template>
  </main>
</template>

<style scoped>
.head-row { display: flex; justify-content: space-between; align-items: center; }
.actions { display: flex; gap: 0.5rem; }
.meta { display: grid; grid-template-columns: max-content 1fr; gap: 0.3rem 1rem; margin: 0.5rem 0 1rem; }
.meta dt { color: var(--ink-soft); }
.meta dd { margin: 0; }
.badge { margin-left: 0.5rem; padding: 0.1rem 0.4rem; border-radius: 4px; font-size: 0.75rem; vertical-align: middle; background: var(--surface-2); color: var(--ink-soft); }
.version-row { display: flex; gap: 1rem; align-items: flex-end; flex-wrap: wrap; margin-bottom: 0.5rem; }
.version-row label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
.compare-toggle { flex-direction: row !important; align-items: center; gap: 0.35rem !important; }
.panes { display: grid; grid-template-columns: 1fr; gap: 0.75rem; }
.panes.split { grid-template-columns: 1fr 1fr; }
.schema { margin: 0; padding: 0.75rem; background: var(--surface-2); border-radius: 6px; overflow: auto; font-size: 0.8rem; max-height: 60vh; }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.error { color: var(--crit); }
</style>
