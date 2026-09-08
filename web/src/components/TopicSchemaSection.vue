<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
import { api } from '@/api/client'
import { useSchemaRegistry } from '@/composables/useSchemaRegistry'
import { compatibilityLabel, type SubjectSummary, type TopicSchemas } from '@/lib/schemas'
import SchemaRegisterModal from './SchemaRegisterModal.vue'

// 토픽 상세의 "스키마" 섹션. Registry 미설정이면 아무것도 그리지 않는다(호출도 안 함).
const props = defineProps<{ topic: string }>()
const { configured } = useSchemaRegistry()

const data = ref<TopicSchemas | null>(null)
const error = ref('')
const registerKind = ref<'key' | 'value' | null>(null)

async function load() {
  if (!configured.value) return
  try {
    data.value = await api<TopicSchemas>(`/schemas/topics/${encodeURIComponent(props.topic)}`)
    error.value = ''
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(load)
watch(configured, (on) => { if (on && !data.value) load() })

function cell(s: SubjectSummary | null) {
  return s ? `${s.schemaType} · v${s.latestVersion} · ${compatibilityLabel(s)}` : '없음'
}
function onRegistered() { registerKind.value = null; load() }
</script>

<template>
  <section v-if="configured" class="topic-schemas">
    <h2>스키마</h2>
    <p v-if="error" class="error">{{ error }}</p>
    <table v-else-if="data">
      <thead><tr><th>종류</th><th>스키마</th><th></th></tr></thead>
      <tbody>
        <tr class="key">
          <td>key</td>
          <td>
            <RouterLink v-if="data.key" :to="`/schemas/${encodeURIComponent(data.key.subject)}`">{{ cell(data.key) }}</RouterLink>
            <span v-else>{{ cell(null) }}</span>
          </td>
          <td><button type="button" class="btn register-key" @click="registerKind = 'key'">{{ data.key ? '새 버전 등록' : '등록' }}</button></td>
        </tr>
        <tr class="value">
          <td>value</td>
          <td>
            <RouterLink v-if="data.value" :to="`/schemas/${encodeURIComponent(data.value.subject)}`">{{ cell(data.value) }}</RouterLink>
            <span v-else>{{ cell(null) }}</span>
          </td>
          <td><button type="button" class="btn register-value" @click="registerKind = 'value'">{{ data.value ? '새 버전 등록' : '등록' }}</button></td>
        </tr>
      </tbody>
    </table>
    <SchemaRegisterModal v-if="registerKind" :topic="topic" :kind="registerKind" @close="registerKind = null" @registered="onRegistered" />
  </section>
</template>

<style scoped>
.error { color: var(--crit); }
</style>
