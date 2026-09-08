import { ref, computed } from 'vue'
import { api } from '@/api/client'
import type { SchemaRegistryStatus } from '@/lib/schemas'

// 모듈 스코프 싱글턴: App 진입 시 1회 로드. configured=false 면 메뉴·토픽 섹션을 숨긴다.
const status = ref<SchemaRegistryStatus | null>(null)

export function useSchemaRegistry() {
  async function load() {
    try {
      status.value = await api<SchemaRegistryStatus>('/schemas/status')
    } catch {
      status.value = { configured: false, urls: [], globalCompatibility: null }
    }
  }
  function setStatus(s: SchemaRegistryStatus | null) { status.value = s }
  const configured = computed(() => status.value?.configured === true)
  const globalCompatibility = computed(() => status.value?.globalCompatibility ?? null)
  return { status, configured, globalCompatibility, load, setStatus }
}
