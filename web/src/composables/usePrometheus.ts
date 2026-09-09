import { ref, computed } from 'vue'
import { api } from '@/api/client'
import type { PrometheusStatus } from '@/lib/metrics'

// 모듈 스코프 싱글턴: App 진입·로그인 성공 시 로드. configured=false 면 Prometheus 섹션·화면을 숨긴다.
const status = ref<PrometheusStatus | null>(null)

export function usePrometheus() {
  async function load() {
    try {
      status.value = await api<PrometheusStatus>('/prometheus/status')
    } catch {
      status.value = { configured: false, url: '', healthy: false }
    }
  }
  function setStatus(s: PrometheusStatus | null) { status.value = s }
  const configured = computed(() => status.value?.configured === true)
  const healthy = computed(() => status.value?.healthy === true)
  return { status, configured, healthy, load, setStatus }
}
