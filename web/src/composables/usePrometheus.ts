import { ref, computed } from 'vue'
import { api } from '@/api/client'
import type { PrometheusStatus } from '@/lib/metrics'

// 모듈 스코프 싱글턴: App 진입·로그인 성공 시 로드. configured=false 면 Prometheus 섹션·화면을 숨긴다.
const status = ref<PrometheusStatus | null>(null)
// 동시에 여러 곳(App.vue, 각 화면)에서 load()/ready() 를 호출해도 요청은 한 번만 나가도록 진행 중 요청을 공유한다.
let inflight: Promise<void> | null = null

export function usePrometheus() {
  async function load() {
    if (inflight) return inflight
    inflight = (async () => {
      try {
        status.value = await api<PrometheusStatus>('/prometheus/status')
      } catch {
        status.value = { configured: false, url: '', healthy: false }
      } finally {
        inflight = null
      }
    })()
    return inflight
  }
  // status 가 아직 없으면(App.vue 의 loadPrometheus() 가 끝나기 전이면) 로드가 끝날 때까지 기다린다.
  // 화면 onMounted 가 App.vue 의 onMounted 보다 먼저 실행되는 레이스를 막기 위함.
  function ready(): Promise<void> {
    if (status.value !== null) return Promise.resolve()
    return load()
  }
  function setStatus(s: PrometheusStatus | null) { status.value = s }
  const configured = computed(() => status.value?.configured === true)
  const healthy = computed(() => status.value?.healthy === true)
  return { status, configured, healthy, load, ready, setStatus }
}
