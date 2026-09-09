import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const configured = ref(true)
// 기본값: 즉시 resolve. 레이스 컨디션 테스트에서만 컨트롤 가능한 Promise 로 교체한다.
let readyImpl: () => Promise<void> = () => Promise.resolve()
vi.mock('@/composables/usePrometheus', () => ({
  usePrometheus: () => ({ configured, ready: () => readyImpl() }),
}))

import { api } from '@/api/client'
import ClusterView from '../ClusterView.vue'

const cluster = { clusterId: 'c1', controllerId: 1, brokers: [
  { id: 1, host: '10.0.0.1', port: 9094 }, { id: 2, host: '10.0.0.2', port: 9094 },
] }
const monitor = { lastCollectedAt: null, consecutiveFailures: 0, certs: [],
  prometheusLastCollectedAt: '2026-09-09T00:00:00Z', prometheusConsecutiveFailures: 0 }
const health = {
  configured: true, asOf: '2026-09-09T00:00:00Z', activeControllers: 1, offlinePartitions: 0, underReplicated: 1,
  underMinIsr: 0, uncleanElectionsLastHour: 0, uncleanElectionsTotal: 0, activeBrokers: 2, fencedBrokers: 0,
  brokers: [
    { id: 1, host: '10.0.0.1', scraped: true, bytesInPerSec: 1048576, bytesOutPerSec: 524288, messagesInPerSec: 10,
      p99ProduceMs: 12, p99FetchMs: 1500, handlerIdlePct: 95.5, networkIdlePct: 99, heapUsedPct: 40.2, cpuPct: 5,
      leaderCount: 10, partitionCount: 30, underReplicated: 0 },
    { id: 2, host: '10.0.0.2', scraped: false, bytesInPerSec: 0, bytesOutPerSec: 0, messagesInPerSec: 0,
      p99ProduceMs: 0, p99FetchMs: 0, handlerIdlePct: 0, networkIdlePct: 0, heapUsedPct: 0, cpuPct: 0,
      leaderCount: 0, partitionCount: 0, underReplicated: 0 },
  ],
}

function mockApi(healthResult: unknown = health) {
  vi.mocked(api).mockImplementation((url: string) => {
    if (url === '/cluster') return Promise.resolve(cluster)
    if (url === '/monitor/status') return Promise.resolve(monitor)
    if (url === '/cluster/health') return healthResult instanceof Error ? Promise.reject(healthResult) : Promise.resolve(healthResult)
    if (url.startsWith('/metrics')) return Promise.resolve([])
    if (url === '/monitor/disk') return Promise.resolve({ thresholdPct: 80, brokers: [] })
    return Promise.reject(new Error(`unexpected url: ${url}`))
  })
}

const mountOpts = { global: { stubs: { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' }, TrendChart: true } } }

describe('ClusterView (Prometheus)', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    configured.value = true
    readyImpl = () => Promise.resolve()
  })

  it('배지 6개와 브로커 지표 열을 보여주고 브로커 id 는 상세 링크다', async () => {
    mockApi()
    const w = mount(ClusterView, mountOpts)
    await flushPromises()

    const badges = w.findAll('.health-badges .badge')
    expect(badges).toHaveLength(6)
    expect(badges[2]?.classes()).toContain('warn') // 복제 부족 1
    expect(badges[0]?.classes()).toContain('ok')

    const headers = w.findAll('thead th').map((th) => th.text())
    expect(headers).toEqual(expect.arrayContaining(['유입', '유출', 'Produce p99', 'Fetch p99', '핸들러 유휴', '힙']))
    const rows = w.findAll('tbody tr')
    expect(rows[0]?.text()).toContain('1.0 MB/s')
    expect(rows[0]?.text()).toContain('512.0 KB/s')
    expect(rows[0]?.text()).toContain('12 ms')
    expect(rows[0]?.text()).toContain('1.5 s')
    expect(rows[0]?.text()).toContain('95.5%')
    expect(rows[0]?.text()).toContain('40.2%')
    expect(rows[0]?.find('a').attributes('href')).toBe('/brokers/1')
    // scraped=false 는 —
    expect(rows[1]?.text()).toContain('—')
    expect(rows[1]?.find('a').attributes('href')).toBe('/brokers/2')
    expect(w.text()).toContain('Prometheus 마지막 수집')
  })

  it('Prometheus 접속 불가면 배지 대신 한 줄 문구를 보여주고 표는 유지된다', async () => {
    mockApi(new Error('Prometheus 접속 불가'))
    const w = mount(ClusterView, mountOpts)
    await flushPromises()
    expect(w.find('.health-badges').exists()).toBe(false)
    expect(w.find('.prom-error').text()).toContain('Prometheus 접속 불가')
    expect(w.findAll('tbody tr')).toHaveLength(2)
    expect(w.findAll('tbody tr')[0]?.find('a').attributes('href')).toBe('/brokers/1')
  })

  it('미설정이면 배지·지표 열·링크 없이 기존 화면 그대로다', async () => {
    configured.value = false
    mockApi()
    const w = mount(ClusterView, mountOpts)
    await flushPromises()
    expect(w.find('.health-badges').exists()).toBe(false)
    expect(w.findAll('thead th').map((th) => th.text())).not.toContain('유입')
    expect(w.findAll('tbody tr')[0]?.find('a').exists()).toBe(false)
    expect(vi.mocked(api).mock.calls.map((c) => c[0])).not.toContain('/cluster/health')
  })

  it('App.vue 의 Prometheus 상태 로드가 늦게 끝나도(레이스) 배지가 표시된다', async () => {
    mockApi()
    // App.vue 의 loadPrometheus() 가 아직 끝나지 않은 상태를 재현: configured 가 false 로 시작하고,
    // ready() 가 resolve 돼야 true 로 바뀐다.
    configured.value = false
    let resolveReady!: () => void
    readyImpl = () => new Promise<void>((resolve) => { resolveReady = resolve })

    const w = mount(ClusterView, mountOpts)
    await flushPromises()
    // ready() 가 아직 진행 중이므로 배지는 없어야 한다
    expect(w.find('.health-badges').exists()).toBe(false)

    configured.value = true
    resolveReady()
    await flushPromises()

    expect(w.findAll('.health-badges .badge')).toHaveLength(6)
    expect(vi.mocked(api).mock.calls.map((c) => c[0])).toContain('/cluster/health')
  })
})
