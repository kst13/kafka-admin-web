import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
vi.mock('@/composables/useSession', () => ({ useSession: () => ({ isAdmin: { value: false } }) }))
const configured = ref(false)
// 기본값: 즉시 resolve. 레이스 컨디션 테스트에서만 컨트롤 가능한 Promise 로 교체한다.
let readyImpl: () => Promise<void> = () => Promise.resolve()
vi.mock('@/composables/usePrometheus', () => ({
  usePrometheus: () => ({ configured, ready: () => readyImpl() }),
}))
vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { name: 't' } }),
  useRouter: () => ({ push: vi.fn() }),
}))
vi.mock('@/components/TopicSchemaSection.vue', () => ({ default: { name: 'TopicSchemaSection', template: '<div />' } }))

import { api } from '@/api/client'
import TopicDetailView from '../TopicDetailView.vue'
import TrendChart from '@/components/TrendChart.vue'
import MetricChart from '@/components/MetricChart.vue'

const detail = {
  name: 't',
  partitions: [
    { partition: 0, leader: 1, replicas: [1], isr: [1] },
    { partition: 1, leader: 1, replicas: [1], isr: [1] },
  ],
  configs: {},
}
const throughput = [
  { partition: 0, endOffset: 120, count: 60, ratePerMin: 1.0 },
  { partition: 1, endOffset: 10, count: 0, ratePerMin: 0.0 },
]
const produced = [
  { sampledAt: '2026-08-20T09:00:00Z', value: 100 },
  { sampledAt: '2026-08-20T09:30:00Z', value: 130 },
  { sampledAt: '2026-08-20T10:00:00Z', value: 160 },
]

function partitionSeries(key: string, values: number[]) {
  return { key, unit: key.includes('LOG_SIZE') ? 'bytes' : 'count', range: '1h', stepSeconds: 30,
    series: values.map((v, i) => ({ name: String(i), points: [{ t: '2026-09-09T00:00:00Z', v: v / 2 }, { t: '2026-09-09T00:00:30Z', v }] })) }
}

function mockFallback(withSamples = true) {
  vi.mocked(api).mockImplementation((url: string) => {
    if (url.startsWith('/topics/t/messages')) return Promise.resolve([])
    if (url.startsWith('/topics/t/throughput')) return Promise.resolve(withSamples ? throughput : [])
    if (url.startsWith('/metrics')) return Promise.resolve(withSamples ? produced : [])
    if (url === '/topics/t') return Promise.resolve(detail)
    return Promise.reject(new Error(`unexpected url: ${url}`))
  })
}

function mockPrometheus() {
  vi.mocked(api).mockImplementation((url: string) => {
    if (url.startsWith('/topics/t/messages')) return Promise.resolve([])
    if (url === '/topics/t') return Promise.resolve(detail)
    if (url.startsWith('/topics/t/series')) {
      const key = new URL(url, 'http://x').searchParams.get('key') ?? ''
      const range = new URL(url, 'http://x').searchParams.get('range') ?? ''
      if (key === 'TOPIC_RETAINED_BY_PARTITION') return Promise.resolve(partitionSeries(key, [1500, 20]))
      if (key === 'TOPIC_LOG_SIZE_BY_PARTITION') return Promise.resolve(partitionSeries(key, [2048, 1024 * 1024]))
      return Promise.resolve({ key, unit: key === 'TOPIC_BYTES_IN' ? 'bytes/s' : 'msg/s', range, stepSeconds: 30,
        series: [{ name: key, points: [{ t: '2026-09-09T00:00:00Z', v: 3 }] }] })
    }
    return Promise.reject(new Error(`unexpected url: ${url}`))
  })
}

describe('TopicDetailView 폴백(수집기) 모드', () => {
  beforeEach(() => { vi.mocked(api).mockReset(); configured.value = false; readyImpl = () => Promise.resolve() })

  it('파티션 표에 endOffset·최근 1시간 유입·분당 속도를 보여주고 추이를 TrendChart 로 그린다', async () => {
    mockFallback()
    const w = mount(TopicDetailView)
    await flushPromises()
    const rows = w.findAll('tbody tr')
    expect(rows[0]?.text()).toContain('120')
    expect(rows[0]?.text()).toContain('60')
    expect(rows[0]?.text()).toContain('1.0')
    const chart = w.findComponent(TrendChart)
    expect(chart.exists()).toBe(true)
    expect((chart.props('points') as { v: number }[]).map((p) => p.v)).toEqual([30, 30])
    expect(w.findComponent(MetricChart).exists()).toBe(false)
    expect(w.find('select.partition-select').exists()).toBe(false)
    expect(vi.mocked(api).mock.calls.map((c) => c[0] as string).some((u) => u.includes('/series'))).toBe(false)
  })

  it('샘플이 없으면 예시 데이터 없이 문구만 보여준다', async () => {
    mockFallback(false)
    const w = mount(TopicDetailView)
    await flushPromises()
    expect(w.text()).not.toContain('예시')
    expect(w.find('.demo').exists()).toBe(false)
    expect(w.findComponent(TrendChart).exists()).toBe(false)
    expect(w.text()).toContain('수집된 유입 샘플이 아직 없습니다')
    expect(w.findAll('tbody tr')[0]?.text()).toContain('—')
  })
})

describe('TopicDetailView Prometheus 모드', () => {
  beforeEach(() => { vi.mocked(api).mockReset(); configured.value = true; readyImpl = () => Promise.resolve() })

  it('파티션 표에 보유 메시지·로그 크기를 보여주고 endOffset 열은 없다', async () => {
    mockPrometheus()
    const w = mount(TopicDetailView)
    await flushPromises()
    const headers = w.findAll('thead th').map((th) => th.text())
    expect(headers).toEqual(expect.arrayContaining(['보유 메시지', '로그 크기']))
    expect(headers).not.toContain('endOffset')
    expect(headers).not.toContain('최근 1시간 유입')
    const rows = w.findAll('table tbody tr')
    expect(rows[0]?.text()).toContain('1,500')
    expect(rows[0]?.text()).toContain('2.0 KB')
    expect(rows[1]?.text()).toContain('20')
    expect(rows[1]?.text()).toContain('1.0 MB')
    expect(w.text()).not.toContain('예시')
    expect(vi.mocked(api).mock.calls.map((c) => c[0] as string).some((u) => u.includes('/throughput'))).toBe(false)
  })

  it('유입 추이는 MetricChart 로 그리고 메시지/바이트 전환과 범위 변경 시 다시 조회한다', async () => {
    mockPrometheus()
    const w = mount(TopicDetailView)
    await flushPromises()
    const chart = w.findComponent(MetricChart)
    expect(chart.exists()).toBe(true)
    expect(chart.props('unit')).toBe('msg/s')
    expect(w.findComponent(TrendChart).exists()).toBe(false)
    const calls = () => vi.mocked(api).mock.calls.map((c) => c[0] as string)
    expect(calls()).toContain('/topics/t/series?key=TOPIC_MESSAGES_IN&range=1h')

    await w.findAll('.metric-tabs button').find((b) => b.text() === '바이트')?.trigger('click')
    await flushPromises()
    expect(calls()).toContain('/topics/t/series?key=TOPIC_BYTES_IN&range=1h')
    expect(w.findComponent(MetricChart).props('unit')).toBe('bytes/s')

    await w.findAll('.range-tabs button').find((b) => b.text() === '7일')?.trigger('click')
    await flushPromises()
    expect(calls()).toContain('/topics/t/series?key=TOPIC_BYTES_IN&range=7d')
  })

  it('Prometheus 조회가 실패하면 파티션 표는 유지되고 차트 자리에 오류 문구', async () => {
    vi.mocked(api).mockImplementation((url: string) => {
      if (url.startsWith('/topics/t/messages')) return Promise.resolve([])
      if (url === '/topics/t') return Promise.resolve(detail)
      if (url.startsWith('/topics/t/series')) return Promise.reject(new Error('Prometheus 접속 불가'))
      return Promise.reject(new Error(`unexpected url: ${url}`))
    })
    const w = mount(TopicDetailView)
    await flushPromises()
    expect(w.findAll('table tbody tr')).toHaveLength(2)
    expect(w.findAll('table tbody tr')[0]?.text()).toContain('—')
    expect(w.text()).toContain('Prometheus 접속 불가')
  })

  it('App.vue 의 Prometheus 상태 로드가 늦게 끝나도(레이스) Prometheus 경로를 탄다', async () => {
    // App.vue 의 loadPrometheus() 가 아직 끝나지 않은 상태를 재현: configured 가 false 로 시작하고,
    // ready() 가 resolve 돼야 true 로 바뀐다.
    configured.value = false
    let resolveReady!: () => void
    readyImpl = () => new Promise<void>((resolve) => { resolveReady = resolve })
    mockPrometheus()

    const w = mount(TopicDetailView)
    await flushPromises()
    // ready() 가 아직 진행 중이므로 Prometheus 경로로 아직 넘어가지 않았어야 한다
    expect(w.findComponent(MetricChart).exists()).toBe(false)
    expect(vi.mocked(api).mock.calls.map((c) => c[0] as string).some((u) => u.includes('/series'))).toBe(false)

    configured.value = true
    resolveReady()
    await flushPromises()

    expect(w.findComponent(MetricChart).exists()).toBe(true)
    expect(vi.mocked(api).mock.calls.map((c) => c[0] as string)).toContain('/topics/t/series?key=TOPIC_MESSAGES_IN&range=1h')
  })

  it('오래된(stale) 유입 조회 응답이 늦게 도착해도 최신 선택(바이트) 결과가 유지된다', async () => {
    let resolveStale!: (v: unknown) => void
    const stale = new Promise((resolve) => { resolveStale = resolve })
    vi.mocked(api).mockImplementation((url: string) => {
      if (url.startsWith('/topics/t/messages')) return Promise.resolve([])
      if (url === '/topics/t') return Promise.resolve(detail)
      if (url.startsWith('/topics/t/series')) {
        const key = new URL(url, 'http://x').searchParams.get('key') ?? ''
        const range = new URL(url, 'http://x').searchParams.get('range') ?? ''
        if (key === 'TOPIC_RETAINED_BY_PARTITION') return Promise.resolve(partitionSeries(key, [1500, 20]))
        if (key === 'TOPIC_LOG_SIZE_BY_PARTITION') return Promise.resolve(partitionSeries(key, [2048, 1024 * 1024]))
        if (key === 'TOPIC_MESSAGES_IN' && range === '1h') return stale
        return Promise.resolve({ key, unit: key === 'TOPIC_BYTES_IN' ? 'bytes/s' : 'msg/s', range, stepSeconds: 30,
          series: [{ name: key, points: [{ t: '2026-09-09T00:00:00Z', v: 999 }] }] })
      }
      return Promise.reject(new Error(`unexpected url: ${url}`))
    })
    const w = mount(TopicDetailView)
    await flushPromises()

    await w.findAll('.metric-tabs button').find((b) => b.text() === '바이트')?.trigger('click')
    await flushPromises()

    let chart = w.findComponent(MetricChart)
    expect(chart.props('unit')).toBe('bytes/s')
    expect((chart.props('series') as { points: { v: number }[] }[])[0]?.points[0]?.v).toBe(999)

    resolveStale({
      key: 'TOPIC_MESSAGES_IN', unit: 'msg/s', range: '1h', stepSeconds: 30,
      series: [{ name: 'TOPIC_MESSAGES_IN', points: [{ t: '2026-09-09T00:00:00Z', v: 111 }] }],
    })
    await flushPromises()

    chart = w.findComponent(MetricChart)
    expect(chart.props('unit')).toBe('bytes/s')
    expect((chart.props('series') as { points: { v: number }[] }[])[0]?.points[0]?.v).toBe(999)
  })
})
