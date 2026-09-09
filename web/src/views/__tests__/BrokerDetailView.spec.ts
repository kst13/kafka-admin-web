import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
vi.mock('vue-router', () => ({ useRoute: () => ({ params: { id: '2' } }) }))

import { api } from '@/api/client'
import BrokerDetailView from '../BrokerDetailView.vue'
import MetricChart from '@/components/MetricChart.vue'

const health = {
  configured: true, asOf: '2026-09-09T00:00:00Z', activeControllers: 1, offlinePartitions: 0, underReplicated: 0,
  underMinIsr: 0, uncleanElectionsLastHour: 0, uncleanElectionsTotal: 0, activeBrokers: 2, fencedBrokers: 0,
  brokers: [
    { id: 1, host: '10.0.0.1', scraped: true, bytesInPerSec: 1, bytesOutPerSec: 1, messagesInPerSec: 1, p99ProduceMs: 1,
      p99FetchMs: 1, handlerIdlePct: 1, networkIdlePct: 1, heapUsedPct: 1, cpuPct: 1, leaderCount: 1, partitionCount: 1, underReplicated: 0 },
    { id: 2, host: '10.0.0.2', scraped: true, bytesInPerSec: 2097152, bytesOutPerSec: 1048576, messagesInPerSec: 250.4,
      p99ProduceMs: 12, p99FetchMs: 34, handlerIdlePct: 88.8, networkIdlePct: 97.1, heapUsedPct: 41.5, cpuPct: 12.3,
      leaderCount: 15, partitionCount: 45, underReplicated: 1 },
  ],
}
const alerts = [
  { ruleType: 'LATENCY_HIGH', subjectKey: '2', message: '브로커 2 Produce p99 1200ms (임계치 1000ms)', value: 1200, threshold: 1000, occurredAt: '2026-09-09T00:00:00Z' },
  { ruleType: 'HEAP_HIGH', subjectKey: '1', message: '다른 브로커', value: 90, threshold: 85, occurredAt: '2026-09-09T00:00:00Z' },
  { ruleType: 'LAG_HIGH', subjectKey: '2', message: '그룹 이름이 2 인 랙 알림', value: 1, threshold: 1, occurredAt: '2026-09-09T00:00:00Z' },
]

function seriesFor(url: string, v = 7) {
  const key = new URL(url, 'http://x').searchParams.get('key') ?? ''
  const range = new URL(url, 'http://x').searchParams.get('range') ?? ''
  const unit = key.endsWith('_MS') ? 'ms' : key.endsWith('_PCT') ? '%' : key === 'BROKER_REQUEST_QUEUE' ? 'count' : 'bytes/s'
  return { key, unit, range, stepSeconds: 30, series: [{ name: key, points: [{ t: '2026-09-09T00:00:00Z', v }] }] }
}

function mockApi() {
  vi.mocked(api).mockImplementation((url: string) => {
    if (url === '/cluster/health') return Promise.resolve(health)
    if (url === '/alerts') return Promise.resolve(alerts)
    if (url.startsWith('/brokers/2/series')) return Promise.resolve(seriesFor(url))
    return Promise.reject(new Error(`unexpected url: ${url}`))
  })
}

describe('BrokerDetailView', () => {
  beforeEach(() => { vi.mocked(api).mockReset() })

  it('카드에 이 브로커의 현재값을 포맷해 보여준다', async () => {
    mockApi()
    const w = mount(BrokerDetailView)
    await flushPromises()
    expect(w.find('h1').text()).toContain('브로커 2')
    expect(w.text()).toContain('10.0.0.2')
    const cards = w.find('.cards').text()
    expect(cards).toContain('2.0 MB/s')
    expect(cards).toContain('1.0 MB/s')
    expect(cards).toContain('250.4 msg/s')
    expect(cards).toContain('12 ms')
    expect(cards).toContain('34 ms')
    expect(cards).toContain('88.8%')
    expect(cards).toContain('97.1%')
    expect(cards).toContain('41.5%')
    expect(cards).toContain('12.3%')
    expect(cards).toContain('15 / 45')
    expect(cards).toContain('요청 큐')
  })

  it('차트 4개를 그리고 범위를 바꾸면 시리즈 9개를 새 범위로 다시 조회한다', async () => {
    mockApi()
    const w = mount(BrokerDetailView)
    await flushPromises()
    const charts = w.findAllComponents(MetricChart)
    expect(charts).toHaveLength(4)
    expect(charts.map((c) => c.props('title'))).toEqual(['처리량', '지연', '부하', 'JVM'])
    expect((charts[0]?.props('series') as { name: string }[]).map((s) => s.name)).toEqual(['BROKER_BYTES_IN', 'BROKER_BYTES_OUT'])
    expect(charts[1]?.props('unit')).toBe('ms')
    const firstCalls = vi.mocked(api).mock.calls.map((c) => c[0] as string).filter((u) => u.startsWith('/brokers/2/series'))
    expect(firstCalls).toHaveLength(9)
    expect(firstCalls.every((u) => u.includes('range=1h'))).toBe(true)

    const btn = w.findAll('.range-tabs button').find((b) => b.text() === '24시간')
    await btn?.trigger('click')
    await flushPromises()
    const after = vi.mocked(api).mock.calls.map((c) => c[0] as string).filter((u) => u.includes('range=24h'))
    expect(after).toHaveLength(9)
    expect(btn?.classes()).toContain('on')
  })

  it('알림 이력은 이 브로커(subjectKey=id)의 브로커 규칙만 라벨과 함께 보여준다', async () => {
    mockApi()
    const w = mount(BrokerDetailView)
    await flushPromises()
    const rows = w.findAll('.broker-alerts tbody tr')
    expect(rows).toHaveLength(1)
    expect(rows[0]?.text()).toContain('요청 지연 초과')
    expect(rows[0]?.text()).toContain('1200ms')
  })

  it('없는 브로커면 오류 문구', async () => {
    vi.mocked(api).mockImplementation((url: string) => {
      if (url === '/cluster/health') return Promise.resolve({ ...health, brokers: [] })
      if (url === '/alerts') return Promise.resolve([])
      return Promise.reject(new Error('존재하지 않는 브로커입니다: 2'))
    })
    const w = mount(BrokerDetailView)
    await flushPromises()
    expect(w.find('.error').text()).toContain('존재하지 않는 브로커')
  })

  it('범위를 빠르게 전환하면 나중에 시작한 요청의 결과만 반영한다', async () => {
    const resolvers: Array<() => void> = []
    vi.mocked(api).mockImplementation((url: string) => {
      if (url === '/cluster/health') return Promise.resolve(health)
      if (url === '/alerts') return Promise.resolve(alerts)
      if (url.startsWith('/brokers/2/series')) {
        const range = new URL(url, 'http://x').searchParams.get('range')
        if (range === '1h') {
          return new Promise((resolve) => { resolvers.push(() => resolve(seriesFor(url, 1))) })
        }
        return Promise.resolve(seriesFor(url, 99))
      }
      return Promise.reject(new Error(`unexpected url: ${url}`))
    })

    const w = mount(BrokerDetailView)
    await flushPromises() // health/alerts resolve; 1h series requests are pending (deferred)

    const btn = w.findAll('.range-tabs button').find((b) => b.text() === '24시간')
    await btn?.trigger('click')
    await flushPromises() // 24h series resolve immediately and win the race

    resolvers.forEach((resolve) => resolve())
    await flushPromises() // stale 1h batch resolves after, but must be discarded

    const charts = w.findAllComponents(MetricChart)
    const throughput = charts[0]?.props('series') as { points: { v: number }[] }[]
    expect(throughput.every((s) => s.points[0]?.v === 99)).toBe(true)
    expect(btn?.classes()).toContain('on')
  })

  it('시리즈 조회 실패 배너는 다음 범위가 성공하면 사라진다', async () => {
    vi.mocked(api).mockImplementation((url: string) => {
      if (url === '/cluster/health') return Promise.resolve(health)
      if (url === '/alerts') return Promise.resolve(alerts)
      if (url.startsWith('/brokers/2/series')) {
        const range = new URL(url, 'http://x').searchParams.get('range')
        if (range === '1h') return Promise.reject(new Error('시리즈 조회 실패(1h)'))
        return Promise.resolve(seriesFor(url))
      }
      return Promise.reject(new Error(`unexpected url: ${url}`))
    })

    const w = mount(BrokerDetailView)
    await flushPromises()
    expect(w.find('.error').exists()).toBe(true)
    expect(w.find('.error').text()).toContain('시리즈 조회 실패(1h)')

    const btn = w.findAll('.range-tabs button').find((b) => b.text() === '24시간')
    await btn?.trigger('click')
    await flushPromises()
    expect(w.find('.error').exists()).toBe(false)
  })
})
