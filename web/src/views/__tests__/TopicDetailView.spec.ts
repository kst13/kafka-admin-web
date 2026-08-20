import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
vi.mock('@/composables/useSession', () => ({ useSession: () => ({ isAdmin: { value: false } }) }))
vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { name: 't' } }),
  useRouter: () => ({ push: vi.fn() }),
}))

import { api } from '@/api/client'
import TopicDetailView from '../TopicDetailView.vue'
import TrendChart from '@/components/TrendChart.vue'

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
const producedP1 = [
  { sampledAt: '2026-08-20T09:00:00Z', value: 0 },
  { sampledAt: '2026-08-20T09:30:00Z', value: 5 },
  { sampledAt: '2026-08-20T10:00:00Z', value: 15 },
]

function mockApiByUrl() {
  vi.mocked(api).mockImplementation((url: string) => {
    if (url.startsWith('/topics/t/messages')) return Promise.resolve([])
    if (url.startsWith('/topics/t/throughput')) return Promise.resolve(throughput)
    if (url.includes('type=PRODUCED_PARTITION') && url.includes(`subject=${encodeURIComponent('t|1')}`))
      return Promise.resolve(producedP1)
    if (url.startsWith('/metrics')) return Promise.resolve(produced)
    if (url.startsWith('/topics/t')) return Promise.resolve(detail)
    return Promise.reject(new Error(`unexpected url: ${url}`))
  })
}

describe('TopicDetailView 파티션별 유입량', () => {
  // 화살표 함수가 mock을 반환하면 vitest가 cleanup 함수로 호출하므로 반드시 블록으로 감싼다
  beforeEach(() => {
    vi.mocked(api).mockReset()
  })

  it('파티션 표에 endOffset·최근 1시간 유입·분당 속도를 보여준다', async () => {
    mockApiByUrl()
    const wrapper = mount(TopicDetailView)
    await flushPromises()

    const rows = wrapper.findAll('tbody tr')
    expect(rows[0]?.text()).toContain('120') // endOffset
    expect(rows[0]?.text()).toContain('60') // 최근 1시간 유입
    expect(rows[0]?.text()).toContain('1.0') // 분당 속도
    expect(rows[1]?.text()).toContain('10')
  })

  it('실데이터가 있으면 예시 표시가 없다', async () => {
    mockApiByUrl()
    const wrapper = mount(TopicDetailView)
    await flushPromises()

    expect(wrapper.text()).not.toContain('예시')
    expect(wrapper.find('.demo').exists()).toBe(false)
  })

  it('수집 샘플이 없으면 예시 데이터를 배지와 함께 흐리게 보여준다', async () => {
    vi.mocked(api).mockImplementation((url: string) => {
      if (url.startsWith('/topics/t/messages')) return Promise.resolve([])
      if (url.startsWith('/topics/t/throughput')) return Promise.resolve([])
      if (url.startsWith('/metrics')) return Promise.resolve([])
      if (url.startsWith('/topics/t')) return Promise.resolve(detail)
      return Promise.reject(new Error(`unexpected url: ${url}`))
    })
    const wrapper = mount(TopicDetailView)
    await flushPromises()

    // 예시 배지와 안내 문구
    expect(wrapper.text()).toContain('예시')
    expect(wrapper.text()).toContain('아직 수집된 샘플이 없어 예시 데이터를 표시 중입니다')
    // 파티션 표에 파티션 수만큼 예시 값이 흐린 스타일로 채워진다
    expect(wrapper.findAll('td.demo').length).toBeGreaterThan(0)
    // 예시 추이 차트도 그려진다
    const chart = wrapper.findComponent(TrendChart)
    expect(chart.exists()).toBe(true)
    expect((chart.props('points') as unknown[]).length).toBeGreaterThan(1)
  })

  it('토픽 유입 추이 차트를 시간대별 증가분으로 그린다', async () => {
    mockApiByUrl()
    const wrapper = mount(TopicDetailView)
    await flushPromises()

    expect(wrapper.text()).toContain('유입 추이')
    const chart = wrapper.findComponent(TrendChart)
    expect(chart.exists()).toBe(true)
    // 누적 100→130→160 은 시간대별 증가분 30, 30 으로 변환된다
    const points = chart.props('points') as { t: string; v: number }[]
    expect(points.map((p) => p.v)).toEqual([30, 30])
  })

  it('예시 모드에서도 파티션 드롭다운이 보이고 파티션별 예시 곡선을 그린다', async () => {
    vi.mocked(api).mockImplementation((url: string) => {
      if (url.startsWith('/topics/t/messages')) return Promise.resolve([])
      if (url.startsWith('/topics/t/throughput')) return Promise.resolve([])
      if (url.startsWith('/metrics')) return Promise.resolve([])
      if (url.startsWith('/topics/t')) return Promise.resolve(detail)
      return Promise.reject(new Error(`unexpected url: ${url}`))
    })
    const wrapper = mount(TopicDetailView)
    await flushPromises()

    const select = wrapper.find('select.partition-select')
    expect(select.exists()).toBe(true)

    const allPoints = wrapper.findComponent(TrendChart).props('points') as { v: number }[]
    const callsBefore = vi.mocked(api).mock.calls.length
    await select.setValue('1')
    await flushPromises()

    const p1Points = wrapper.findComponent(TrendChart).props('points') as { v: number }[]
    expect(p1Points.length).toBeGreaterThan(1)
    // 파티션별로 다른 예시 곡선을 보여준다
    expect(p1Points.map((p) => p.v)).not.toEqual(allPoints.map((p) => p.v))
    // 예시 모드에서는 서버 조회를 하지 않는다
    expect(vi.mocked(api).mock.calls.length).toBe(callsBefore)
  })

  it('파티션을 선택하면 해당 파티션의 유입 추이로 차트가 바뀐다', async () => {
    mockApiByUrl()
    const wrapper = mount(TopicDetailView)
    await flushPromises()

    await wrapper.find('select.partition-select').setValue('1')
    await flushPromises()

    // 파티션 1 누적 0→5→15 는 시간대별 증가분 5, 10 으로 변환된다
    const chart = wrapper.findComponent(TrendChart)
    const points = chart.props('points') as { t: string; v: number }[]
    expect(points.map((p) => p.v)).toEqual([5, 10])

    // 전체로 되돌리면 토픽 추이로 복귀한다
    await wrapper.find('select.partition-select').setValue('all')
    await flushPromises()
    const back = wrapper.findComponent(TrendChart).props('points') as { t: string; v: number }[]
    expect(back.map((p) => p.v)).toEqual([30, 30])
  })
})
