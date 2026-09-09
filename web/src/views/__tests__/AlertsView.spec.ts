import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

const prometheusConfigured = ref(true)
vi.mock('@/composables/usePrometheus', () => ({
  usePrometheus: () => ({ configured: prometheusConfigured, ready: () => Promise.resolve() }),
}))

import { api } from '@/api/client'
import AlertsView from '../AlertsView.vue'

const alerts = [
  { ruleType: 'LATENCY_HIGH', subjectKey: '2', message: 'm1', value: 1, threshold: 1, occurredAt: '2026-09-09T00:00:00Z' },
  { ruleType: 'OFFLINE_PARTITIONS', subjectKey: 'cluster', message: 'm2', value: 1, threshold: 0, occurredAt: '2026-09-09T00:01:00Z' },
  { ruleType: 'LAG_HIGH', subjectKey: 'g1', message: 'm3', value: 1, threshold: 1, occurredAt: '2026-09-09T00:02:00Z' },
  { ruleType: 'WEIRD', subjectKey: 'x', message: 'm4', value: 1, threshold: 1, occurredAt: '2026-09-09T00:03:00Z' },
]
const opts = { global: { stubs: { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } } } }

describe('AlertsView', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    prometheusConfigured.value = true
  })

  it('규칙 타입에 한글 라벨을 붙이고 대상은 화면으로 링크한다', async () => {
    vi.mocked(api).mockResolvedValue(alerts)
    const w = mount(AlertsView, opts)
    await flushPromises()
    const rows = w.findAll('tbody tr')
    expect(rows[0]?.text()).toContain('요청 지연 초과')
    expect(rows[0]?.find('a').attributes('href')).toBe('/brokers/2')
    expect(rows[1]?.text()).toContain('오프라인 파티션')
    expect(rows[1]?.find('a').attributes('href')).toBe('/')
    expect(rows[2]?.find('a').attributes('href')).toBe('/groups/g1')
    expect(rows[3]?.text()).toContain('WEIRD')
    expect(rows[3]?.find('a').exists()).toBe(false)
    expect(rows[0]?.find('td:nth-child(2)').attributes('title')).toContain('p99')
  })

  it('미설정이면 브로커 규칙 대상은 링크가 아니다', async () => {
    prometheusConfigured.value = false
    vi.mocked(api).mockResolvedValue([
      { ruleType: 'DISK_HIGH', subjectKey: '3', message: 'm1', value: 1, threshold: 1, occurredAt: '2026-09-09T00:00:00Z' },
      { ruleType: 'LATENCY_HIGH', subjectKey: '2', message: 'm2', value: 1, threshold: 1, occurredAt: '2026-09-09T00:01:00Z' },
      { ruleType: 'OFFLINE_PARTITIONS', subjectKey: 'cluster', message: 'm3', value: 1, threshold: 0, occurredAt: '2026-09-09T00:02:00Z' },
      { ruleType: 'LAG_HIGH', subjectKey: 'g1', message: 'm4', value: 1, threshold: 1, occurredAt: '2026-09-09T00:03:00Z' },
    ])
    const w = mount(AlertsView, opts)
    await flushPromises()
    const rows = w.findAll('tbody tr')
    expect(rows[0]?.find('a').exists()).toBe(false)
    expect(rows[0]?.text()).toContain('디스크 사용률 초과')
    expect(rows[1]?.find('a').exists()).toBe(false)
    expect(rows[1]?.text()).toContain('요청 지연 초과')
    expect(rows[2]?.find('a').attributes('href')).toBe('/')
    expect(rows[3]?.find('a').attributes('href')).toBe('/groups/g1')
  })
})
