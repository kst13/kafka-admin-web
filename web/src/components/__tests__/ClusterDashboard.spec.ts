import { afterEach, describe, expect, it, vi } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import ClusterDashboard from '../ClusterDashboard.vue'
import { api } from '@/api/client'
vi.mock('@/api/client', () => ({ api: vi.fn() }))
const snapshot = { sampledAt: new Date().toISOString(), stale: false, incomingPerSec: null,
  laggingGroups: 1, underReplicated: 0, maxDiskUsedPct: null,
  lagTop: [{ name: 'order/group', value: 12 }], growingLagTop: [], incomingTop: [] }
const options = { global: { stubs: { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } } } }
afterEach(() => { vi.useRealTimers(); vi.resetAllMocks() })
describe('ClusterDashboard', () => {
  it('shows unknown values and encoded detail links', async () => {
    vi.mocked(api).mockResolvedValue(snapshot)
    const w = mount(ClusterDashboard, options)
    await flushPromises()
    expect(w.find('.cards').text()).toContain('—')
    expect(w.find('a').attributes('href')).toBe('/groups/order%2Fgroup')
    w.unmount()
  })
  it('refreshes, pauses, retains data on failure, and stops after unmount', async () => {
    vi.useFakeTimers()
    vi.mocked(api).mockResolvedValue(snapshot)
    const w = mount(ClusterDashboard, options)
    await flushPromises()
    await vi.advanceTimersByTimeAsync(60_000)
    expect(api).toHaveBeenCalledTimes(2)
    await w.find('input').setValue(false)
    await vi.advanceTimersByTimeAsync(60_000)
    expect(api).toHaveBeenCalledTimes(2)
    vi.mocked(api).mockRejectedValue(new Error('offline'))
    await w.find('button').trigger('click')
    await flushPromises()
    expect(w.find('[role="alert"]').exists()).toBe(true)
    expect(w.find('a').text()).toBe('order/group')
    w.unmount()
    await vi.advanceTimersByTimeAsync(60_000)
    expect(api).toHaveBeenCalledTimes(3)
  })
  it('does not overlap slow refreshes', async () => {
    vi.useFakeTimers()
    vi.mocked(api).mockReturnValue(new Promise(() => {}))
    const w = mount(ClusterDashboard, options)
    await vi.advanceTimersByTimeAsync(120_000)
    expect(api).toHaveBeenCalledTimes(1)
    w.unmount()
  })
})
