import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import KafkaAppPermissionModal from '../KafkaAppPermissionModal.vue'

const topics = [
  { name: 'orders', partitionCount: 3, replicationFactor: 3 },
  { name: 'events', partitionCount: 1, replicationFactor: 3 },
]
const detail = { name: 'order-api', owner: null, description: null, createdAt: null, registered: true,
  permissions: [{ topic: 'orders', mode: 'consume' }], otherAcls: [] }

describe('KafkaAppPermissionModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('토픽과 모드를 고르면 영향 요약이 바뀌고 PUT 을 호출한다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics).mockResolvedValueOnce(detail)
    const wrapper = mount(KafkaAppPermissionModal, { props: { app: 'order-api' } })
    await flushPromises()
    expect(wrapper.find('button.primary').attributes('disabled')).toBeDefined()
    await wrapper.find('select[name="topic"]').setValue('orders')
    await wrapper.find('select[name="mode"]').setValue('consume')
    expect(wrapper.text()).toContain("컨슈머 그룹 'order-api*'")
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    const call = vi.mocked(api).mock.calls[1]!
    expect(call[0]).toBe('/ops/kafka-apps/order-api/topics/orders')
    expect(call[1]).toMatchObject({ method: 'PUT' })
    expect(JSON.parse((call[1] as RequestInit).body as string)).toEqual({ mode: 'consume' })
    expect(wrapper.emitted('saved')).toEqual([[detail]])
  })

  it('변경 모드는 토픽을 고정하고 현재 모드를 기본값으로 둔다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics)
    const wrapper = mount(KafkaAppPermissionModal, { props: { app: 'order-api', topic: 'orders', mode: 'produce' } })
    await flushPromises()
    expect(wrapper.find('select[name="topic"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('orders')
    expect((wrapper.find('select[name="mode"]').element as HTMLSelectElement).value).toBe('produce')
    expect(wrapper.find('button.primary').attributes('disabled')).toBeUndefined()
  })

  it('실패하면 에러를 표시한다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics).mockRejectedValueOnce(new Error('존재하지 않는 토픽입니다'))
    const wrapper = mount(KafkaAppPermissionModal, { props: { app: 'order-api', topic: 'ghost', mode: 'produce' } })
    await flushPromises()
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('존재하지 않는 토픽입니다')
    expect(wrapper.emitted('saved')).toBeUndefined()
  })
})
