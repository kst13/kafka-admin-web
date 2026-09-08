import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const isAdmin = ref(true)
vi.mock('@/composables/useSession', () => ({
  useSession: () => ({ isAdmin, session: ref({ username: 'admin', role: 'ADMIN' }), load: vi.fn() }),
}))
vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { name: 'order-api' } }),
  useRouter: () => ({ push: vi.fn() }),
}))

import { api } from '@/api/client'
import KafkaAppDetailView from '../KafkaAppDetailView.vue'

const stubs = { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } }

const detail = {
  name: 'order-api', owner: 'dev1', description: '주문', createdAt: '2026-09-04T00:00:00Z', registered: true,
  permissions: [{ topic: 'orders', mode: 'both' }, { topic: 'events', mode: 'consume' }],
  otherAcls: [{ resourceType: 'TOPIC', patternType: 'PREFIXED', name: 'legacy-', operation: 'WRITE' }],
}

describe('KafkaAppDetailView', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    isAdmin.value = true
  })

  it('메타데이터·권한 표·기타 ACL 을 보여주고 ADMIN 에게 변경 버튼을 노출한다', async () => {
    vi.mocked(api).mockResolvedValueOnce(detail)
    const wrapper = mount(KafkaAppDetailView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('dev1')
    expect(wrapper.text()).toContain('both (쓰기+읽기)')
    expect(wrapper.text()).toContain("'order-api*'")
    expect(wrapper.text()).toContain('legacy-')
    expect(wrapper.find('button.add-permission').exists()).toBe(true)
    expect(wrapper.find('button.revoke[data-topic="orders"]').exists()).toBe(true)
    expect(wrapper.find('button.reset-password').exists()).toBe(true)
    expect(wrapper.find('button.delete-app').exists()).toBe(true)
  })

  it('DEVELOPER 에게는 변경 버튼이 없다', async () => {
    isAdmin.value = false
    vi.mocked(api).mockResolvedValueOnce(detail)
    const wrapper = mount(KafkaAppDetailView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.find('button.add-permission').exists()).toBe(false)
    expect(wrapper.find('button.revoke').exists()).toBe(false)
    expect(wrapper.find('button.delete-app').exists()).toBe(false)
    expect(wrapper.text()).toContain('orders')
  })

  it('회수는 DELETE 를 호출하고 응답으로 표를 갱신한다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(detail)
      .mockResolvedValueOnce({ ...detail, permissions: [{ topic: 'events', mode: 'consume' }] })
    const wrapper = mount(KafkaAppDetailView, { global: { stubs } })
    await flushPromises()
    await wrapper.find('button.revoke[data-topic="orders"]').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/kafka-apps/order-api/topics/orders', { method: 'DELETE' })
    expect(wrapper.find('button.revoke[data-topic="orders"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('events')
  })

  it('미등록 계정은 변경 대신 등록 안내를 보여준다', async () => {
    vi.mocked(api).mockResolvedValueOnce({ ...detail, registered: false, owner: null, permissions: [] })
    const wrapper = mount(KafkaAppDetailView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('미등록')
    expect(wrapper.find('button.add-permission').exists()).toBe(false)
    expect(wrapper.find('button.register-app').exists()).toBe(true)
  })
})
