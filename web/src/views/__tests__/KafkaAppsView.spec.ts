import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const isAdmin = ref(true)
vi.mock('@/composables/useSession', () => ({
  useSession: () => ({ isAdmin, session: ref({ username: 'admin', role: 'ADMIN' }), load: vi.fn() }),
}))

import { api } from '@/api/client'
import KafkaAppsView from '../KafkaAppsView.vue'

const apps = [
  { name: 'order-api', owner: 'dev1', description: '주문', registered: true, topicCount: 2 },
  { name: 'kafka-admin', owner: null, description: null, registered: false, topicCount: 0 },
]

const stubs = { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } }

describe('KafkaAppsView', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    isAdmin.value = true
  })

  it('등록·미등록 계정을 나열하고 ADMIN 에게 추가·등록 버튼을 보여준다', async () => {
    vi.mocked(api).mockResolvedValueOnce(apps)
    const wrapper = mount(KafkaAppsView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('order-api')
    expect(wrapper.text()).toContain('dev1')
    expect(wrapper.text()).toContain('미등록')
    expect(wrapper.find('button.create-app').exists()).toBe(true)
    expect(wrapper.find('button.register-app[data-name="kafka-admin"]').exists()).toBe(true)
    expect(wrapper.find('a[href="/kafka-apps/order-api"]').exists()).toBe(true)
  })

  it('DEVELOPER 에게는 변경 버튼이 없다', async () => {
    isAdmin.value = false
    vi.mocked(api).mockResolvedValueOnce(apps)
    const wrapper = mount(KafkaAppsView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.find('button.create-app').exists()).toBe(false)
    expect(wrapper.find('button.register-app').exists()).toBe(false)
    expect(wrapper.text()).toContain('order-api')
  })

  it('조회 실패 메시지를 표시한다', async () => {
    vi.mocked(api).mockRejectedValueOnce(new Error('브로커 접속 불가'))
    const wrapper = mount(KafkaAppsView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('브로커 접속 불가')
  })
})
