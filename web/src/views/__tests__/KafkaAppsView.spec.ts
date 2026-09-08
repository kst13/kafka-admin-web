import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const isAdmin = ref(true)
vi.mock('@/composables/useSession', () => ({
  useSession: () => ({ isAdmin, session: ref({ username: 'admin', role: 'ADMIN' }), load: vi.fn() }),
}))

import { api } from '@/api/client'
import KafkaAppsView from '../KafkaAppsView.vue'
import KafkaAppCreateModal from '@/components/KafkaAppCreateModal.vue'

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

describe('KafkaAppsView 생성 직후 반영', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.mocked(api).mockReset()
    isAdmin.value = true
  })
  afterEach(() => vi.useRealTimers())

  it('생성 직후 registered=true 로 나타날 때까지 재조회하고, 나타나면 멈춘다', async () => {
    const without = apps
    const created = { name: 'new-app', owner: null, description: null, registered: true, topicCount: 0 }
    const withNew = [...without, created]
    vi.mocked(api)
      .mockResolvedValueOnce(without) // onMounted: 최초 목록
      .mockResolvedValueOnce([]) // 생성 모달 onMounted: 담당자 목록(/ops/users)
      .mockResolvedValueOnce(without) // created 직후 1차 재조회: 아직 없음
      .mockResolvedValueOnce(withNew) // 2차 재조회: 나타남
    const wrapper = mount(KafkaAppsView, { global: { stubs } })
    await flushPromises()

    await wrapper.find('button.create-app').trigger('click')
    await flushPromises() // 모달의 담당자 목록 조회 완료

    wrapper.findComponent(KafkaAppCreateModal).vm.$emit('created', 'new-app')
    await flushPromises()

    await vi.advanceTimersByTimeAsync(600)
    await flushPromises()

    expect(wrapper.text()).toContain('new-app')
    const listCalls = vi.mocked(api).mock.calls.filter(([url]) => url === '/kafka-apps')
    expect(listCalls).toHaveLength(3)

    // 나타난 뒤에는 더 재조회하지 않는다
    await vi.advanceTimersByTimeAsync(3000)
    expect(vi.mocked(api).mock.calls.filter(([url]) => url === '/kafka-apps')).toHaveLength(3)
  })
})
