import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
vi.mock('@/composables/useSession', () => ({ useSession: () => ({ isAdmin: { value: true } }) }))

import { api } from '@/api/client'
import TopicsView from '../TopicsView.vue'
import TopicCreateModal from '@/components/TopicCreateModal.vue'

const stubs = { RouterLink: { template: '<a><slot /></a>' } }
const created = { name: 'new-topic', partitionCount: 3, replicationFactor: 3 }

describe('TopicsView 생성 직후 반영', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.mocked(api).mockReset()
  })
  afterEach(() => vi.useRealTimers())

  it('생성 이벤트를 받으면 서버 목록에 아직 없어도 즉시 표에 보인다', async () => {
    // 최초 로드와 재조회 모두 아직 새 토픽이 없는 목록을 돌려준다 (브로커 전파 지연)
    vi.mocked(api).mockResolvedValue([{ name: 'a', partitionCount: 1, replicationFactor: 1 }])
    const wrapper = mount(TopicsView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).not.toContain('new-topic')

    await wrapper.find('button.primary').trigger('click')
    wrapper.findComponent(TopicCreateModal).vm.$emit('created', created)
    await flushPromises()

    expect(wrapper.text()).toContain('new-topic')
    expect(wrapper.findComponent(TopicCreateModal).exists()).toBe(false)
  })

  it('서버 목록에 새 토픽이 나타날 때까지 잠깐 재조회하고, 나타나면 서버 목록으로 교체한다', async () => {
    const without = [{ name: 'a', partitionCount: 1, replicationFactor: 1 }]
    const withNew = [...without, { name: 'new-topic', partitionCount: 3, replicationFactor: 3 }]
    vi.mocked(api)
      .mockResolvedValueOnce(without) // onMounted
      .mockResolvedValueOnce(without) // 생성 직후 1차 재조회: 아직 없음
      .mockResolvedValueOnce(withNew) // 2차 재조회: 나타남
    const wrapper = mount(TopicsView, { global: { stubs } })
    await flushPromises()

    await wrapper.find('button.primary').trigger('click')
    wrapper.findComponent(TopicCreateModal).vm.$emit('created', created)
    await flushPromises()
    expect(wrapper.text()).toContain('new-topic') // 낙관적 반영

    await vi.advanceTimersByTimeAsync(600)
    await flushPromises()
    expect(api).toHaveBeenCalledTimes(3)
    expect(wrapper.findAll('tbody tr')).toHaveLength(2)

    // 나타난 뒤에는 더 재조회하지 않는다
    await vi.advanceTimersByTimeAsync(3000)
    expect(api).toHaveBeenCalledTimes(3)
  })
})
