import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const isAdmin = ref(true)
vi.mock('@/composables/useSession', () => ({ useSession: () => ({ isAdmin }) }))

import { api } from '@/api/client'
import GroupsView from '../GroupsView.vue'
import GroupRegisterModal from '@/components/GroupRegisterModal.vue'

const stubs = { RouterLink: { template: '<a><slot /></a>' } }
const registered = { groupId: 'new-group', state: 'Empty', memberCount: 0 }

describe('GroupsView', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    vi.mocked(api).mockReset()
    isAdmin.value = true
  })
  afterEach(() => vi.useRealTimers())

  it('ADMIN 에게만 그룹 등록 버튼이 보인다', async () => {
    vi.mocked(api).mockResolvedValue([])
    expect(mount(GroupsView, { global: { stubs } }).find('button.primary').exists()).toBe(true)
    isAdmin.value = false
    expect(mount(GroupsView, { global: { stubs } }).find('button.primary').exists()).toBe(false)
  })

  it('등록 이벤트를 받으면 즉시 표에 보이고, 서버 목록에 나타나면 그것으로 교체한다', async () => {
    const without = [{ groupId: 'a', state: 'Stable', memberCount: 2 }]
    vi.mocked(api)
      .mockResolvedValueOnce(without) // onMounted
      .mockResolvedValueOnce(without) // 1차 재조회
      .mockResolvedValueOnce([...without, registered]) // 2차 재조회
    const wrapper = mount(GroupsView, { global: { stubs } })
    await flushPromises()

    await wrapper.find('button.primary').trigger('click')
    wrapper.findComponent(GroupRegisterModal).vm.$emit('registered', registered)
    await flushPromises()
    expect(wrapper.text()).toContain('new-group')
    expect(wrapper.findComponent(GroupRegisterModal).exists()).toBe(false)

    await vi.advanceTimersByTimeAsync(600)
    await flushPromises()
    expect(api).toHaveBeenCalledTimes(3)
    expect(wrapper.findAll('tbody tr')).toHaveLength(2)
    await vi.advanceTimersByTimeAsync(3000)
    expect(api).toHaveBeenCalledTimes(3)
  })
})
