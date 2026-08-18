import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import GroupRegisterModal from '../GroupRegisterModal.vue'

const topics = [
  { name: 'orders', partitionCount: 3, replicationFactor: 3 },
  { name: 'payments', partitionCount: 1, replicationFactor: 3 },
]

describe('GroupRegisterModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('토픽 목록을 불러와 체크박스로 보여주고 시작 위치 기본값은 latest 다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics)
    const wrapper = mount(GroupRegisterModal)
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/topics')
    expect(wrapper.findAll('input[type="checkbox"]')).toHaveLength(2)
    expect((wrapper.find('input[value="latest"]').element as HTMLInputElement).checked).toBe(true)
  })

  it('제출하면 POST /ops/groups 를 호출하고 registered 를 emit 한다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics).mockResolvedValueOnce({ groupId: 'order-svc' })
    const wrapper = mount(GroupRegisterModal)
    await flushPromises()
    await wrapper.find('input[name="groupId"]').setValue('order-svc')
    await wrapper.find('input[type="checkbox"][value="orders"]').setValue(true)
    await wrapper.find('input[value="earliest"]').setValue(true)
    expect(wrapper.text()).toContain("'order-svc' 그룹을 orders 의 처음(earliest)부터")
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    const [path, opts] = vi.mocked(api).mock.calls[1]!
    expect(path).toBe('/ops/groups')
    expect(JSON.parse(opts!.body as string)).toEqual({
      groupId: 'order-svc', topics: ['orders'], startFrom: 'earliest',
    })
    expect(wrapper.emitted('registered')![0]).toEqual([{ groupId: 'order-svc', state: 'Empty', memberCount: 0 }])
  })

  it('토픽을 하나도 고르지 않으면 제출하지 않고 안내한다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics)
    const wrapper = mount(GroupRegisterModal)
    await flushPromises()
    await wrapper.find('input[name="groupId"]').setValue('g')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    expect(api).toHaveBeenCalledTimes(1)
    expect(wrapper.text()).toContain('토픽을 1개 이상')
  })

  it('서버 에러 메시지를 팝업 안에 표시한다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics).mockRejectedValueOnce(new Error('이미 존재하는 그룹입니다'))
    const wrapper = mount(GroupRegisterModal)
    await flushPromises()
    await wrapper.find('input[name="groupId"]').setValue('dup')
    await wrapper.find('input[type="checkbox"][value="orders"]').setValue(true)
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    expect(wrapper.text()).toContain('이미 존재하는 그룹입니다')
    expect(wrapper.emitted('registered')).toBeUndefined()
  })
})
