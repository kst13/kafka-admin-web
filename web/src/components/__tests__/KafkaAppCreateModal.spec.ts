import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import KafkaAppCreateModal from '../KafkaAppCreateModal.vue'

const siteUsers = [
  { id: 1, username: 'admin', role: 'ADMIN' },
  { id: 2, username: 'dev1', role: 'DEVELOPER' },
]

describe('KafkaAppCreateModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('이름이 규칙에 맞기 전엔 생성 버튼 비활성', async () => {
    vi.mocked(api).mockResolvedValueOnce(siteUsers)
    const wrapper = mount(KafkaAppCreateModal)
    await flushPromises()
    expect(wrapper.find('button.primary').attributes('disabled')).toBeDefined()
    await wrapper.find('input[name="name"]').setValue('bad name')
    expect(wrapper.find('button.primary').attributes('disabled')).toBeDefined()
    await wrapper.find('input[name="name"]').setValue('order-api')
    expect(wrapper.find('button.primary').attributes('disabled')).toBeUndefined()
  })

  it('생성 성공 시 비밀번호 단계로 바뀌고 created 를 emit 한다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(siteUsers)
      .mockResolvedValueOnce({ name: 'order-api', password: 'Pw123456Pw123456Pw123456' })
    const wrapper = mount(KafkaAppCreateModal)
    await flushPromises()
    await wrapper.find('input[name="name"]').setValue('order-api')
    await wrapper.find('select[name="owner"]').setValue('dev1')
    await wrapper.find('input[name="description"]').setValue('주문')
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()

    const call = vi.mocked(api).mock.calls[1]!
    expect(call[0]).toBe('/ops/kafka-apps')
    expect(JSON.parse((call[1] as RequestInit).body as string)).toEqual({
      name: 'order-api', owner: 'dev1', description: '주문',
    })
    expect(wrapper.text()).toContain('Pw123456Pw123456Pw123456')
    expect(wrapper.text()).toContain('닫으면 다시 볼 수 없습니다')
    expect(wrapper.find('input[name="name"]').exists()).toBe(false)
    expect(wrapper.emitted('created')).toEqual([['order-api']])
  })

  it('실패하면 에러를 보여주고 폼에 머문다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(siteUsers)
      .mockRejectedValueOnce(new Error('이미 존재하는 Kafka 계정입니다: order-api'))
    const wrapper = mount(KafkaAppCreateModal)
    await flushPromises()
    await wrapper.find('input[name="name"]').setValue('order-api')
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('이미 존재하는 Kafka 계정입니다')
    expect(wrapper.find('input[name="name"]').exists()).toBe(true)
    expect(wrapper.emitted('created')).toBeUndefined()
  })

  it('등록 모드는 이름을 고정하고 register 를 호출하며 비밀번호 단계가 없다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(siteUsers)
      .mockResolvedValueOnce({ name: 'legacy', registered: true, permissions: [], otherAcls: [] })
    const wrapper = mount(KafkaAppCreateModal, { props: { registerName: 'legacy' } })
    await flushPromises()
    expect(wrapper.find('input[name="name"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('legacy')
    await wrapper.find('select[name="owner"]').setValue('dev1')
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    const call = vi.mocked(api).mock.calls[1]!
    expect(call[0]).toBe('/ops/kafka-apps/legacy/register')
    expect(JSON.parse((call[1] as RequestInit).body as string)).toEqual({ owner: 'dev1', description: '' })
    expect(wrapper.emitted('registered')).toEqual([['legacy']])
    expect(wrapper.text()).not.toContain('닫으면 다시 볼 수 없습니다')
  })
})
