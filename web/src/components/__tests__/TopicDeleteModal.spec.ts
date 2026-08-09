import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import TopicDeleteModal from '../TopicDeleteModal.vue'

describe('TopicDeleteModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('토픽명이 일치하기 전에는 삭제 버튼이 비활성', async () => {
    const wrapper = mount(TopicDeleteModal, { props: { name: 'orders' } })
    const btn = wrapper.find('button.danger')
    expect(btn.attributes('disabled')).toBeDefined()
    await wrapper.find('input').setValue('order')
    expect(wrapper.find('button.danger').attributes('disabled')).toBeDefined()
  })

  it('토픽명이 일치하면 활성화되고 DELETE 를 호출한다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(TopicDeleteModal, { props: { name: 'orders' } })
    await wrapper.find('input').setValue('orders')
    expect(wrapper.find('button.danger').attributes('disabled')).toBeUndefined()
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/topics/orders', { method: 'DELETE' })
    expect(wrapper.emitted('deleted')).toHaveLength(1)
  })

  it('실패하면 에러를 표시하고 deleted 를 emit 하지 않는다', async () => {
    // mockRejectedValue(persistent) leaves the mock's implementation set to
    // `() => Promise.reject(err)` after this call returns. That keeps the
    // rejected-promise machinery "live" past the point this test's assertions
    // finish, and vitest v4's spy/runner then reports it as an unhandled
    // rejection even though TopicDeleteModal's try/catch does handle it
    // synchronously (verified by instrumenting the component directly).
    // mockRejectedValueOnce consumes the one-shot implementation on this
    // single call, matching what the component actually does (exactly one
    // DELETE call) and avoiding the leftover implementation. This mirrors
    // the working pattern already used in TopicCreateModal.spec.ts's
    // equivalent error-path test. The assertions below are unchanged.
    vi.mocked(api).mockRejectedValueOnce(new Error('존재하지 않는 토픽입니다'))
    const wrapper = mount(TopicDeleteModal, { props: { name: 'orders' } })
    await wrapper.find('input').setValue('orders')
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('존재하지 않는 토픽입니다')
    expect(wrapper.emitted('deleted')).toBeUndefined()
  })
})
