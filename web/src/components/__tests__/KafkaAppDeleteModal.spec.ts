import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import KafkaAppDeleteModal from '../KafkaAppDeleteModal.vue'

describe('KafkaAppDeleteModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('앱 이름이 일치하기 전에는 삭제 버튼이 비활성', async () => {
    const wrapper = mount(KafkaAppDeleteModal, { props: { name: 'order-api' } })
    expect(wrapper.find('button.danger').attributes('disabled')).toBeDefined()
    await wrapper.find('input').setValue('order-ap')
    expect(wrapper.find('button.danger').attributes('disabled')).toBeDefined()
  })

  it('일치하면 DELETE 를 호출하고 deleted 를 emit 한다', async () => {
    vi.mocked(api).mockResolvedValueOnce(undefined)
    const wrapper = mount(KafkaAppDeleteModal, { props: { name: 'order-api' } })
    await wrapper.find('input').setValue('order-api')
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/kafka-apps/order-api', { method: 'DELETE' })
    expect(wrapper.emitted('deleted')).toHaveLength(1)
  })

  it('실패하면 에러를 표시하고 deleted 를 emit 하지 않는다', async () => {
    vi.mocked(api).mockRejectedValueOnce(new Error('kafka-admin 계정에 Cluster Alter 권한이 필요합니다'))
    const wrapper = mount(KafkaAppDeleteModal, { props: { name: 'order-api' } })
    await wrapper.find('input').setValue('order-api')
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('Cluster Alter 권한')
    expect(wrapper.emitted('deleted')).toBeUndefined()
  })
})
