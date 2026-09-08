import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import SchemaDeleteModal from '../SchemaDeleteModal.vue'

describe('SchemaDeleteModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('서브젝트 이름이 일치하기 전에는 삭제 버튼이 비활성', async () => {
    const wrapper = mount(SchemaDeleteModal, { props: { subject: 'orders-value' } })
    expect(wrapper.find('button.danger').attributes('disabled')).toBeDefined()
    await wrapper.find('input').setValue('orders-valu')
    expect(wrapper.find('button.danger').attributes('disabled')).toBeDefined()
    expect(wrapper.text()).toContain('soft delete')
  })

  it('일치하면 DELETE 를 호출하고 deleted 를 emit 한다', async () => {
    vi.mocked(api).mockResolvedValueOnce({ subject: 'orders-value', deletedVersions: [1, 2] })
    const wrapper = mount(SchemaDeleteModal, { props: { subject: 'orders-value' } })
    await wrapper.find('input').setValue('orders-value')
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/schemas/subjects/orders-value', { method: 'DELETE' })
    expect(wrapper.emitted('deleted')).toHaveLength(1)
  })

  it('실패하면 에러를 표시하고 deleted 를 emit 하지 않는다', async () => {
    vi.mocked(api).mockRejectedValueOnce(new Error('존재하지 않는 서브젝트/버전입니다: orders-value'))
    const wrapper = mount(SchemaDeleteModal, { props: { subject: 'orders-value' } })
    await wrapper.find('input').setValue('orders-value')
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('존재하지 않는 서브젝트')
    expect(wrapper.emitted('deleted')).toBeUndefined()
  })
})
