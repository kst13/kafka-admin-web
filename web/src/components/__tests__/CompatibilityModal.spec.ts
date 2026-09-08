import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import CompatibilityModal from '../CompatibilityModal.vue'

describe('CompatibilityModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('전역 모드: 레벨을 고르면 PUT /ops/schemas/config 하고 saved 를 emit', async () => {
    vi.mocked(api).mockResolvedValueOnce({ configured: true, urls: [], globalCompatibility: 'FULL' })
    const wrapper = mount(CompatibilityModal, { props: { current: 'BACKWARD' } })
    expect((wrapper.find('select[name="compatibility"]').element as HTMLSelectElement).value).toBe('BACKWARD')
    expect(wrapper.find('option[value=""]').exists()).toBe(false)
    await wrapper.find('select[name="compatibility"]').setValue('FULL')
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/schemas/config', { method: 'PUT', body: JSON.stringify({ compatibility: 'FULL' }) })
    expect(wrapper.emitted('saved')![0]![0]).toMatchObject({ globalCompatibility: 'FULL' })
  })

  it('서브젝트 모드: NONE 경고와 전역 상속(null) 전송', async () => {
    vi.mocked(api).mockResolvedValueOnce({ subject: 'orders-value', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL', versions: [] })
    const wrapper = mount(CompatibilityModal, { props: { subject: 'orders-value', current: 'FULL', source: 'SUBJECT' } })
    expect(wrapper.find('option[value=""]').exists()).toBe(true)
    await wrapper.find('select[name="compatibility"]').setValue('NONE')
    expect(wrapper.find('.warn-none').exists()).toBe(true)
    await wrapper.find('select[name="compatibility"]').setValue('')
    expect(wrapper.find('.warn-none').exists()).toBe(false)
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/schemas/subjects/orders-value/config', { method: 'PUT', body: JSON.stringify({ compatibility: null }) })
    expect(wrapper.emitted('saved')).toHaveLength(1)
  })

  it('실패 메시지를 표시한다', async () => {
    vi.mocked(api).mockRejectedValueOnce(new Error('Schema Registry 접속 불가'))
    const wrapper = mount(CompatibilityModal, { props: { current: 'BACKWARD' } })
    await wrapper.find('select[name="compatibility"]').setValue('FULL')
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('Schema Registry 접속 불가')
    expect(wrapper.emitted('saved')).toBeUndefined()
  })
})
