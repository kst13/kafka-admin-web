import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import TopicEditModal from '../TopicEditModal.vue'

const props = { name: 'orders', currentPartitions: 3, configs: { 'retention.ms': '86400000' } }

describe('TopicEditModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('감소 불가 경고와 현재 값을 보여준다', () => {
    const wrapper = mount(TopicEditModal, { props })
    expect(wrapper.text()).toContain('줄일 수 없습니다')
    expect((wrapper.find('input[name="partitions"]').element as HTMLInputElement).value).toBe('3')
  })

  it('변경한 필드만 PATCH 본문에 담는다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(TopicEditModal, { props })
    await wrapper.find('input[name="partitions"]').setValue('6')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    const body = JSON.parse(vi.mocked(api).mock.calls[0]![1]!.body as string)
    expect(body.partitions).toBe(6)
    expect(body.configs).toBeUndefined()
    expect(wrapper.emitted('updated')).toHaveLength(1)
  })

  it('retention 변경은 configs 로 담는다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(TopicEditModal, { props })
    await wrapper.find('input[name="retentionMs"]').setValue('7200000')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    const body = JSON.parse(vi.mocked(api).mock.calls[0]![1]!.body as string)
    expect(body.partitions).toBeUndefined()
    expect(body.configs).toEqual({ 'retention.ms': '7200000' })
  })
})
