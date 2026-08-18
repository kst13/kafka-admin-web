import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import TopicCreateModal from '../TopicCreateModal.vue'

describe('TopicCreateModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('입력값으로 영향 요약을 보여준다', async () => {
    const wrapper = mount(TopicCreateModal)
    await wrapper.find('input[name="name"]').setValue('orders')
    await wrapper.find('input[name="partitions"]').setValue('6')
    await wrapper.find('input[name="replicationFactor"]').setValue('3')
    expect(wrapper.text()).toContain('파티션 6, 복제 팩터 3으로 생성합니다')
  })

  it('제출하면 POST /ops/topics 를 호출하고 created 를 emit 한다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(TopicCreateModal)
    await wrapper.find('input[name="name"]').setValue('orders')
    await wrapper.find('input[name="partitions"]').setValue('6')
    await wrapper.find('input[name="replicationFactor"]').setValue('3')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/topics', expect.objectContaining({ method: 'POST' }))
    expect(wrapper.emitted('created')).toHaveLength(1)
    // 목록 화면이 즉시 반영할 수 있도록 생성한 토픽 요약을 함께 넘긴다
    expect(wrapper.emitted('created')![0]).toEqual([
      { name: 'orders', partitionCount: 6, replicationFactor: 3 },
    ])
  })

  it('retention.ms 는 기본값 604800000(7일)이 채워져 있고 그대로 전송된다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(TopicCreateModal)
    expect((wrapper.find('input[name="retentionMs"]').element as HTMLInputElement).value).toBe('604800000')
    await wrapper.find('input[name="name"]').setValue('orders')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    const body = JSON.parse(vi.mocked(api).mock.calls[0]![1]!.body as string)
    expect(body.configs).toEqual({ 'retention.ms': '604800000' })
  })

  it('retention 을 입력하면 configs 로 전송한다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(TopicCreateModal)
    await wrapper.find('input[name="name"]').setValue('orders')
    await wrapper.find('input[name="partitions"]').setValue('3')
    await wrapper.find('input[name="replicationFactor"]').setValue('1')
    await wrapper.find('input[name="retentionMs"]').setValue('86400000')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    const body = JSON.parse(vi.mocked(api).mock.calls[0]![1]!.body as string)
    expect(body.configs).toEqual({ 'retention.ms': '86400000' })
  })

  it('서버 에러 메시지를 팝업 안에 표시한다', async () => {
    vi.mocked(api).mockRejectedValueOnce(new Error('이미 존재하는 토픽입니다'))
    const wrapper = mount(TopicCreateModal)
    await wrapper.find('input[name="name"]').setValue('dup')
    await wrapper.find('input[name="partitions"]').setValue('1')
    await wrapper.find('input[name="replicationFactor"]').setValue('1')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    expect(wrapper.text()).toContain('이미 존재하는 토픽입니다')
    expect(wrapper.emitted('created')).toBeUndefined()
  })
})
