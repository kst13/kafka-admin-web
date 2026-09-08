import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const configured = ref(true)
vi.mock('@/composables/useSchemaRegistry', () => ({
  useSchemaRegistry: () => ({ configured, globalCompatibility: ref('BACKWARD'), status: ref(null), load: vi.fn(), setStatus: vi.fn() }),
}))

import { api } from '@/api/client'
import TopicSchemaSection from '../TopicSchemaSection.vue'

const stubs = { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } }
const value = { subject: 'orders-value', topic: 'orders', kind: 'value', latestVersion: 2, schemaType: 'AVRO', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL' }

describe('TopicSchemaSection', () => {
  beforeEach(() => { vi.mocked(api).mockReset(); configured.value = true })

  it('key/value 행을 보여주고 없는 쪽은 등록 버튼', async () => {
    vi.mocked(api).mockResolvedValueOnce({ topic: 'orders', key: null, value })
    const wrapper = mount(TopicSchemaSection, { props: { topic: 'orders' }, global: { stubs } })
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/schemas/topics/orders')
    expect(wrapper.find('tr.value').text()).toContain('AVRO')
    expect(wrapper.find('tr.value').text()).toContain('v2')
    expect(wrapper.find('a[href="/schemas/orders-value"]').exists()).toBe(true)
    expect(wrapper.find('tr.key').text()).toContain('없음')
    expect(wrapper.find('button.register-key').exists()).toBe(true)
    expect(wrapper.find('button.register-value').exists()).toBe(true) // 새 버전 등록
  })

  it('Registry 미설정이면 섹션을 그리지 않고 호출도 하지 않는다', async () => {
    configured.value = false
    const wrapper = mount(TopicSchemaSection, { props: { topic: 'orders' }, global: { stubs } })
    await flushPromises()
    expect(wrapper.find('section.topic-schemas').exists()).toBe(false)
    expect(api).not.toHaveBeenCalled()
  })

  it('조회 실패는 섹션 안에만 표시한다', async () => {
    vi.mocked(api).mockRejectedValueOnce(new Error('Schema Registry 접속 불가'))
    const wrapper = mount(TopicSchemaSection, { props: { topic: 'orders' }, global: { stubs } })
    await flushPromises()
    expect(wrapper.find('section.topic-schemas').text()).toContain('Schema Registry 접속 불가')
  })
})
