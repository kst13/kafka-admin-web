import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const isAdmin = ref(true)
vi.mock('@/composables/useSession', () => ({
  useSession: () => ({ isAdmin, session: ref({ username: 'admin', role: 'ADMIN' }), load: vi.fn() }),
}))
const globalCompatibility = ref<string | null>('BACKWARD')
vi.mock('@/composables/useSchemaRegistry', () => ({
  useSchemaRegistry: () => ({ configured: ref(true), globalCompatibility, status: ref(null), load: vi.fn(), setStatus: vi.fn() }),
}))

import { api } from '@/api/client'
import SchemasView from '../SchemasView.vue'

const subjects = [
  { subject: 'orders-value', topic: 'orders', kind: 'value', latestVersion: 3, schemaType: 'AVRO', compatibility: 'FULL', compatibilitySource: 'SUBJECT' },
  { subject: 'orders-key', topic: 'orders', kind: 'key', latestVersion: 1, schemaType: 'JSON', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL' },
  { subject: 'com.x.Y', topic: null, kind: 'other', latestVersion: 2, schemaType: 'PROTOBUF', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL' },
]
const stubs = { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } }

describe('SchemasView', () => {
  beforeEach(() => { vi.mocked(api).mockReset(); isAdmin.value = true })

  it('토픽별 표와 기타 표, 전역 호환성을 보여준다', async () => {
    vi.mocked(api).mockResolvedValueOnce(subjects)
    const wrapper = mount(SchemasView, { global: { stubs } })
    await flushPromises()
    const rows = wrapper.findAll('table.topics tbody tr')
    expect(rows).toHaveLength(1)
    expect(rows[0]!.text()).toContain('orders')
    expect(rows[0]!.text()).toContain('AVRO')
    expect(rows[0]!.text()).toContain('v3')
    expect(rows[0]!.text()).toContain('JSON')
    expect(wrapper.find('a[href="/schemas/orders-value"]').exists()).toBe(true)
    expect(wrapper.find('a[href="/topics/orders"]').exists()).toBe(true)
    expect(wrapper.find('table.others').text()).toContain('com.x.Y')
    expect(wrapper.find('.global-compat').text()).toContain('BACKWARD')
    expect(wrapper.find('button.register-schema').exists()).toBe(true)
  })

  it('비어 있으면 안내 문구와 등록 버튼', async () => {
    vi.mocked(api).mockResolvedValueOnce([])
    const wrapper = mount(SchemasView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('등록된 스키마가 없습니다')
    expect(wrapper.find('button.register-schema').exists()).toBe(true)
  })

  it('DEVELOPER 도 등록 버튼은 보이고, 조회 실패 메시지를 표시한다', async () => {
    isAdmin.value = false
    vi.mocked(api).mockRejectedValueOnce(new Error('Schema Registry 접속 불가'))
    const wrapper = mount(SchemasView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('Schema Registry 접속 불가')
    expect(wrapper.find('button.register-schema').exists()).toBe(true)
  })
})
