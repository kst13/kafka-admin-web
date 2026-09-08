import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const isAdmin = ref(true)
vi.mock('@/composables/useSession', () => ({
  useSession: () => ({ isAdmin, session: ref({ username: 'admin', role: 'ADMIN' }), load: vi.fn() }),
}))
const subjectParam = ref('orders-value')
vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { subject: subjectParam.value } }),
  useRouter: () => ({ push: vi.fn() }),
}))

import { api } from '@/api/client'
import SchemaSubjectView from '../SchemaSubjectView.vue'

const detail = {
  subject: 'orders-value', topic: 'orders', kind: 'value', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL',
  versions: [{ version: 2, id: 9, schemaType: 'AVRO' }, { version: 1, id: 4, schemaType: 'AVRO' }],
}
const v2 = { subject: 'orders-value', version: 2, id: 9, schemaType: 'AVRO', schema: '{"type":"record","name":"Order","fields":[]}', references: [] }
const v1 = { subject: 'orders-value', version: 1, id: 4, schemaType: 'AVRO', schema: '{"type":"string"}', references: [] }
const stubs = { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } }

function mockApi() {
  vi.mocked(api).mockImplementation((url: string) => {
    if (url === '/schemas/subjects/orders-value') return Promise.resolve(detail)
    if (url === '/schemas/subjects/orders-value/versions/2') return Promise.resolve(v2)
    if (url === '/schemas/subjects/orders-value/versions/1') return Promise.resolve(v1)
    return Promise.reject(new Error(`unexpected ${url}`))
  })
}

describe('SchemaSubjectView', () => {
  beforeEach(() => { vi.mocked(api).mockReset(); isAdmin.value = true; subjectParam.value = 'orders-value' })

  it('메타데이터와 최신 버전 본문을 정렬해 보여주고 ADMIN 버튼을 노출한다', async () => {
    mockApi()
    const wrapper = mount(SchemaSubjectView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('orders-value')
    expect(wrapper.find('a[href="/topics/orders"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('전역 (BACKWARD)')
    expect((wrapper.find('select[name="version"]').element as HTMLSelectElement).value).toBe('2')
    expect(wrapper.find('pre.schema').text()).toContain('"name": "Order"')
    expect(wrapper.find('button.register-version').exists()).toBe(true)
    expect(wrapper.find('button.change-compat').exists()).toBe(true)
    expect(wrapper.find('button.delete-subject').exists()).toBe(true)
  })

  it('버전을 바꾸면 본문을 다시 불러오고, 비교를 켜면 두 번째 본문을 나란히 보여준다', async () => {
    mockApi()
    const wrapper = mount(SchemaSubjectView, { global: { stubs } })
    await flushPromises()
    await wrapper.find('select[name="version"]').setValue('1')
    await flushPromises()
    expect(wrapper.find('pre.schema').text()).toContain('"type": "string"')
    await wrapper.find('input[name="compare"]').setValue(true)
    await wrapper.find('select[name="compare-version"]').setValue('2')
    await flushPromises()
    expect(wrapper.find('pre.schema-compare').text()).toContain('"name": "Order"')
  })

  it('DEVELOPER 는 새 버전 등록만 보이고, 규칙 밖 서브젝트는 등록 버튼도 없다', async () => {
    isAdmin.value = false
    mockApi()
    let wrapper = mount(SchemaSubjectView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.find('button.register-version').exists()).toBe(true)
    expect(wrapper.find('button.change-compat').exists()).toBe(false)
    expect(wrapper.find('button.delete-subject').exists()).toBe(false)

    subjectParam.value = 'com.x.Y'
    vi.mocked(api).mockImplementation((url: string) => {
      if (url === '/schemas/subjects/com.x.Y') return Promise.resolve({ ...detail, subject: 'com.x.Y', topic: null, kind: 'other' })
      return Promise.resolve({ ...v2, subject: 'com.x.Y' })
    })
    wrapper = mount(SchemaSubjectView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.find('button.register-version').exists()).toBe(false)
    expect(wrapper.text()).toContain('기타')
  })

  it('조회 실패 메시지를 표시한다', async () => {
    vi.mocked(api).mockRejectedValue(new Error('존재하지 않는 서브젝트/버전입니다: orders-value'))
    const wrapper = mount(SchemaSubjectView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('존재하지 않는 서브젝트')
  })
})
