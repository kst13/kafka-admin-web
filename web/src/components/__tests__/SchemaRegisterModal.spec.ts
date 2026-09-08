import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import SchemaRegisterModal from '../SchemaRegisterModal.vue'

const topics = [{ name: 'orders', partitionCount: 3, replicationFactor: 3 }, { name: 'events', partitionCount: 1, replicationFactor: 3 }]

describe('SchemaRegisterModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('검사 통과 전엔 등록 버튼이 비활성이고, 검사 실패 사유를 보여준다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(topics)
      .mockResolvedValueOnce({ compatible: false, messages: ['READER_FIELD_MISSING_DEFAULT_VALUE'] })
    const wrapper = mount(SchemaRegisterModal)
    await flushPromises()
    expect(wrapper.find('button.register').attributes('disabled')).toBeDefined()
    await wrapper.find('select[name="topic"]').setValue('orders')
    await wrapper.find('textarea[name="schema"]').setValue('{"type":"string"}')
    await wrapper.find('button.check').trigger('click')
    await flushPromises()
    const call = vi.mocked(api).mock.calls[1]!
    expect(call[0]).toBe('/schemas/compatibility')
    expect(JSON.parse((call[1] as RequestInit).body as string)).toEqual({
      topic: 'orders', kind: 'value', schemaType: 'AVRO', schema: '{"type":"string"}',
    })
    expect(wrapper.find('.check-fail').text()).toContain('READER_FIELD_MISSING_DEFAULT_VALUE')
    expect(wrapper.find('button.register').attributes('disabled')).toBeDefined()
  })

  it('검사 통과 후 등록하면 registered 를 emit 하고 결과를 표시한다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(topics)
      .mockResolvedValueOnce({ compatible: true, messages: [] })
      .mockResolvedValueOnce({ subject: 'orders-key', id: 5, version: 1 })
    const wrapper = mount(SchemaRegisterModal)
    await flushPromises()
    await wrapper.find('select[name="topic"]').setValue('orders')
    await wrapper.find('input[name="kind"][value="key"]').setValue()
    await wrapper.find('select[name="schemaType"]').setValue('JSON')
    await wrapper.find('textarea[name="schema"]').setValue('{"type":"string"}')
    await wrapper.find('button.check').trigger('click')
    await flushPromises()
    expect(wrapper.find('.check-ok').exists()).toBe(true)
    expect(wrapper.find('button.register').attributes('disabled')).toBeUndefined()
    await wrapper.find('button.register').trigger('click')
    await flushPromises()
    const call = vi.mocked(api).mock.calls[2]!
    expect(call[0]).toBe('/schemas/register')
    expect(JSON.parse((call[1] as RequestInit).body as string)).toMatchObject({ topic: 'orders', kind: 'key', schemaType: 'JSON' })
    expect(wrapper.find('.registered').text()).toContain('orders-key')
    expect(wrapper.find('.registered').text()).toContain('v1')
    expect(wrapper.emitted('registered')).toEqual([[{ subject: 'orders-key', id: 5, version: 1 }]])
  })

  it('입력을 바꾸면 검사 결과가 초기화된다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics).mockResolvedValueOnce({ compatible: true, messages: [] })
    const wrapper = mount(SchemaRegisterModal)
    await flushPromises()
    await wrapper.find('select[name="topic"]').setValue('orders')
    await wrapper.find('textarea[name="schema"]').setValue('{}')
    await wrapper.find('button.check').trigger('click')
    await flushPromises()
    expect(wrapper.find('button.register').attributes('disabled')).toBeUndefined()
    await wrapper.find('textarea[name="schema"]').setValue('{"a":1}')
    expect(wrapper.find('.check-ok').exists()).toBe(false)
    expect(wrapper.find('button.register').attributes('disabled')).toBeDefined()
  })

  it('토픽·종류가 고정되면 선택 UI 가 없고 검사 본문에 그 값이 들어간다', async () => {
    vi.mocked(api).mockResolvedValueOnce({ compatible: true, messages: [] })
    const wrapper = mount(SchemaRegisterModal, { props: { topic: 'orders', kind: 'value' } })
    await flushPromises()
    expect(wrapper.find('select[name="topic"]').exists()).toBe(false)
    expect(wrapper.find('input[name="kind"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('orders-value')
    await wrapper.find('textarea[name="schema"]').setValue('{}')
    await wrapper.find('button.check').trigger('click')
    await flushPromises()
    expect(JSON.parse((vi.mocked(api).mock.calls[0]![1] as RequestInit).body as string)).toMatchObject({ topic: 'orders', kind: 'value' })
  })

  it('등록 실패 메시지를 표시한다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(topics)
      .mockResolvedValueOnce({ compatible: true, messages: [] })
      .mockRejectedValueOnce(new Error('존재하지 않는 토픽입니다'))
    const wrapper = mount(SchemaRegisterModal)
    await flushPromises()
    await wrapper.find('select[name="topic"]').setValue('orders')
    await wrapper.find('textarea[name="schema"]').setValue('{}')
    await wrapper.find('button.check').trigger('click')
    await flushPromises()
    await wrapper.find('button.register').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('존재하지 않는 토픽입니다')
    expect(wrapper.emitted('registered')).toBeUndefined()
  })

  it('검사 응답이 늦게 도착해도 입력이 바뀌었으면 결과를 반영하지 않는다', async () => {
    let resolveCheck!: (v: unknown) => void
    vi.mocked(api)
      .mockResolvedValueOnce(topics)
      .mockImplementationOnce(() => new Promise((r) => { resolveCheck = r }))
    const wrapper = mount(SchemaRegisterModal)
    await flushPromises()
    await wrapper.find('select[name="topic"]').setValue('orders')
    await wrapper.find('textarea[name="schema"]').setValue('{}')
    await wrapper.find('button.check').trigger('click')
    await wrapper.vm.$nextTick()

    // 검사가 진행 중인 동안엔 입력을 바꿀 수 없다 — 검사 대상이 검사 도중 바뀌는 것을 막는다
    expect(wrapper.find('select[name="topic"]').attributes('disabled')).toBeDefined()
    expect(wrapper.find('input[name="kind"][value="key"]').attributes('disabled')).toBeDefined()
    expect(wrapper.find('select[name="schemaType"]').attributes('disabled')).toBeDefined()
    expect(wrapper.find('textarea[name="schema"]').attributes('disabled')).toBeDefined()

    resolveCheck({ compatible: true, messages: [] })
    await flushPromises()

    expect(wrapper.find('.check-ok').exists()).toBe(true)
    expect(wrapper.find('button.register').attributes('disabled')).toBeUndefined()
    // 검사가 끝나면 다시 입력할 수 있다
    expect(wrapper.find('textarea[name="schema"]').attributes('disabled')).toBeUndefined()

    await wrapper.find('textarea[name="schema"]').setValue('{"a":1}')
    expect(wrapper.find('.check-ok').exists()).toBe(false)
    expect(wrapper.find('button.register').attributes('disabled')).toBeDefined()
  })
})
