import { describe, it, expect, vi, afterEach } from 'vitest'
import { mount } from '@vue/test-utils'
import PasswordReveal from '../PasswordReveal.vue'

describe('PasswordReveal', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    vi.restoreAllMocks()
  })

  it('클립보드 API 가 있으면 writeText 로 복사하고 버튼이 복사됨으로 바뀐다', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', {
      value: { writeText },
      configurable: true,
    })

    const wrapper = mount(PasswordReveal, { props: { name: 'order-api', password: 'Secret123Secret123' } })
    await wrapper.find('button.copy').trigger('click')
    await wrapper.vm.$nextTick()
    await Promise.resolve()
    await wrapper.vm.$nextTick()

    expect(writeText).toHaveBeenCalledWith('Secret123Secret123')
    expect(wrapper.find('button.copy').text()).toBe('복사됨')

    // @ts-expect-error test cleanup
    delete navigator.clipboard
  })

  it('클립보드 API 가 없고 execCommand 도 실패하면 복사 실패 안내를 보여준다', async () => {
    Object.defineProperty(navigator, 'clipboard', {
      value: undefined,
      configurable: true,
    })
    document.execCommand = vi.fn().mockReturnValue(false)

    const wrapper = mount(PasswordReveal, { props: { name: 'order-api', password: 'Secret123Secret123' } })
    await wrapper.find('button.copy').trigger('click')
    await wrapper.vm.$nextTick()

    expect(wrapper.find('.copy-failed').exists()).toBe(true)
    expect(wrapper.find('.copy-failed').text()).toContain('복사 실패')
    expect(wrapper.find('button.copy').text()).toBe('복사')

    // @ts-expect-error test cleanup
    delete navigator.clipboard
  })
})
