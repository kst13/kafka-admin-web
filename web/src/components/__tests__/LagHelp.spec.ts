import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import LagHelp from '../LagHelp.vue'

describe('LagHelp', () => {
  it('처음에는 팝업이 보이지 않는다', () => {
    const wrapper = mount(LagHelp)
    expect(wrapper.find('[role="dialog"]').exists()).toBe(false)
  })

  it('버튼을 누르면 랙 안내 팝업이 열린다', async () => {
    const wrapper = mount(LagHelp)
    await wrapper.find('button.help-btn').trigger('click')
    const dialog = wrapper.find('[role="dialog"]')
    expect(dialog.exists()).toBe(true)
    expect(dialog.text()).toContain('최신 오프셋 − 커밋 오프셋')
  })

  it('닫기 버튼을 누르면 팝업이 닫힌다', async () => {
    const wrapper = mount(LagHelp)
    await wrapper.find('button.help-btn').trigger('click')
    await wrapper.find('button.close-btn').trigger('click')
    expect(wrapper.find('[role="dialog"]').exists()).toBe(false)
  })

  it('배경을 누르면 팝업이 닫히지만 본문 클릭은 닫히지 않는다', async () => {
    const wrapper = mount(LagHelp)
    await wrapper.find('button.help-btn').trigger('click')
    await wrapper.find('[role="dialog"]').trigger('click')
    expect(wrapper.find('[role="dialog"]').exists()).toBe(true)
    await wrapper.find('.overlay').trigger('click')
    expect(wrapper.find('[role="dialog"]').exists()).toBe(false)
  })
})
