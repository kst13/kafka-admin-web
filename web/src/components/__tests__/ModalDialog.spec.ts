import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import ModalDialog from '../ModalDialog.vue'

describe('ModalDialog', () => {
  it('제목과 본문 슬롯을 렌더링한다', () => {
    const wrapper = mount(ModalDialog, {
      props: { title: '토픽 생성' },
      slots: { default: '<p>본문</p>' },
    })
    expect(wrapper.find('[role="dialog"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('토픽 생성')
    expect(wrapper.text()).toContain('본문')
  })

  it('오버레이 클릭과 닫기 버튼이 close 를 emit 한다', async () => {
    const wrapper = mount(ModalDialog, { props: { title: 't' } })
    await wrapper.find('.overlay').trigger('click')
    await wrapper.find('.close-btn').trigger('click')
    expect(wrapper.emitted('close')).toHaveLength(2)
  })

  it('다이얼로그 내부 클릭은 close 를 emit 하지 않는다', async () => {
    const wrapper = mount(ModalDialog, { props: { title: 't' } })
    await wrapper.find('.dialog').trigger('click')
    expect(wrapper.emitted('close')).toBeUndefined()
  })
})
