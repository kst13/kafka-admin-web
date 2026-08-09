import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

const push = vi.fn()
vi.mock('vue-router', () => ({ useRouter: () => ({ push }) }))

const load = vi.fn()
vi.mock('@/composables/useSession', () => ({ useSession: () => ({ load }) }))

import { api } from '@/api/client'
import LoginView from '../LoginView.vue'

describe('LoginView', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    push.mockReset()
    load.mockReset()
  })

  it('로그인 성공 시 세션을 재조회한 뒤 라우터를 이동한다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(LoginView)
    await wrapper.find('input[placeholder="아이디"]').setValue('admin')
    await wrapper.find('input[placeholder="비밀번호"]').setValue('secret')
    await wrapper.find('form').trigger('submit')
    await flushPromises()

    expect(load).toHaveBeenCalledTimes(1)
    expect(push).toHaveBeenCalledWith('/')
    const loadOrder = load.mock.invocationCallOrder[0]!
    const pushOrder = push.mock.invocationCallOrder[0]!
    expect(loadOrder).toBeLessThan(pushOrder)
  })

  it('로그인 실패 시 세션을 재조회하지 않고 에러 메시지를 보여준다', async () => {
    vi.mocked(api).mockRejectedValue(new Error('401'))
    const wrapper = mount(LoginView)
    await wrapper.find('input[placeholder="아이디"]').setValue('admin')
    await wrapper.find('input[placeholder="비밀번호"]').setValue('wrong')
    await wrapper.find('form').trigger('submit')
    await flushPromises()

    expect(load).not.toHaveBeenCalled()
    expect(push).not.toHaveBeenCalled()
    expect(wrapper.text()).toContain('아이디 또는 비밀번호가 올바르지 않습니다')
  })
})
