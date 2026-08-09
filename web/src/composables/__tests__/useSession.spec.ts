import { describe, it, expect, vi, beforeEach } from 'vitest'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import { useSession } from '../useSession'

describe('useSession', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
  })

  it('ADMIN 세션이면 isAdmin 이 true', async () => {
    vi.mocked(api).mockResolvedValue({ username: 'a', role: 'ADMIN' })
    const { load, isAdmin } = useSession()
    await load()
    expect(isAdmin.value).toBe(true)
  })

  it('DEVELOPER 세션이면 isAdmin 이 false', async () => {
    vi.mocked(api).mockResolvedValue({ username: 'd', role: 'DEVELOPER' })
    const { load, isAdmin } = useSession()
    await load()
    expect(isAdmin.value).toBe(false)
  })

  it('조회 실패면 세션 없음으로 처리', async () => {
    vi.mocked(api).mockRejectedValue(new Error('401'))
    const { load, session, isAdmin } = useSession()
    await load()
    expect(session.value).toBeNull()
    expect(isAdmin.value).toBe(false)
  })
})
