import { describe, it, expect, vi, beforeEach } from 'vitest'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import { useSchemaRegistry } from '../useSchemaRegistry'

describe('useSchemaRegistry', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    useSchemaRegistry().setStatus(null)
  })

  it('설정되어 있으면 configured 와 전역 호환성을 노출한다', async () => {
    vi.mocked(api).mockResolvedValue({ configured: true, urls: ['http://sr'], globalCompatibility: 'BACKWARD' })
    const { load, configured, globalCompatibility } = useSchemaRegistry()
    await load()
    expect(configured.value).toBe(true)
    expect(globalCompatibility.value).toBe('BACKWARD')
    expect(api).toHaveBeenCalledWith('/schemas/status')
  })

  it('미설정이거나 조회 실패면 configured 가 false', async () => {
    vi.mocked(api).mockResolvedValue({ configured: false, urls: [], globalCompatibility: null })
    const { load, configured } = useSchemaRegistry()
    await load()
    expect(configured.value).toBe(false)
    vi.mocked(api).mockRejectedValue(new Error('401'))
    await load()
    expect(configured.value).toBe(false)
  })

  it('setStatus 로 전역 호환성 변경을 반영한다', () => {
    const { setStatus, globalCompatibility } = useSchemaRegistry()
    setStatus({ configured: true, urls: [], globalCompatibility: 'FULL' })
    expect(globalCompatibility.value).toBe('FULL')
  })
})
