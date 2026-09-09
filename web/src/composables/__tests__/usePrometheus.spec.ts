import { describe, it, expect, vi, beforeEach } from 'vitest'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import { usePrometheus } from '../usePrometheus'

describe('usePrometheus', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    usePrometheus().setStatus(null)
  })

  it('설정되어 있으면 configured 와 healthy 를 노출한다', async () => {
    vi.mocked(api).mockResolvedValue({ configured: true, url: 'http://prom:9090', healthy: true })
    const { load, configured, healthy } = usePrometheus()
    await load()
    expect(configured.value).toBe(true)
    expect(healthy.value).toBe(true)
    expect(api).toHaveBeenCalledWith('/prometheus/status')
  })

  it('미설정이거나 조회 실패면 configured 가 false', async () => {
    vi.mocked(api).mockResolvedValue({ configured: false, url: '', healthy: false })
    const { load, configured } = usePrometheus()
    await load()
    expect(configured.value).toBe(false)
    vi.mocked(api).mockRejectedValue(new Error('401'))
    await load()
    expect(configured.value).toBe(false)
  })
})
