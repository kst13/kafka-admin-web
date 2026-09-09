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

  it('ready() 는 status 가 이미 있으면 api 를 다시 호출하지 않는다', async () => {
    vi.mocked(api).mockResolvedValue({ configured: true, url: 'http://prom:9090', healthy: true })
    const { load, ready } = usePrometheus()
    await load()
    vi.mocked(api).mockClear()
    await ready()
    expect(api).not.toHaveBeenCalled()
  })

  it('ready() 는 status 가 없으면 api 를 호출해 로드를 기다린다', async () => {
    vi.mocked(api).mockResolvedValue({ configured: true, url: 'http://prom:9090', healthy: true })
    const { ready, configured } = usePrometheus()
    await ready()
    expect(api).toHaveBeenCalledTimes(1)
    expect(configured.value).toBe(true)
  })

  it('동시에 load() 를 두 번 호출해도 요청은 한 번만 나간다', async () => {
    let resolveApi!: (v: { configured: boolean; url: string; healthy: boolean }) => void
    vi.mocked(api).mockImplementation(
      () => new Promise((resolve) => { resolveApi = resolve }),
    )
    const { load, configured } = usePrometheus()
    const p1 = load()
    const p2 = load()
    expect(api).toHaveBeenCalledTimes(1)
    resolveApi({ configured: true, url: 'http://prom:9090', healthy: true })
    await Promise.all([p1, p2])
    expect(configured.value).toBe(true)
  })
})
