export class UnauthorizedError extends Error {}

// 409(비호환) 등 오류 응답의 details[](사유 목록)를 호출부에서 쓸 수 있도록 status·details 를 함께 담는다.
export class ApiError extends Error {
  constructor(message: string, public readonly status: number, public readonly details: string[] = []) {
    super(message)
  }
}

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...init,
  })
  if (res.status === 401) throw new UnauthorizedError()
  if (!res.ok) {
    const body = await res.json().catch(() => ({ error: `HTTP ${res.status}` }))
    throw new ApiError(body.error ?? `HTTP ${res.status}`, res.status, Array.isArray(body.details) ? body.details : [])
  }
  if (res.status === 204) return undefined as T
  return res.json()
}
