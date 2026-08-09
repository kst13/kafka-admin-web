import { ref, computed } from 'vue'
import { api } from '@/api/client'

export interface Session {
  username: string
  role: 'ADMIN' | 'DEVELOPER'
}

// 모듈 스코프 싱글턴: 어느 컴포넌트에서 불러도 같은 세션 상태를 본다
const session = ref<Session | null>(null)

export function useSession() {
  async function load() {
    try {
      session.value = await api<Session>('/auth/me')
    } catch {
      session.value = null
    }
  }
  const isAdmin = computed(() => session.value?.role === 'ADMIN')
  return { session, isAdmin, load }
}
