import { createRouter, createWebHistory } from 'vue-router'
import { api, UnauthorizedError } from '@/api/client'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    { path: '/login', component: () => import('@/views/LoginView.vue') },
    { path: '/', component: () => import('@/views/ClusterView.vue') },
    { path: '/topics', component: () => import('@/views/TopicsView.vue') },
    { path: '/topics/:name', component: () => import('@/views/TopicDetailView.vue') },
    { path: '/groups', component: () => import('@/views/GroupsView.vue') },
    { path: '/groups/:groupId', component: () => import('@/views/GroupDetailView.vue') },
    { path: '/alerts', component: () => import('@/views/AlertsView.vue') },
  ],
})

router.beforeEach(async (to) => {
  if (to.path === '/login') return true
  try {
    await api('/auth/me')
    return true
  } catch (e) {
    if (e instanceof UnauthorizedError) return '/login'
    return true // 브로커/서버 오류는 각 화면이 표시한다
  }
})

export default router
