<script setup lang="ts">
import { useRoute } from 'vue-router'
const route = useRoute()
</script>

<template>
  <nav v-if="route.path !== '/login'">
    <span class="brand">Kafka Admin</span>
    <RouterLink to="/">클러스터</RouterLink>
    <RouterLink to="/topics">토픽</RouterLink>
    <RouterLink to="/groups">컨슈머 그룹</RouterLink>
    <RouterLink to="/alerts">알림</RouterLink>
  </nav>
  <RouterView />
</template>

<style scoped>
nav {
  display: flex;
  align-items: center;
  gap: 0.25rem;
  padding: 0.75rem 0;
  border-bottom: 1px solid var(--line);
}
.brand {
  font-weight: 700;
  font-size: 0.95rem;
  color: var(--accent);
  margin-right: 1rem;
  letter-spacing: 0.02em;
}
nav a {
  padding: 0.35rem 0.8rem;
  border-radius: 6px;
  color: var(--ink-soft);
  font-size: 0.92rem;
  font-weight: 600;
}
nav a:hover {
  background: var(--surface-2);
  text-decoration: none;
}
/* "/"(클러스터)는 정확히 일치할 때만, 나머지는 하위 경로(/topics/xxx)에서도 활성 표시 */
nav a.router-link-exact-active,
nav a.router-link-active:not([href='/']) {
  color: var(--accent);
  background: var(--accent-soft);
}
</style>
