<script setup lang="ts">
import { onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { useSession } from '@/composables/useSession'

const route = useRoute()
const { load } = useSession()
onMounted(load)
</script>

<template>
  <nav v-if="route.path !== '/login'">
    <RouterLink to="/">클러스터</RouterLink>
    <RouterLink to="/topics">토픽</RouterLink>
    <RouterLink to="/groups">컨슈머 그룹</RouterLink>
    <RouterLink to="/alerts">알림</RouterLink>
  </nav>
  <RouterView />
</template>

<style scoped>
nav { display: flex; gap: 1rem; padding: 1rem; border-bottom: 1px solid #ddd; }
</style>
