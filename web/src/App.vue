<script setup lang="ts">
import { useRoute } from 'vue-router'
const route = useRoute()
</script>

<template>
  <nav v-if="route.path !== '/login'">
    <RouterLink to="/">클러스터</RouterLink>
    <RouterLink to="/topics">토픽</RouterLink>
    <RouterLink to="/groups">컨슈머 그룹</RouterLink>
  </nav>
  <RouterView />
</template>

<style scoped>
nav { display: flex; gap: 1rem; padding: 1rem; border-bottom: 1px solid #ddd; }
</style>
