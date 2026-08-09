<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { api } from '@/api/client'

const router = useRouter()
const username = ref('')
const password = ref('')
const error = ref('')

async function login() {
  error.value = ''
  try {
    await api('/auth/login', {
      method: 'POST',
      body: JSON.stringify({ username: username.value, password: password.value }),
    })
    router.push('/')
  } catch {
    error.value = '아이디 또는 비밀번호가 올바르지 않습니다'
  }
}
</script>

<template>
  <main class="login">
    <h1>Kafka Admin</h1>
    <form @submit.prevent="login">
      <input v-model="username" placeholder="아이디" autocomplete="username" />
      <input v-model="password" type="password" placeholder="비밀번호"
             autocomplete="current-password" />
      <button type="submit">로그인</button>
      <p v-if="error" class="error">{{ error }}</p>
    </form>
  </main>
</template>

<style scoped>
.login {
  max-width: 340px;
  margin: 15vh auto;
  display: flex;
  flex-direction: column;
  gap: 1rem;
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 8px;
  box-shadow: var(--shadow);
  padding: 1.75rem;
}
.login h1 { text-align: center; margin-bottom: 0.25rem; }
form { display: flex; flex-direction: column; gap: 0.6rem; }
</style>
