<script setup lang="ts">
import { ref } from 'vue'

defineProps<{ name: string; password: string }>()
const copied = ref(false)

async function copy(text: string) {
  try {
    await navigator.clipboard.writeText(text)
    copied.value = true
  } catch {
    copied.value = false
  }
}
</script>

<template>
  <div class="reveal">
    <p class="warn">아래 비밀번호는 지금 한 번만 표시됩니다. <strong>닫으면 다시 볼 수 없습니다.</strong></p>
    <dl>
      <dt>사용자명</dt>
      <dd><code>{{ name }}</code></dd>
      <dt>비밀번호</dt>
      <dd>
        <code class="pw">{{ password }}</code>
        <button type="button" class="btn copy" @click="copy(password)">{{ copied ? '복사됨' : '복사' }}</button>
      </dd>
      <dt>SASL 설정</dt>
      <dd><code>security.protocol=SASL_SSL, sasl.mechanism=SCRAM-SHA-512</code></dd>
    </dl>
  </div>
</template>

<style scoped>
.warn { color: var(--warn, #b7791f); margin: 0 0 0.75rem; }
dl { display: grid; grid-template-columns: max-content 1fr; gap: 0.4rem 1rem; margin: 0; }
dt { color: var(--ink-soft); }
dd { margin: 0; display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap; }
.pw { font-size: 1rem; letter-spacing: 0.04em; user-select: all; }
.copy { padding: 0.15rem 0.5rem; font-size: 0.8rem; }
</style>
