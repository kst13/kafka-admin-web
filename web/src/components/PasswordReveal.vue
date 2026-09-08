<script setup lang="ts">
import { ref } from 'vue'

defineProps<{ name: string; password: string }>()
const copied = ref(false)
const copyFailed = ref(false)
const pwEl = ref<HTMLElement | null>(null)

// HTTP(비보안 컨텍스트) 배포에서는 navigator.clipboard 가 undefined 라 writeText 가 존재하지 않는다.
// 그 경우 비밀번호 <code> 를 직접 선택해 execCommand('copy') 로 폴백한다.
async function copy(text: string) {
  copyFailed.value = false
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text)
      copied.value = true
      return
    } catch {
      // 폴백으로 진행
    }
  }
  const ok = selectAndCopyFallback()
  if (ok) {
    copied.value = true
  } else {
    copied.value = false
    copyFailed.value = true
  }
}

function selectAndCopyFallback(): boolean {
  const el = pwEl.value
  if (!el) return false
  const selection = window.getSelection()
  const range = document.createRange()
  range.selectNodeContents(el)
  selection?.removeAllRanges()
  selection?.addRange(range)
  try {
    return document.execCommand('copy')
  } catch {
    return false
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
        <code ref="pwEl" class="pw">{{ password }}</code>
        <button type="button" class="btn copy" @click="copy(password)">{{ copied ? '복사됨' : '복사' }}</button>
      </dd>
      <dt>SASL 설정</dt>
      <dd><code>security.protocol=SASL_SSL, sasl.mechanism=SCRAM-SHA-512</code></dd>
    </dl>
    <p v-if="copyFailed" class="copy-failed">복사 실패 — 비밀번호를 직접 드래그해 복사하세요</p>
  </div>
</template>

<style scoped>
.warn { color: var(--warn, #b7791f); margin: 0 0 0.75rem; }
dl { display: grid; grid-template-columns: max-content 1fr; gap: 0.4rem 1rem; margin: 0; }
dt { color: var(--ink-soft); }
dd { margin: 0; display: flex; align-items: center; gap: 0.5rem; flex-wrap: wrap; }
.pw { font-size: 1rem; letter-spacing: 0.04em; user-select: all; }
.copy { padding: 0.15rem 0.5rem; font-size: 0.8rem; }
.copy-failed { color: var(--crit, #c0392b); margin: 0.5rem 0 0; font-size: 0.85rem; }
</style>
