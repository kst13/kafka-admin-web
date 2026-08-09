<script setup lang="ts">
import { onMounted, onUnmounted } from 'vue'

defineProps<{ title: string }>()
const emit = defineEmits<{ close: [] }>()

function onKeydown(e: KeyboardEvent) {
  if (e.key === 'Escape') emit('close')
}
onMounted(() => document.addEventListener('keydown', onKeydown))
onUnmounted(() => document.removeEventListener('keydown', onKeydown))
</script>

<template>
  <div class="overlay" @click.self="emit('close')">
    <div class="dialog" role="dialog" aria-modal="true">
      <div class="dialog-head">
        <h2>{{ title }}</h2>
        <button class="close-btn" type="button" aria-label="닫기" @click="emit('close')">✕</button>
      </div>
      <div class="dialog-body">
        <slot />
      </div>
      <div class="dialog-foot">
        <slot name="footer" />
      </div>
    </div>
  </div>
</template>

<style scoped>
.overlay {
  position: fixed;
  inset: 0;
  background: rgba(10, 18, 23, 0.55);
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1.5rem;
  z-index: 10;
}
.dialog {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 8px;
  max-width: 480px;
  width: 100%;
  max-height: 80vh;
  display: flex;
  flex-direction: column;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.35);
}
.dialog-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem 1.25rem;
  border-bottom: 1px solid var(--line);
}
.dialog-head h2 {
  font-size: 1.05rem;
  margin: 0;
}
.close-btn {
  border: none;
  background: none;
  font-size: 1rem;
  color: var(--ink-soft);
  padding: 0.25rem 0.5rem;
}
.close-btn:hover {
  color: var(--ink);
}
.dialog-body {
  padding: 1rem 1.25rem;
  overflow-y: auto;
  font-size: 0.9rem;
  line-height: 1.65;
}
.dialog-foot {
  padding: 0.75rem 1.25rem 1rem;
  display: flex;
  justify-content: flex-end;
  gap: 0.5rem;
}
</style>
