<script setup lang="ts">
import { computed, ref } from 'vue'
import { toPolyline } from '@/lib/trend'

const props = defineProps<{ points: { t: string; v: number }[] }>()

const W = 320
const H = 80
const PAD = 8
const hoverIndex = ref<number | null>(null)

const values = computed(() => props.points.map((p) => p.v))
const polyline = computed(() => toPolyline(values.value, W, H, PAD))
const last = computed(() => props.points.at(-1))
const hovered = computed(() =>
  hoverIndex.value === null ? null : props.points[hoverIndex.value],
)

function xOf(i: number): number {
  const n = props.points.length
  return n < 2 ? W / 2 : PAD + ((W - PAD * 2) * i) / (n - 1)
}
// toPolyline의 평평한 선(값 1개/전부 동일) 분기와 반드시 같은 y를 반환해야
// 호버 마커가 선 위에 정확히 얹힌다.
function yOf(i: number): number {
  const vs = values.value
  const max = Math.max(...vs)
  const min = Math.min(...vs)
  if (vs.length === 1 || max === min) return H / 2
  const v = vs[i] ?? 0
  return PAD + (H - PAD * 2) - ((H - PAD * 2) * v) / max
}
function onMove(e: MouseEvent) {
  if (props.points.length === 0) return
  const rect = (e.currentTarget as SVGElement).getBoundingClientRect()
  const x = ((e.clientX - rect.left) / rect.width) * W
  const n = props.points.length
  const i = Math.round(((x - PAD) / (W - PAD * 2)) * (n - 1))
  hoverIndex.value = Math.min(Math.max(i, 0), n - 1)
}
</script>

<template>
  <div class="trend">
    <svg
      :viewBox="`0 0 ${W} ${H}`"
      role="img"
      aria-label="랙 추이 차트"
      @mousemove="onMove"
      @mouseleave="hoverIndex = null"
    >
      <polyline :points="polyline" fill="none" stroke="#1d4ed8" stroke-width="2" />
      <circle
        v-if="hoverIndex !== null"
        :cx="xOf(hoverIndex)"
        :cy="yOf(hoverIndex)"
        r="4"
        fill="#1d4ed8"
      />
    </svg>
    <p class="reading">
      <template v-if="hovered">
        {{ new Date(hovered.t).toLocaleTimeString() }} — {{ hovered.v.toLocaleString() }}
      </template>
      <template v-else-if="last"> 현재 {{ last.v.toLocaleString() }} </template>
      <template v-else> 데이터 없음 </template>
    </p>
  </div>
</template>

<style scoped>
.trend svg { width: 100%; max-width: 480px; display: block; }
.reading { margin: 0.25rem 0 0; font-size: 0.85rem; color: #555; }
</style>
