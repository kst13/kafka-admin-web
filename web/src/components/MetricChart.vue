<script setup lang="ts">
import { computed, ref } from 'vue'
import { formatValue, type Series } from '@/lib/metrics'

const props = defineProps<{ series: Series[]; unit: string; title: string }>()

const W = 640
const H = 200
const PAD = 8
const COLORS = ['var(--accent)', 'var(--warn)', 'var(--ok)', 'var(--crit)']

const hoverIndex = ref<number | null>(null)

// x 축은 모든 시리즈의 시각 합집합(정렬). 시리즈마다 같은 시각의 값을 찾는다 (없으면 선을 끊지 않고 건너뜀).
const times = computed(() => {
  const set = new Set<string>()
  for (const s of props.series) for (const p of s.points) set.add(p.t)
  return [...set].sort((a, b) => Date.parse(a) - Date.parse(b))
})
const hasData = computed(() => times.value.length > 0)
const max = computed(() => {
  let m = 0
  for (const s of props.series) for (const p of s.points) if (p.v > m) m = p.v
  return m
})

function xOf(i: number): number {
  const n = times.value.length
  return n < 2 ? W / 2 : PAD + ((W - PAD * 2) * i) / (n - 1)
}
function yOf(v: number): number {
  const innerH = H - PAD * 2
  if (max.value === 0) return PAD + innerH
  return PAD + innerH - (innerH * v) / max.value
}

const lines = computed(() =>
  props.series.map((s, si) => {
    const byTime = new Map(s.points.map((p) => [p.t, p.v]))
    const pts: string[] = []
    times.value.forEach((t, i) => {
      const v = byTime.get(t)
      if (v !== undefined) pts.push(`${xOf(i)},${yOf(v)}`)
    })
    const lastPoint = s.points[s.points.length - 1]
    return {
      name: s.name,
      color: COLORS[si % COLORS.length] ?? COLORS[0]!,
      points: pts.join(' '),
      last: lastPoint ? formatValue(lastPoint.v, props.unit) : null,
    }
  }),
)

const hovered = computed(() => {
  if (hoverIndex.value === null) return null
  const t = times.value[hoverIndex.value]
  if (t === undefined) return null
  return {
    t,
    values: props.series.map((s) => ({ name: s.name, v: s.points.find((p) => p.t === t)?.v ?? null })),
  }
})

function onMove(e: MouseEvent) {
  const n = times.value.length
  if (n === 0) return
  const rect = (e.currentTarget as SVGElement).getBoundingClientRect()
  const x = ((e.clientX - rect.left) / rect.width) * W
  const i = Math.round(((x - PAD) / (W - PAD * 2)) * (n - 1))
  hoverIndex.value = Math.min(Math.max(i, 0), n - 1)
}
</script>

<template>
  <div class="metric-chart">
    <div class="chart-head">
      <h3>{{ title }}</h3>
      <ul class="legend">
        <li v-for="l in lines" :key="l.name" class="legend-item">
          <span class="swatch" :style="{ background: l.color }"></span>
          {{ l.name }}
          <span v-if="l.last !== null" class="legend-value">{{ l.last }}</span>
        </li>
      </ul>
    </div>
    <div v-if="hasData" class="chart-body">
      <div class="y-axis">
        <span class="y-max">{{ formatValue(max, unit) }}</span>
        <span class="y-min">{{ formatValue(0, unit) }}</span>
      </div>
      <svg :viewBox="`0 0 ${W} ${H}`" role="img" :aria-label="title" @mousemove="onMove" @mouseleave="hoverIndex = null">
        <!-- stroke 속성은 CSS 변수를 못 받으므로 style 로 지정 -->
        <polyline v-for="l in lines" :key="l.name" :points="l.points" fill="none" stroke-width="2" :style="{ stroke: l.color }" />
        <line v-if="hoverIndex !== null" class="cursor" :x1="xOf(hoverIndex)" :x2="xOf(hoverIndex)" :y1="PAD" :y2="H - PAD" />
      </svg>
    </div>
    <p v-else class="empty">데이터 없음</p>
    <p v-if="hovered" class="reading">
      {{ new Date(hovered.t).toLocaleTimeString() }}
      <span v-for="v in hovered.values" :key="v.name" class="reading-item">
        {{ v.name }}: {{ v.v === null ? '—' : formatValue(v.v, unit) }}
      </span>
    </p>
  </div>
</template>

<style scoped>
.metric-chart { margin-bottom: 1rem; }
.chart-head { display: flex; align-items: baseline; justify-content: space-between; gap: 1rem; flex-wrap: wrap; }
.chart-head h3 { margin: 0 0 0.25rem; font-size: 0.95rem; }
.legend { list-style: none; display: flex; gap: 0.75rem; margin: 0; padding: 0; font-size: 0.8rem; color: var(--ink-soft); }
.legend-item { display: flex; align-items: center; gap: 0.3rem; }
.legend-value { color: var(--ink); font-variant-numeric: tabular-nums; }
.swatch { width: 10px; height: 10px; border-radius: 2px; display: inline-block; }
.chart-body { display: flex; gap: 0.4rem; }
.y-axis { display: flex; flex-direction: column; justify-content: space-between; font-size: 0.7rem; color: var(--ink-soft); white-space: nowrap; }
svg { flex: 1; min-width: 0; height: auto; display: block; background: var(--surface); border: 1px solid var(--line); border-radius: 6px; }
.cursor { stroke: var(--ink-soft); stroke-dasharray: 3 3; }
.empty, .reading { margin: 0.25rem 0 0; font-size: 0.85rem; color: var(--ink-soft); }
.reading-item { margin-left: 0.75rem; color: var(--ink); }
</style>
