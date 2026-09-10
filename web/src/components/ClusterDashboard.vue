<script setup lang="ts">
import { computed, ref, onMounted, onUnmounted } from 'vue'
import { api } from '@/api/client'
interface RankedValue { name: string; value: number }
interface Dashboard {
  sampledAt: string | null; stale: boolean; incomingPerSec: number | null
  laggingGroups: number | null; underReplicated: number | null; maxDiskUsedPct: number | null
  lagTop: RankedValue[]; growingLagTop: RankedValue[]; incomingTop: RankedValue[]
}
const emit = defineEmits<{ refresh: [] }>()
const data = ref<Dashboard | null>(null)
const error = ref('')
const loading = ref(false)
const autoRefresh = ref(true)
const refreshedAt = ref<Date | null>(null)
let timer: ReturnType<typeof setInterval> | undefined
let disposed = false
const number = (v: number | null | undefined, digits = 0) => v == null ? '—' : v.toLocaleString('ko-KR', { maximumFractionDigits: digits })
const rankings = computed(() => [
  { title: '적체 그룹 TOP 5', items: data.value?.lagTop ?? [], path: 'groups', unit: '건', digits: 0 },
  { title: '랙 증가 TOP 5', items: data.value?.growingLagTop ?? [], path: 'groups', unit: '건/초', digits: 1 },
  { title: '유입 토픽 TOP 5', items: data.value?.incomingTop ?? [], path: 'topics', unit: '건/초', digits: 1 },
])
async function refresh(manual = false) {
  if (loading.value || disposed) return
  loading.value = true
  if (manual) emit('refresh')
  try {
    const result = await api<Dashboard>('/dashboard')
    if (disposed) return
    data.value = result
    error.value = ''
    refreshedAt.value = new Date()
  } catch {
    if (!disposed) error.value = '통계를 갱신하지 못했습니다. 이전 값이 있다면 유지합니다.'
  } finally { loading.value = false }
}
onMounted(() => {
  refresh()
  timer = setInterval(() => {
    if (autoRefresh.value && document.visibilityState !== 'hidden') refresh()
  }, 60_000)
})
onUnmounted(() => { disposed = true; clearInterval(timer) })
</script>

<template>
  <section class="dashboard" aria-label="클러스터 통계">
    <div class="toolbar">
      <div><h2>운영 요약</h2><p class="hint">문제가 있는 대상을 선택하면 상세 화면으로 이동합니다.</p></div>
      <div class="controls">
        <label><input v-model="autoRefresh" type="checkbox" /> 요약 60초 자동 갱신</label>
        <button type="button" :disabled="loading" @click="refresh(true)">{{ loading ? '갱신 중…' : '새로고침' }}</button>
      </div>
    </div>
    <p class="hint">요약 갱신: {{ refreshedAt?.toLocaleTimeString() ?? '—' }} · 지표 수집: {{ data?.sampledAt ? new Date(data.sampledAt).toLocaleString() : '아직 없음' }}</p>
    <p v-if="error" role="alert" class="warning">{{ error }}</p>
    <p v-else-if="data?.stale" class="warning">{{ data.sampledAt ? '수집 후 3분 이상 지났습니다. 마지막 수집값입니다.' : '최근 5분 내 수집 이력이 없습니다. 수집 상태를 확인하세요.' }}</p>
    <div class="cards">
      <article><span>전체 유입 추정</span><strong>{{ number(data?.incomingPerSec, 1) }} <small>건/초</small></strong></article>
      <article><span>적체 그룹</span><strong :class="{ warning: (data?.laggingGroups ?? 0) > 0 }">{{ number(data?.laggingGroups) }} <small>개</small></strong><p>랙이 0보다 큰 그룹</p></article>
      <article><span>미복제 파티션</span><strong :class="{ warning: (data?.underReplicated ?? 0) > 0 }">{{ number(data?.underReplicated) }} <small>개</small></strong><p>정상 기준 0개</p></article>
      <article><span>디스크 최고 사용률</span><strong>{{ number(data?.maxDiskUsedPct, 1) }} <small>%</small></strong></article>
    </div>
    <div class="rankings">
      <article v-for="ranking in rankings" :key="ranking.title">
        <h3>{{ ranking.title }}</h3>
        <ol v-if="ranking.items.length">
          <li v-for="item in ranking.items" :key="item.name">
            <RouterLink :to="`/${ranking.path}/${encodeURIComponent(item.name)}`">{{ item.name }}</RouterLink>
            <span>{{ number(item.value, ranking.digits) }} <small>{{ ranking.unit }}</small></span>
          </li>
        </ol>
        <p v-else class="hint">{{ !data?.sampledAt ? '수집 데이터 없음' : '표시할 양수 값이 없거나 비교 샘플이 부족합니다.' }}</p>
      </article>
    </div>
    <p class="hint">유입·랙 증가는 최근 두 수집 시점 사이의 초당 변화입니다. 유입은 오프셋 기반 추정치이며, 비교 이력 부족·오프셋 감소 시 전체 유입은 —로 표시합니다.</p>
  </section>
</template>

<style scoped>
.dashboard { margin: 1rem 0 2rem; }
.toolbar, .controls { display: flex; align-items: center; justify-content: space-between; gap: 1rem; flex-wrap: wrap; }
h2 { margin: 0; }
.hint, article p, small { color: var(--ink-soft); font-size: 0.8rem; font-weight: normal; }
.controls { font-size: 0.85rem; }
button { background: var(--surface); color: var(--ink); border: 1px solid var(--line); border-radius: 6px; padding: 0.4rem 0.8rem; cursor: pointer; }
button:disabled { opacity: 0.6; cursor: wait; }
.cards { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 0.8rem; margin: 1rem 0; }
article { background: var(--surface); border: 1px solid var(--line); border-radius: 10px; padding: 1rem; }
.cards article > span { color: var(--ink-soft); font-size: 0.9rem; }
strong { display: block; font-size: 1.8rem; font-variant-numeric: tabular-nums; }
.warning { color: var(--crit); }
.rankings { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 0.8rem; margin-bottom: 0.75rem; }
h3 { font-size: 1rem; margin-bottom: 0.75rem; }
ol { padding-left: 1.2rem; }
li { padding: 0.45rem 0; border-bottom: 1px solid var(--line); }
li a { overflow-wrap: anywhere; }
li > span { display: block; text-align: right; font-variant-numeric: tabular-nums; }
@media (max-width: 900px) { .cards { grid-template-columns: repeat(2, minmax(0, 1fr)); } .rankings { grid-template-columns: 1fr; } }
@media (max-width: 480px) { .cards { grid-template-columns: 1fr; } }
</style>
