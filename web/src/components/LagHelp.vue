<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'

const open = ref(false)

function onKeydown(e: KeyboardEvent) {
  if (e.key === 'Escape') open.value = false
}
onMounted(() => document.addEventListener('keydown', onKeydown))
onUnmounted(() => document.removeEventListener('keydown', onKeydown))
</script>

<template>
  <button class="help-btn" type="button" @click="open = true">랙이란?</button>
  <div v-if="open" class="overlay" @click.self="open = false">
    <div class="dialog" role="dialog" aria-modal="true" aria-labelledby="lag-help-title">
      <div class="dialog-head">
        <h2 id="lag-help-title">랙(Lag) 이해하기</h2>
        <button class="close-btn" type="button" aria-label="닫기" @click="open = false">✕</button>
      </div>
      <div class="dialog-body">
        <p>
          랙은 <strong>컨슈머가 아직 읽지 못하고 밀려 있는 메시지 수</strong>다. 프로듀서가
          파티션에 쌓은 위치와 컨슈머가 읽은 위치의 차이로 계산한다.
        </p>
        <p class="formula">랙 = 최신 오프셋 − 커밋 오프셋</p>

        <h3>읽는 법</h3>
        <ul>
          <li><strong>0 또는 일정 수준 유지</strong> — 컨슈머가 생산 속도를 따라잡고 있다. 정상.</li>
          <li>
            <strong>계속 증가</strong> — 컨슈머가 못 따라가고 있다. 컨슈머 장애·처리 지연·트래픽
            급증의 신호.
          </li>
          <li>
            <strong>특정 파티션만 증가</strong> — 핫 파티션(키 쏠림) 또는 처리 실패 메시지에서 멈춘
            컨슈머. 아래 표의 파티션별 분포로 확인한다.
          </li>
        </ul>

        <h3>애플리케이션에서 해결하기 (우선순위 순)</h3>
        <ol>
          <li>
            <strong>처리 로직 최적화</strong> — 랙의 원인은 대부분 다운스트림(DB·외부 API)이다.
            건별 처리를 poll 배치 단위 bulk 처리로 바꾸는 것이 효과가 가장 크다.
          </li>
          <li>
            <strong>컨슈머 스케일 아웃</strong> — 같은 그룹에 인스턴스를 추가한다. 단, 컨슈머 수는
            파티션 수를 넘으면 의미가 없다(부족하면 파티션 증가 선행).
          </li>
          <li>
            <strong>리밸런스·설정 튜닝</strong> — 배치 처리 시간에 맞춰
            <code>max.poll.interval.ms</code>를 늘리고, CooperativeStickyAssignor·정적 멤버십으로
            리밸런스 중단을 줄인다.
          </li>
          <li>
            <strong>키 설계·포이즌 메시지</strong> — 핫 파티션은 파티션 키 재설계, 처리 실패
            메시지는 재시도 제한 + DLQ 토픽으로 격리한다.
          </li>
        </ol>

        <p class="note">
          이 사이트는 감지·진단 정보 제공까지만 담당한다. 랙 해소는 컨슈머 애플리케이션의 영역이며,
          오프셋 리셋은 밀린 메시지를 처리하는 것이 아니라 건너뛰거나 되감는 최후 수단이다.
        </p>
      </div>
    </div>
  </div>
</template>

<style scoped>
.help-btn {
  padding: 0.35rem 0.9rem;
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  color: var(--ink);
  font-size: 0.85rem;
}
.help-btn:hover {
  border-color: var(--accent);
  color: var(--accent);
}
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
  max-width: 560px;
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
  padding: 1rem 1.25rem 1.25rem;
  overflow-y: auto;
  font-size: 0.9rem;
  line-height: 1.65;
  color: var(--ink);
}
.dialog-body h3 {
  font-size: 0.95rem;
  margin: 1rem 0 0.35rem;
}
.dialog-body ul,
.dialog-body ol {
  margin: 0;
  padding-left: 1.25rem;
}
.dialog-body li {
  margin-bottom: 0.35rem;
}
.formula {
  font-family: ui-monospace, Menlo, Consolas, monospace;
  background: var(--surface-2);
  border-radius: 6px;
  padding: 0.5rem 0.75rem;
  margin: 0.5rem 0;
}
.note {
  margin-top: 1rem;
  padding: 0.6rem 0.75rem;
  background: var(--warn-soft);
  border-radius: 6px;
  color: var(--warn);
  font-size: 0.85rem;
}
</style>
