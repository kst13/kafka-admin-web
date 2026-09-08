<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import type { KafkaAppSummary } from '@/lib/kafkaApps'
import KafkaAppCreateModal from '@/components/KafkaAppCreateModal.vue'

const apps = ref<KafkaAppSummary[]>([])
const error = ref('')
const showCreate = ref(false)
const registerTarget = ref<string | null>(null)
const { isAdmin } = useSession()

async function load() {
  try {
    apps.value = await api<KafkaAppSummary[]>('/kafka-apps')
    error.value = ''
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(load)

// 생성 모달은 비밀번호를 보여주는 동안 열려 있어야 하므로 created 시점엔 목록만 갱신하고 닫지 않는다
function onCreated() { load() }
function onRegistered() { registerTarget.value = null; load() }
</script>

<template>
  <main>
    <div class="head-row">
      <h1>Kafka 계정</h1>
      <button v-if="isAdmin" type="button" class="btn primary create-app" @click="showCreate = true">앱 계정 추가</button>
    </div>
    <p class="hint">
      애플리케이션이 브로커에 접속할 때 쓰는 SCRAM 계정입니다. 앱(서비스) 단위로 만들고 토픽별 produce/consume 권한을 부여합니다.
      <span v-if="!isAdmin">변경은 ADMIN 만 할 수 있습니다.</span>
    </p>
    <p v-if="error" class="error">{{ error }}</p>
    <table v-else>
      <thead><tr><th>앱 이름</th><th>담당 개발자</th><th>설명</th><th>권한 토픽</th><th v-if="isAdmin"></th></tr></thead>
      <tbody>
        <tr v-for="a in apps" :key="a.name" :class="{ unregistered: !a.registered }">
          <td>
            <RouterLink :to="`/kafka-apps/${a.name}`">{{ a.name }}</RouterLink>
            <span v-if="!a.registered" class="badge">미등록</span>
          </td>
          <td>{{ a.owner ?? '—' }}</td>
          <td>{{ a.description ?? '—' }}</td>
          <td>{{ a.topicCount }}</td>
          <td v-if="isAdmin">
            <button v-if="!a.registered" type="button" class="btn register-app" :data-name="a.name"
                    @click="registerTarget = a.name">등록</button>
          </td>
        </tr>
      </tbody>
    </table>
    <KafkaAppCreateModal v-if="showCreate" @close="showCreate = false" @created="onCreated" />
    <KafkaAppCreateModal v-if="registerTarget" :register-name="registerTarget"
                         @close="registerTarget = null" @registered="onRegistered" />
  </main>
</template>

<style scoped>
.head-row { display: flex; justify-content: space-between; align-items: center; }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.unregistered td { color: var(--ink-soft); }
.badge {
  margin-left: 0.5rem; padding: 0.1rem 0.4rem; border-radius: 4px; font-size: 0.75rem;
  background: var(--surface-2); color: var(--ink-soft);
}
.error { color: var(--crit); }
</style>
