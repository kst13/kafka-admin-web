<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import { MODE_LABELS, type KafkaAppDetail, type PermissionMode, type TopicPermission } from '@/lib/kafkaApps'
import KafkaAppPermissionModal from '@/components/KafkaAppPermissionModal.vue'
import KafkaAppDeleteModal from '@/components/KafkaAppDeleteModal.vue'
import KafkaAppResetPasswordModal from '@/components/KafkaAppResetPasswordModal.vue'
import KafkaAppCreateModal from '@/components/KafkaAppCreateModal.vue'

const route = useRoute()
const router = useRouter()
const { isAdmin } = useSession()
const name = String(route.params.name)

const detail = ref<KafkaAppDetail | null>(null)
const error = ref('')
const actionError = ref('')
const showAdd = ref(false)
const editTarget = ref<TopicPermission | null>(null)
const showDelete = ref(false)
const showReset = ref(false)
const showRegister = ref(false)

const canManage = computed(() => isAdmin.value && detail.value?.registered === true)
const hasConsume = computed(() => detail.value?.permissions.some((p) => p.mode !== 'produce') ?? false)

async function load() {
  try {
    detail.value = await api<KafkaAppDetail>(`/kafka-apps/${name}`)
    error.value = ''
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(load)

function onSaved(d: KafkaAppDetail) {
  detail.value = d
  showAdd.value = false
  editTarget.value = null
}

async function revoke(topic: string) {
  actionError.value = ''
  try {
    detail.value = await api<KafkaAppDetail>(`/ops/kafka-apps/${name}/topics/${topic}`, { method: 'DELETE' })
  } catch (e) {
    actionError.value = e instanceof Error ? e.message : '회수 실패'
  }
}

function onDeleted() {
  showDelete.value = false
  router.push('/kafka-apps')
}

function modeLabel(mode: PermissionMode) { return MODE_LABELS[mode] }
</script>

<template>
  <main>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-else-if="detail">
      <div class="head-row">
        <h1>
          {{ detail.name }}
          <span v-if="!detail.registered" class="badge">미등록</span>
        </h1>
        <div v-if="canManage" class="actions">
          <button type="button" class="btn reset-password" @click="showReset = true">비밀번호 재발급</button>
          <button type="button" class="btn danger-outline delete-app" @click="showDelete = true">삭제</button>
        </div>
        <button v-else-if="isAdmin && !detail.registered" type="button" class="btn primary register-app"
                @click="showRegister = true">등록</button>
      </div>

      <dl class="meta">
        <dt>담당 개발자</dt><dd>{{ detail.owner ?? '—' }}</dd>
        <dt>설명</dt><dd>{{ detail.description ?? '—' }}</dd>
        <dt>생성일</dt><dd>{{ detail.createdAt ? new Date(detail.createdAt).toLocaleString() : '—' }}</dd>
      </dl>
      <p v-if="!detail.registered" class="hint">
        브로커에는 있지만 이 사이트에 등록되지 않은 계정입니다. 등록하면 담당자·설명을 기록하고 권한을 관리할 수 있습니다.
      </p>

      <div class="head-row">
        <h2>토픽 권한</h2>
        <button v-if="canManage" type="button" class="btn primary add-permission" @click="showAdd = true">권한 추가</button>
      </div>
      <p v-if="actionError" class="error">{{ actionError }}</p>
      <p v-if="detail.permissions.length === 0" class="hint">부여된 토픽 권한이 없습니다.</p>
      <table v-else>
        <thead><tr><th>토픽</th><th>모드</th><th v-if="canManage"></th></tr></thead>
        <tbody>
          <tr v-for="p in detail.permissions" :key="p.topic">
            <td><RouterLink :to="`/topics/${p.topic}`">{{ p.topic }}</RouterLink></td>
            <td>{{ modeLabel(p.mode) }}</td>
            <td v-if="canManage" class="row-actions">
              <button type="button" class="btn edit" :data-topic="p.topic" @click="editTarget = p">변경</button>
              <button type="button" class="btn danger-outline revoke" :data-topic="p.topic" @click="revoke(p.topic)">회수</button>
            </td>
          </tr>
        </tbody>
      </table>
      <p v-if="hasConsume" class="hint">
        consume 권한이 있어 컨슈머 그룹 <code>'{{ detail.name }}*'</code> (앱 이름 접두어) 를 사용할 수 있습니다.
      </p>

      <template v-if="detail.otherAcls.length > 0">
        <h2>기타 ACL</h2>
        <p class="hint">이 화면의 규칙 밖에서 부여된 ACL 입니다. 읽기 전용이며 브로커에서 직접 관리합니다.</p>
        <table>
          <thead><tr><th>리소스</th><th>패턴</th><th>이름</th><th>오퍼레이션</th></tr></thead>
          <tbody>
            <tr v-for="(a, i) in detail.otherAcls" :key="i">
              <td>{{ a.resourceType }}</td><td>{{ a.patternType }}</td><td>{{ a.name }}</td><td>{{ a.operation }}</td>
            </tr>
          </tbody>
        </table>
      </template>

      <KafkaAppPermissionModal v-if="showAdd" :app="detail.name" @close="showAdd = false" @saved="onSaved" />
      <KafkaAppPermissionModal v-if="editTarget" :app="detail.name" :topic="editTarget.topic" :mode="editTarget.mode"
                               @close="editTarget = null" @saved="onSaved" />
      <KafkaAppDeleteModal v-if="showDelete" :name="detail.name" @close="showDelete = false" @deleted="onDeleted" />
      <KafkaAppResetPasswordModal v-if="showReset" :name="detail.name" @close="showReset = false" />
      <KafkaAppCreateModal v-if="showRegister" :register-name="detail.name"
                           @close="showRegister = false" @registered="showRegister = false; load()" />
    </template>
  </main>
</template>

<style scoped>
.head-row { display: flex; justify-content: space-between; align-items: center; }
.actions, .row-actions { display: flex; gap: 0.5rem; }
.meta { display: grid; grid-template-columns: max-content 1fr; gap: 0.3rem 1rem; margin: 0.5rem 0 1rem; }
.meta dt { color: var(--ink-soft); }
.meta dd { margin: 0; }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.badge {
  margin-left: 0.5rem; padding: 0.1rem 0.4rem; border-radius: 4px; font-size: 0.75rem; vertical-align: middle;
  background: var(--surface-2); color: var(--ink-soft);
}
.error { color: var(--crit); }
</style>
