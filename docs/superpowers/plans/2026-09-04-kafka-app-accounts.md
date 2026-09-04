# Kafka 앱 계정 관리 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 관리자 사이트에서 앱(서비스)별 Kafka SCRAM 계정을 만들고, 토픽별 produce/consume 권한(ACL)을 부여·회수한다. DEVELOPER 는 조회만, ADMIN 만 변경.

**Architecture:** Kafka 브로커가 원본(SCRAM 자격증명·ACL). 로컬 H2 `kafka_app` 테이블에는 앱 이름·담당자·설명만 둔다. 조회는 `kafka` 패키지의 `KafkaAppQueryService`, 변경은 `ops` 패키지의 `KafkaAppCommandService` 가 `AuditRecorder` 를 거쳐 수행한다. 권한↔ACL 변환은 순수 클래스 `AclMapping` 한 곳에 모아 조회·변경이 같은 규칙을 쓴다. 프론트는 기존 `ModalDialog`, `useSession`, `api` 클라이언트 관례를 그대로 따른다.

**Tech Stack:** Spring Boot 4.1 / Java 21 / kafka-clients 4.2 (AdminClient SCRAM·ACL API) / JPA+H2 / Testcontainers 2.0.5 (`apache/kafka:4.0.0`) / Vue 3.5 + TS / vitest 4

**Spec:** `docs/superpowers/specs/2026-09-04-kafka-app-accounts-design.md`

## Global Constraints

- 앱 이름 패턴 `[a-zA-Z0-9._-]{1,64}`. SCRAM 사용자명과 동일. principal 은 `User:<name>`.
- 비밀번호: 24자, `[A-Za-z0-9]`, `SecureRandom`. 응답에 한 번만 담고 DB·감사 params·로그에 남기지 않는다.
- SCRAM 메커니즘 `SCRAM-SHA-512`, iterations 4096.
- ACL 매핑: produce → Topic LITERAL WRITE. consume → Topic LITERAL READ + Group PREFIXED `<name>` READ. both → WRITE+READ (+Group). DESCRIBE 는 만들지 않는다(READ/WRITE 가 암묵 허용). host `*`, ALLOW.
- 그룹 ACL 보정: 그 principal 의 Topic READ 가 하나라도 있으면 Group READ 유지, 없으면 제거.
- 변경 API 는 `/api/ops/**` 아래(기존 `SecurityConfig` 로 ADMIN 제한). 조회 API 는 `/api/kafka-apps` (인증만).
- 모든 변경은 `AuditRecorder.record(actor, action, target, paramsJson, runnable)` 경유. action: `KAFKA_APP_CREATE | KAFKA_APP_REGISTER | KAFKA_APP_RESET_PASSWORD | KAFKA_APP_DELETE | KAFKA_APP_GRANT | KAFKA_APP_REVOKE`.
- AdminClient 호출은 `OpsFutures.await` / `KafkaFutures.await` 를 통해서만(타임아웃 규약은 `KafkaConnectionProperties` 의 5초가 전역 적용).
- H2 예약어 컬럼명 금지 (`VALUE`, `RESULT` 등). 테스트 in-memory DB 는 고유 이름.
- Boot 4 테스트 슬라이스 import: `org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest`, `org.springframework.test.context.bean.override.mockito.MockitoBean`.
- Testcontainers 는 싱글턴 컨테이너 패턴(`static { start(); }`), `@Container` 금지.
- 작업 트리에 미커밋 변경(사이트 계정 관리)이 있다. **이 계획은 git worktree(브랜치 `feature/kafka-app-accounts`, base `production`)에서 실행한다.** 그 변경(UsersView 등)은 worktree 에 없으므로 Task 9 의 UsersView 제목 변경은 파일이 있을 때만 수행한다.
- 커밋 메시지 끝에 붙일 것:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01VqsEEP7LjxWJAg8cwH1o35
  ```
- 백엔드 테스트 실행: `cd was && ./gradlew test --tests '<FQCN>'`. 프론트: `cd web && npx vitest run <path>`.

## File Structure

백엔드 (`was/src/main/java/com/osstem/kafkaadmin/`):

| 파일 | 책임 |
|---|---|
| `kafka/PermissionMode.java` | `PRODUCE/CONSUME/BOTH` enum + 문자열 파싱 |
| `kafka/AclMapping.java` | 권한↔ACL 순수 변환(바인딩 생성, 필터, 역매핑) |
| `kafka/dto/Dtos.java` (수정) | `KafkaAppSummary`, `TopicPermission`, `RawAcl`, `KafkaAppDetail` 레코드 추가 |
| `kafka/KafkaAppQueryService.java` | SCRAM 목록 + ACL + 메타데이터 병합 조회 |
| `ops/KafkaApp.java`, `ops/KafkaAppRepository.java` | 메타데이터 엔티티/저장소 |
| `ops/KafkaAppExistsException.java`, `ops/KafkaAppNotFoundException.java` | 409/404 용 예외 |
| `ops/PasswordGenerator.java` | 24자 난수 |
| `ops/OpsFutures.java` (수정) | `ClusterAuthorizationException`, `ResourceNotFoundException` 통과 |
| `ops/KafkaAppCommandService.java` | 생성·등록·재발급·삭제·권한 부여·회수 |
| `api/KafkaAppQueryController.java` | `GET /api/kafka-apps`, `GET /api/kafka-apps/{name}` |
| `api/KafkaAppOpsController.java` | `/api/ops/kafka-apps/**` 변경 API |
| `api/ApiExceptionHandler.java` (수정) | 409/404/403 매핑 추가 |

백엔드 테스트 (`was/src/test/java/com/osstem/kafkaadmin/`): `kafka/AclMappingTest`, `ops/PasswordGeneratorTest`, `ops/KafkaAppRepositoryTest`, `ops/OpsFuturesTest`, `kafka/KafkaSecureIntegrationTestBase`, `kafka/KafkaSecureContainerIT`, `kafka/KafkaAppQueryServiceIT`, `ops/KafkaAppCommandServiceTest`, `ops/KafkaAppCommandServiceIT`, `api/KafkaAppQueryControllerTest`, `api/KafkaAppOpsControllerTest`.

프론트 (`web/src/`):

| 파일 | 책임 |
|---|---|
| `lib/kafkaApps.ts` | 타입, 모드 라벨, 영향 요약 문구(순수 함수) |
| `components/PasswordReveal.vue` | 비밀번호 1회 표시 + 복사 |
| `components/KafkaAppCreateModal.vue` | 생성/등록 폼 → 비밀번호 단계 |
| `components/KafkaAppPermissionModal.vue` | 토픽 + 모드 선택, 영향 요약 |
| `components/KafkaAppDeleteModal.vue` | 이름 타이핑 확인 삭제 |
| `components/KafkaAppResetPasswordModal.vue` | 확인 → 비밀번호 단계 |
| `views/KafkaAppsView.vue` | 목록 |
| `views/KafkaAppDetailView.vue` | 상세 + 권한 표 |
| `router/index.ts`, `App.vue` (수정) | 라우트·메뉴 |

---

### Task 1: PermissionMode + AclMapping (순수 변환)

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/kafka/PermissionMode.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/kafka/AclMapping.java`
- Modify: `was/src/main/java/com/osstem/kafkaadmin/kafka/dto/Dtos.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/kafka/AclMappingTest.java`

**Interfaces:**
- Produces: `PermissionMode { PRODUCE, CONSUME, BOTH; static PermissionMode parse(String); String value() }`
- Produces: `AclMapping.principal(String app)`, `topicBindings(app, topic, mode): List<AclBinding>`, `groupBinding(app): AclBinding`, `principalFilter(app): AclBindingFilter`, `topicFilter(app, topic): AclBindingFilter`, `groupFilter(app): AclBindingFilter`, `derive(app, Collection<AclBinding>): Derived`
- Produces DTO: `Dtos.TopicPermission(String topic, String mode)`, `Dtos.RawAcl(String resourceType, String patternType, String name, String operation)`, `Dtos.KafkaAppSummary(String name, String owner, String description, boolean registered, int topicCount)`, `Dtos.KafkaAppDetail(String name, String owner, String description, Instant createdAt, boolean registered, List<TopicPermission> permissions, List<RawAcl> otherAcls)`

- [ ] **Step 1: DTO 레코드 추가**

`Dtos.java` 의 마지막 레코드 뒤에 추가 (파일 상단 import 에 `java.time.Instant`, `java.util.List` 는 이미 있다):

```java
    // Kafka 앱 계정 (SCRAM + ACL)
    public record TopicPermission(String topic, String mode) {}
    public record RawAcl(String resourceType, String patternType, String name, String operation) {}
    public record KafkaAppSummary(String name, String owner, String description,
                                  boolean registered, int topicCount) {}
    public record KafkaAppDetail(String name, String owner, String description, Instant createdAt,
                                 boolean registered, List<TopicPermission> permissions,
                                 List<RawAcl> otherAcls) {}
```

- [ ] **Step 2: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.RawAcl;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicPermission;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class AclMappingTest {

    private static AclBinding acl(ResourceType type, String name, PatternType pattern, AclOperation op) {
        return new AclBinding(new ResourcePattern(type, name, pattern),
                new AccessControlEntry("User:order-api", "*", op, AclPermissionType.ALLOW));
    }

    @Test
    void produce는_토픽_WRITE_하나() {
        List<AclBinding> b = AclMapping.topicBindings("order-api", "orders", PermissionMode.PRODUCE);
        assertThat(b).containsExactly(acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.WRITE));
    }

    @Test
    void consume는_토픽_READ_하나이고_그룹은_별도_바인딩() {
        assertThat(AclMapping.topicBindings("order-api", "orders", PermissionMode.CONSUME))
                .containsExactly(acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ));
        assertThat(AclMapping.groupBinding("order-api"))
                .isEqualTo(acl(ResourceType.GROUP, "order-api", PatternType.PREFIXED, AclOperation.READ));
    }

    @Test
    void both는_WRITE와_READ() {
        assertThat(AclMapping.topicBindings("order-api", "orders", PermissionMode.BOTH))
                .containsExactlyInAnyOrder(
                        acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.WRITE),
                        acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ));
    }

    @Test
    void 역매핑은_토픽별_모드를_계산하고_그룹_ACL은_기타에_넣지_않는다() {
        AclMapping.Derived d = AclMapping.derive("order-api", List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.WRITE),
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ),
                acl(ResourceType.TOPIC, "events", PatternType.LITERAL, AclOperation.READ),
                acl(ResourceType.GROUP, "order-api", PatternType.PREFIXED, AclOperation.READ)));
        assertThat(d.permissions()).containsExactly(
                new TopicPermission("events", "consume"),
                new TopicPermission("orders", "both"));
        assertThat(d.otherAcls()).isEmpty();
        assertThat(d.hasConsume()).isTrue();
    }

    @Test
    void 규칙_밖_ACL은_기타로_노출된다() {
        AclMapping.Derived d = AclMapping.derive("order-api", List.of(
                acl(ResourceType.TOPIC, "ord", PatternType.PREFIXED, AclOperation.WRITE),
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.DESCRIBE),
                acl(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL, AclOperation.IDEMPOTENT_WRITE)));
        assertThat(d.permissions()).isEmpty();
        assertThat(d.otherAcls()).containsExactlyInAnyOrder(
                new RawAcl("TOPIC", "PREFIXED", "ord", "WRITE"),
                new RawAcl("TOPIC", "LITERAL", "orders", "DESCRIBE"),
                new RawAcl("CLUSTER", "LITERAL", "kafka-cluster", "IDEMPOTENT_WRITE"));
        assertThat(d.hasConsume()).isFalse();
    }

    @Test
    void 필터는_principal과_리소스를_정확히_지정한다() {
        assertThat(AclMapping.principalFilter("order-api").entryFilter().principal()).isEqualTo("User:order-api");
        assertThat(AclMapping.topicFilter("order-api", "orders").patternFilter().name()).isEqualTo("orders");
        assertThat(AclMapping.topicFilter("order-api", "orders").patternFilter().patternType()).isEqualTo(PatternType.LITERAL);
        assertThat(AclMapping.groupFilter("order-api").patternFilter().resourceType()).isEqualTo(ResourceType.GROUP);
        assertThat(AclMapping.groupFilter("order-api").patternFilter().patternType()).isEqualTo(PatternType.PREFIXED);
    }

    @Test
    void 모드_파싱은_대소문자를_무시하고_이상값은_IllegalArgument() {
        assertThat(PermissionMode.parse("Produce")).isEqualTo(PermissionMode.PRODUCE);
        assertThat(PermissionMode.BOTH.value()).isEqualTo("both");
        assertThatThrownBy(() -> PermissionMode.parse("admin")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PermissionMode.parse(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
```

- [ ] **Step 3: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.kafka.AclMappingTest'`
Expected: 컴파일 실패 (`PermissionMode`, `AclMapping` 없음)

- [ ] **Step 4: 구현**

`PermissionMode.java`:

```java
package com.osstem.kafkaadmin.kafka;

// 앱 계정의 토픽 권한 모드. 화면/API 값은 소문자("produce" 등).
public enum PermissionMode {
    PRODUCE, CONSUME, BOTH;

    public String value() { return name().toLowerCase(); }

    public boolean canWrite() { return this != CONSUME; }
    public boolean canRead() { return this != PRODUCE; }

    public static PermissionMode parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("mode 는 produce, consume, both 중 하나여야 합니다");
        }
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("mode 는 produce, consume, both 중 하나여야 합니다");
        }
    }
}
```

`AclMapping.java`:

```java
package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.RawAcl;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicPermission;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// 권한(produce/consume/both) <-> Kafka ACL 변환의 단일 출처. 조회(역매핑)와 변경(바인딩 생성)이 같은 규칙을 쓴다.
// 규칙: Topic LITERAL WRITE = produce, READ = consume, 둘 다 = both. consume 이 하나라도 있으면 Group PREFIXED <app> READ.
// DESCRIBE 는 READ/WRITE 가 암묵 허용하므로 만들지 않는다. host "*", ALLOW 고정.
public final class AclMapping {
    private AclMapping() {}

    public static String principal(String app) { return "User:" + app; }

    public static List<AclBinding> topicBindings(String app, String topic, PermissionMode mode) {
        List<AclBinding> out = new ArrayList<>();
        if (mode.canWrite()) out.add(topicAcl(app, topic, AclOperation.WRITE));
        if (mode.canRead()) out.add(topicAcl(app, topic, AclOperation.READ));
        return out;
    }

    public static AclBinding groupBinding(String app) {
        return new AclBinding(new ResourcePattern(ResourceType.GROUP, app, PatternType.PREFIXED),
                allow(app, AclOperation.READ));
    }

    public static AclBindingFilter principalFilter(String app) {
        return new AclBindingFilter(ResourcePatternFilter.ANY, entryFilter(app));
    }

    public static AclBindingFilter topicFilter(String app, String topic) {
        return new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, topic, PatternType.LITERAL), entryFilter(app));
    }

    public static AclBindingFilter groupFilter(String app) {
        return new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.GROUP, app, PatternType.PREFIXED), entryFilter(app));
    }

    public record Derived(List<TopicPermission> permissions, List<RawAcl> otherAcls) {
        public boolean hasConsume() {
            return permissions.stream().anyMatch(p -> !p.mode().equals(PermissionMode.PRODUCE.value()));
        }
    }

    // 토픽명 오름차순 권한 목록 + 규칙 밖 ACL(기타). 그룹 PREFIXED <app> READ 는 규칙의 일부라 기타에 넣지 않는다.
    public static Derived derive(String app, Collection<AclBinding> acls) {
        Map<String, EnumSet<AclOperation>> ops = new TreeMap<>();
        List<RawAcl> others = new ArrayList<>();
        AclBinding group = groupBinding(app);
        for (AclBinding b : acls) {
            if (!b.entry().principal().equals(principal(app))) continue;
            if (b.equals(group)) continue;
            ResourcePattern p = b.pattern();
            AclOperation op = b.entry().operation();
            boolean topicRule = p.resourceType() == ResourceType.TOPIC
                    && p.patternType() == PatternType.LITERAL
                    && (op == AclOperation.READ || op == AclOperation.WRITE)
                    && b.entry().permissionType() == AclPermissionType.ALLOW
                    && "*".equals(b.entry().host());
            if (topicRule) {
                ops.computeIfAbsent(p.name(), k -> EnumSet.noneOf(AclOperation.class)).add(op);
            } else {
                others.add(new RawAcl(p.resourceType().name(), p.patternType().name(), p.name(), op.name()));
            }
        }
        List<TopicPermission> perms = new ArrayList<>();
        ops.forEach((topic, set) -> {
            PermissionMode mode = set.contains(AclOperation.WRITE)
                    ? (set.contains(AclOperation.READ) ? PermissionMode.BOTH : PermissionMode.PRODUCE)
                    : PermissionMode.CONSUME;
            perms.add(new TopicPermission(topic, mode.value()));
        });
        return new Derived(perms, others);
    }

    private static AclBinding topicAcl(String app, String topic, AclOperation op) {
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL), allow(app, op));
    }

    private static AccessControlEntry allow(String app, AclOperation op) {
        return new AccessControlEntry(principal(app), "*", op, AclPermissionType.ALLOW);
    }

    private static AccessControlEntryFilter entryFilter(String app) {
        return new AccessControlEntryFilter(principal(app), null, AclOperation.ANY, AclPermissionType.ANY);
    }
}
```

- [ ] **Step 5: 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.kafka.AclMappingTest'`
Expected: BUILD SUCCESSFUL, 7 tests passed

- [ ] **Step 6: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/kafka/PermissionMode.java was/src/main/java/com/osstem/kafkaadmin/kafka/AclMapping.java was/src/main/java/com/osstem/kafkaadmin/kafka/dto/Dtos.java was/src/test/java/com/osstem/kafkaadmin/kafka/AclMappingTest.java
git commit -m "feat(kafka-app): 권한 모드와 ACL 매핑 규칙"
```

---

### Task 2: 메타데이터 엔티티, 예외, 비밀번호 생성기

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/ops/KafkaApp.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/ops/KafkaAppRepository.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/ops/KafkaAppExistsException.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/ops/KafkaAppNotFoundException.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/ops/PasswordGenerator.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/ops/KafkaAppRepositoryTest.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/ops/PasswordGeneratorTest.java`

**Interfaces:**
- Produces: `KafkaApp(String name, String ownerUsername, String description, Instant createdAt)` + getters `getName/getOwnerUsername/getDescription/getCreatedAt`, `updateMeta(owner, description)`
- Produces: `KafkaAppRepository extends JpaRepository<KafkaApp, Long>` with `Optional<KafkaApp> findByName(String)`, `boolean existsByName(String)`, `void deleteByName(String)`
- Produces: `KafkaAppExistsException(String name)`, `KafkaAppNotFoundException(String name)` (둘 다 `RuntimeException`)
- Produces: `PasswordGenerator.generate(): String` (24자)

- [ ] **Step 1: 실패하는 테스트 작성**

`KafkaAppRepositoryTest.java`:

```java
package com.osstem.kafkaadmin.ops;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.TestPropertySource;
import java.time.Instant;
import static org.assertj.core.api.Assertions.*;

// H2 인메모리로 유니크 제약과 이름 조회를 검증한다. named DB 공유를 피하려고 고유 이름을 쓴다.
@SpringBootTest
@TestPropertySource(properties = "spring.datasource.url=jdbc:h2:mem:kafka-app-repo-test;DB_CLOSE_DELAY=-1")
class KafkaAppRepositoryTest {

    @Autowired KafkaAppRepository repository;

    @Test
    void 이름으로_조회하고_메타데이터를_갱신한다() {
        repository.save(new KafkaApp("repo-t-order-api", "dev1", "주문", Instant.now()));
        KafkaApp found = repository.findByName("repo-t-order-api").orElseThrow();
        assertThat(found.getOwnerUsername()).isEqualTo("dev1");
        found.updateMeta("dev2", "주문 서비스");
        repository.save(found);
        assertThat(repository.findByName("repo-t-order-api").orElseThrow().getDescription()).isEqualTo("주문 서비스");
        assertThat(repository.existsByName("repo-t-order-api")).isTrue();
        assertThat(repository.existsByName("repo-t-missing")).isFalse();
    }

    @Test
    void 같은_이름은_저장할_수_없다() {
        repository.save(new KafkaApp("repo-t-dup", null, null, Instant.now()));
        assertThatThrownBy(() -> repository.saveAndFlush(new KafkaApp("repo-t-dup", null, null, Instant.now())))
                .isInstanceOf(DataIntegrityViolationException.class);
    }
}
```

`PasswordGeneratorTest.java`:

```java
package com.osstem.kafkaadmin.ops;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PasswordGeneratorTest {

    @Test
    void 스물네_자_영숫자이고_매번_다르다() {
        String a = PasswordGenerator.generate();
        String b = PasswordGenerator.generate();
        assertThat(a).hasSize(24).matches("[A-Za-z0-9]{24}");
        assertThat(b).hasSize(24).matches("[A-Za-z0-9]{24}");
        assertThat(a).isNotEqualTo(b);
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.ops.KafkaAppRepositoryTest' --tests 'com.osstem.kafkaadmin.ops.PasswordGeneratorTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 구현**

`KafkaApp.java`:

```java
package com.osstem.kafkaadmin.ops;

import jakarta.persistence.*;
import java.time.Instant;

// Kafka 앱 계정의 메타데이터. 원본(SCRAM·ACL)은 브로커에 있고 여기엔 이름·담당자·설명만 둔다.
// name 은 SCRAM 사용자명과 같다. 비밀번호와 권한은 저장하지 않는다.
@Entity
@Table(name = "kafka_app", uniqueConstraints = @UniqueConstraint(name = "uk_kafka_app_name", columnNames = "name"))
public class KafkaApp {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(nullable = false, length = 64)
    private String name;
    private String ownerUsername;
    @Column(length = 500)
    private String description;
    private Instant createdAt;

    protected KafkaApp() {}

    public KafkaApp(String name, String ownerUsername, String description, Instant createdAt) {
        this.name = name;
        this.ownerUsername = ownerUsername;
        this.description = description;
        this.createdAt = createdAt;
    }

    public void updateMeta(String ownerUsername, String description) {
        this.ownerUsername = ownerUsername;
        this.description = description;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public String getOwnerUsername() { return ownerUsername; }
    public String getDescription() { return description; }
    public Instant getCreatedAt() { return createdAt; }
}
```

`KafkaAppRepository.java`:

```java
package com.osstem.kafkaadmin.ops;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;
import java.util.Optional;

public interface KafkaAppRepository extends JpaRepository<KafkaApp, Long> {
    Optional<KafkaApp> findByName(String name);
    boolean existsByName(String name);
    @Transactional
    void deleteByName(String name);
}
```

`KafkaAppExistsException.java`:

```java
package com.osstem.kafkaadmin.ops;

// 이미 브로커에 있는 SCRAM 계정이거나 이미 등록된 메타데이터 (-> 409)
public class KafkaAppExistsException extends RuntimeException {
    public KafkaAppExistsException(String name) {
        super("이미 존재하는 Kafka 계정입니다: " + name);
    }
}
```

`KafkaAppNotFoundException.java`:

```java
package com.osstem.kafkaadmin.ops;

// 메타데이터가 없는(미등록) 앱에 대한 변경 요청 (-> 404)
public class KafkaAppNotFoundException extends RuntimeException {
    public KafkaAppNotFoundException(String name) {
        super("등록되지 않은 앱입니다: " + name);
    }
}
```

`PasswordGenerator.java`:

```java
package com.osstem.kafkaadmin.ops;

import java.security.SecureRandom;

// SCRAM 비밀번호 생성. 결과는 응답에 한 번만 담고 어디에도 저장하지 않는다.
public final class PasswordGenerator {
    private static final String ALPHABET =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final int LENGTH = 24;
    private static final SecureRandom RANDOM = new SecureRandom();

    private PasswordGenerator() {}

    public static String generate() {
        StringBuilder sb = new StringBuilder(LENGTH);
        for (int i = 0; i < LENGTH; i++) {
            sb.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
        }
        return sb.toString();
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.ops.KafkaAppRepositoryTest' --tests 'com.osstem.kafkaadmin.ops.PasswordGeneratorTest'`
Expected: 3 tests passed

- [ ] **Step 5: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/ops/KafkaApp.java was/src/main/java/com/osstem/kafkaadmin/ops/KafkaAppRepository.java was/src/main/java/com/osstem/kafkaadmin/ops/KafkaAppExistsException.java was/src/main/java/com/osstem/kafkaadmin/ops/KafkaAppNotFoundException.java was/src/main/java/com/osstem/kafkaadmin/ops/PasswordGenerator.java was/src/test/java/com/osstem/kafkaadmin/ops/KafkaAppRepositoryTest.java was/src/test/java/com/osstem/kafkaadmin/ops/PasswordGeneratorTest.java
git commit -m "feat(kafka-app): 메타데이터 엔티티·예외·비밀번호 생성기"
```

---

### Task 3: OpsFutures 예외 통과 확장

**Files:**
- Modify: `was/src/main/java/com/osstem/kafkaadmin/ops/OpsFutures.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/ops/OpsFuturesTest.java`

**Interfaces:**
- Produces: `OpsFutures.await` 가 `ClusterAuthorizationException`, `ResourceNotFoundException` 을 그대로 던진다 (나머지는 기존처럼 `KafkaUnavailableException`)

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class OpsFuturesTest {

    private static <T> KafkaFutureImpl<T> failed(Throwable t) {
        KafkaFutureImpl<T> f = new KafkaFutureImpl<>();
        f.completeExceptionally(t);
        return f;
    }

    @Test
    void 클러스터_인가_거부는_그대로_던진다() {
        assertThatThrownBy(() -> OpsFutures.await(failed(new ClusterAuthorizationException("denied"))))
                .isInstanceOf(ClusterAuthorizationException.class);
    }

    @Test
    void 리소스_없음은_그대로_던진다() {
        assertThatThrownBy(() -> OpsFutures.await(failed(new ResourceNotFoundException("no user"))))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void 타임아웃은_접속불가로_감싼다() {
        assertThatThrownBy(() -> OpsFutures.await(failed(new TimeoutException("slow"))))
                .isInstanceOf(KafkaUnavailableException.class);
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.ops.OpsFuturesTest'`
Expected: 앞의 두 테스트 FAIL (`KafkaUnavailableException` 이 던져짐)

- [ ] **Step 3: 구현**

`OpsFutures.java` 의 import 에 추가:

```java
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.ResourceNotFoundException;
```

`if (cause instanceof TopicExistsException` 조건 목록에 두 줄 추가:

```java
                    || cause instanceof ClusterAuthorizationException
                    || cause instanceof ResourceNotFoundException
```

- [ ] **Step 4: 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.ops.OpsFuturesTest'`
Expected: 3 tests passed

- [ ] **Step 5: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/ops/OpsFutures.java was/src/test/java/com/osstem/kafkaadmin/ops/OpsFuturesTest.java
git commit -m "feat(kafka-app): OpsFutures 가 인가 거부·리소스 없음 예외를 통과시킨다"
```

---
### Task 4: SCRAM + StandardAuthorizer 를 켠 Testcontainers 베이스

**Files:**
- Create: `was/src/test/java/com/osstem/kafkaadmin/kafka/KafkaSecureIntegrationTestBase.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/kafka/KafkaSecureContainerIT.java`

**Interfaces:**
- Produces: `KafkaSecureIntegrationTestBase` (abstract, `@SpringBootTest`) — `protected static final SecureKafkaContainer KAFKA`, `protected static String saslBootstrap()`, `protected static Map<String, Object> saslClientProps(String user, String password)`
- Spring 의 `Admin` 빈은 PLAINTEXT 리스너로 붙는다. PLAINTEXT principal `User:ANONYMOUS` 를 `super.users` 로 두어 SCRAM/ACL 관리가 가능하다. 앱 계정 검증용 클라이언트는 `SASL://` 리스너(9095, SCRAM-SHA-512)로 붙는다.

배경: `org.testcontainers.kafka.KafkaContainer` 2.0.5 는 `withListener` 로 추가한 리스너를 항상 PLAINTEXT 로 만든다. 대신 `configure()` 가 `KAFKA_LISTENERS` / `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` env 를 읽어 합치므로, 생성자에서 env 로 SASL 리스너를 넣고 `containerIsStarting` 을 오버라이드해 광고 주소(advertised listeners)에 SASL 을 포함시킨다. apache/kafka 이미지의 env 변환 규칙: `KAFKA_` 접두어 제거 후 소문자, `_`→`.`, `__`→`_`, `___`→`-`.

- [ ] **Step 1: 베이스 작성**

```java
package com.osstem.kafkaadmin.kafka;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.util.HashMap;
import java.util.Map;

// SCRAM(SASL 리스너 9095) + StandardAuthorizer 를 켠 브로커. Kafka 앱 계정(SCRAM·ACL) IT 전용.
// 기존 KafkaIntegrationTestBase(인증 없음)와 별개 컨테이너·별개 Spring 컨텍스트다.
// 싱글턴 컨테이너 패턴: JVM 당 1회 기동 (@Container 금지 — 컨텍스트 캐시와 충돌).
@SpringBootTest
public abstract class KafkaSecureIntegrationTestBase {

    static final int SASL_PORT = 9095;

    public static class SecureKafkaContainer extends KafkaContainer {
        SecureKafkaContainer() {
            super(DockerImageName.parse("apache/kafka:4.0.0"));
            addExposedPort(SASL_PORT);
            withEnv("KAFKA_LISTENERS",
                    "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094,SASL://0.0.0.0:" + SASL_PORT);
            withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                    "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,SASL:SASL_PLAINTEXT");
            withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "SCRAM-SHA-512");
            withEnv("KAFKA_LISTENER_NAME_SASL_SASL_ENABLED_MECHANISMS", "SCRAM-SHA-512");
            // listener.name.sasl.scram-sha-512.sasl.jaas.config  ('-' 는 env 에서 '___')
            withEnv("KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___512_SASL_JAAS_CONFIG",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required;");
            withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
            withEnv("KAFKA_SUPER_USERS", "User:ANONYMOUS");
            withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false");
        }

        // 부모 구현과 같은 시작 스크립트에 SASL 광고 주소만 더한다.
        @Override
        protected void containerIsStarting(InspectContainerResponse info) {
            String advertised = String.join(",",
                    "PLAINTEXT://" + getBootstrapServers(),
                    "BROKER://" + info.getConfig().getHostName() + ":9093",
                    "SASL://" + getHost() + ":" + getMappedPort(SASL_PORT));
            String script = "#!/bin/bash\n"
                    + "export KAFKA_ADVERTISED_LISTENERS=" + advertised + "\n"
                    + "/etc/kafka/docker/run \n";
            copyFileToContainer(Transferable.of(script, 0777), "/tmp/testcontainers_start.sh");
        }

        String saslBootstrapServers() {
            return getHost() + ":" + getMappedPort(SASL_PORT);
        }
    }

    protected static final SecureKafkaContainer KAFKA = new SecureKafkaContainer();

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("app.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.kafka.security-protocol", () -> "PLAINTEXT");
        registry.add("spring.datasource.url", () -> "jdbc:h2:mem:kafka-secure-it;DB_CLOSE_DELAY=-1");
    }

    protected static String saslBootstrap() { return KAFKA.saslBootstrapServers(); }

    // 앱 계정으로 붙는 producer/consumer 용 설정. 실패를 빨리 보려고 대기 시간을 짧게 둔다.
    protected static Map<String, Object> saslClientProps(String user, String password) {
        Map<String, Object> p = new HashMap<>();
        p.put("bootstrap.servers", saslBootstrap());
        p.put("security.protocol", "SASL_PLAINTEXT");
        p.put("sasl.mechanism", "SCRAM-SHA-512");
        p.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"" + user + "\" password=\"" + password + "\";");
        p.put("request.timeout.ms", 5000);
        p.put("default.api.timeout.ms", 5000);
        p.put("max.block.ms", 5000);
        return p;
    }
}
```

- [ ] **Step 2: 컨테이너 검증 IT 작성**

```java
package com.osstem.kafkaadmin.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;

// 베이스 컨테이너가 의도대로 떠 있는지: PLAINTEXT 는 super user, SASL 은 SCRAM 인증 + ACL 인가.
class KafkaSecureContainerIT extends KafkaSecureIntegrationTestBase {

    @Autowired Admin admin;

    @Test
    void SCRAM_계정으로_인증하고_ACL_없는_토픽은_거부된다() throws Exception {
        admin.createTopics(List.of(new NewTopic("secure-t-smoke", 1, (short) 1))).all().get();
        admin.alterUserScramCredentials(List.of(new UserScramCredentialUpsertion("secure-t-user",
                new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096), "secure-t-pw"))).all().get();
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(admin.describeUserScramCredentials().all().get()).containsKey("secure-t-user"));

        Map<String, Object> props = saslClientProps("secure-t-user", "secure-t-pw");
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>("secure-t-smoke", "k", "v")).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
        }

        admin.createAcls(List.of(AclMapping.topicBindings("secure-t-user", "secure-t-smoke",
                PermissionMode.PRODUCE).get(0))).all().get();
        await().atMost(ofSeconds(10)).untilAsserted(() -> {
            try (KafkaProducer<String, String> producer =
                         new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
                producer.send(new ProducerRecord<>("secure-t-smoke", "k", "v")).get();
            }
        });
        List<AclBinding> acls = List.copyOf(admin.describeAcls(AclMapping.principalFilter("secure-t-user")).values().get());
        assertThat(acls).hasSize(1);
    }
}
```

- [ ] **Step 3: 실행**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.kafka.KafkaSecureContainerIT'`
Expected: 1 test passed.

컨테이너가 뜨지 않으면(컨텍스트 로드 실패, `Timed out waiting for container`) 테스트 클래스에 임시로 `System.out.println(KAFKA.getLogs())` 를 넣어 브로커 로그를 확인한다. 흔한 원인 두 가지: (1) JAAS env 이름 변환 — 로그에 `Could not find a 'KafkaServer' or 'sasl_KafkaServer' entry` 가 보이면 env 키의 `___` 변환이 안 된 것이므로 `withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/tmp/jaas.conf")` 와 `withCopyToContainer(Transferable.of("KafkaServer { org.apache.kafka.common.security.scram.ScramLoginModule required; };\n"), "/tmp/jaas.conf")` 로 대체한다. (2) 광고 주소 — 로그에 `advertised.listeners` 검증 오류가 보이면 `containerIsStarting` 의 문자열을 다시 확인한다.

- [ ] **Step 4: 커밋**

```bash
git add was/src/test/java/com/osstem/kafkaadmin/kafka/KafkaSecureIntegrationTestBase.java was/src/test/java/com/osstem/kafkaadmin/kafka/KafkaSecureContainerIT.java
git commit -m "test(kafka-app): SCRAM+ACL 을 켠 Testcontainers 베이스"
```

---

### Task 5: KafkaAppQueryService (조회)

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/kafka/KafkaAppQueryService.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/kafka/KafkaAppQueryServiceIT.java`

**Interfaces:**
- Consumes: `KafkaAppRepository`, `AclMapping`, `Dtos.KafkaAppSummary/KafkaAppDetail`
- Produces: `List<KafkaAppSummary> listApps()`, `KafkaAppDetail describeApp(String name)` (SCRAM 에도 메타데이터에도 없으면 `KafkaAppNotFoundException`), `Set<String> scramUsers()`

- [ ] **Step 1: 실패하는 IT 작성**

```java
package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppSummary;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicPermission;
import com.osstem.kafkaadmin.ops.KafkaApp;
import com.osstem.kafkaadmin.ops.KafkaAppNotFoundException;
import com.osstem.kafkaadmin.ops.KafkaAppRepository;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;

class KafkaAppQueryServiceIT extends KafkaSecureIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired KafkaAppQueryService queries;
    @Autowired KafkaAppRepository repository;

    private void scram(String user) throws Exception {
        admin.alterUserScramCredentials(List.of(new UserScramCredentialUpsertion(user,
                new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096), "pw"))).all().get();
    }

    @Test
    void 등록_계정과_미등록_계정을_구분해_나열하고_상세는_권한을_역매핑한다() throws Exception {
        scram("q-t-registered");
        scram("q-t-unregistered");
        repository.save(new KafkaApp("q-t-registered", "dev1", "조회 테스트", Instant.now()));
        List<AclBinding> acls = new ArrayList<>(AclMapping.topicBindings("q-t-registered", "q-t-orders", PermissionMode.BOTH));
        acls.addAll(AclMapping.topicBindings("q-t-registered", "q-t-events", PermissionMode.CONSUME));
        acls.add(AclMapping.groupBinding("q-t-registered"));
        admin.createAcls(acls).all().get();

        await().atMost(ofSeconds(10)).untilAsserted(() -> {
            List<KafkaAppSummary> list = queries.listApps();
            assertThat(list).anySatisfy(s -> {
                assertThat(s.name()).isEqualTo("q-t-registered");
                assertThat(s.registered()).isTrue();
                assertThat(s.owner()).isEqualTo("dev1");
                assertThat(s.topicCount()).isEqualTo(2);
            });
            assertThat(list).anySatisfy(s -> {
                assertThat(s.name()).isEqualTo("q-t-unregistered");
                assertThat(s.registered()).isFalse();
                assertThat(s.topicCount()).isZero();
            });
        });

        KafkaAppDetail d = queries.describeApp("q-t-registered");
        assertThat(d.permissions()).containsExactly(
                new TopicPermission("q-t-events", "consume"),
                new TopicPermission("q-t-orders", "both"));
        assertThat(d.otherAcls()).isEmpty();
        assertThat(d.registered()).isTrue();
        assertThat(d.createdAt()).isNotNull();

        KafkaAppDetail u = queries.describeApp("q-t-unregistered");
        assertThat(u.registered()).isFalse();
        assertThat(u.owner()).isNull();
        assertThat(u.permissions()).isEmpty();
    }

    @Test
    void 어디에도_없는_이름은_NotFound() {
        assertThatThrownBy(() -> queries.describeApp("q-t-ghost"))
                .isInstanceOf(KafkaAppNotFoundException.class);
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.kafka.KafkaAppQueryServiceIT'`
Expected: 컴파일 실패 (`KafkaAppQueryService` 없음)

- [ ] **Step 3: 구현**

```java
package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppSummary;
import com.osstem.kafkaadmin.ops.KafkaApp;
import com.osstem.kafkaadmin.ops.KafkaAppNotFoundException;
import com.osstem.kafkaadmin.ops.KafkaAppRepository;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.springframework.stereotype.Service;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

// Kafka 앱 계정 조회. 원본은 브로커(SCRAM 사용자 목록 + ACL), 메타데이터(담당자·설명)는 H2.
// 브로커에는 있는데 메타데이터가 없는 계정(kafka-admin, admin 등)은 registered=false 로 나열만 한다.
@Service
public class KafkaAppQueryService {

    private final Admin admin;
    private final KafkaAppRepository repository;

    public KafkaAppQueryService(Admin admin, KafkaAppRepository repository) {
        this.admin = admin;
        this.repository = repository;
    }

    public Set<String> scramUsers() {
        return KafkaFutures.await(admin.describeUserScramCredentials().all()).keySet();
    }

    public List<KafkaAppSummary> listApps() {
        Set<String> names = new TreeSet<>(scramUsers());
        Map<String, KafkaApp> meta = repository.findAll().stream()
                .collect(Collectors.toMap(KafkaApp::getName, a -> a));
        names.addAll(meta.keySet());
        Collection<AclBinding> allAcls = KafkaFutures.await(admin.describeAcls(AclBindingFilter.ANY).values());
        return names.stream().map(name -> {
            KafkaApp app = meta.get(name);
            int topicCount = AclMapping.derive(name, allAcls).permissions().size();
            return new KafkaAppSummary(name,
                    app == null ? null : app.getOwnerUsername(),
                    app == null ? null : app.getDescription(),
                    app != null, topicCount);
        }).toList();
    }

    public KafkaAppDetail describeApp(String name) {
        Optional<KafkaApp> app = repository.findByName(name);
        if (app.isEmpty() && !scramUsers().contains(name)) {
            throw new KafkaAppNotFoundException(name);
        }
        Collection<AclBinding> acls = KafkaFutures.await(
                admin.describeAcls(AclMapping.principalFilter(name)).values());
        AclMapping.Derived derived = AclMapping.derive(name, acls);
        return new KafkaAppDetail(name,
                app.map(KafkaApp::getOwnerUsername).orElse(null),
                app.map(KafkaApp::getDescription).orElse(null),
                app.map(KafkaApp::getCreatedAt).orElse(null),
                app.isPresent(), derived.permissions(), derived.otherAcls());
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.kafka.KafkaAppQueryServiceIT'`
Expected: 2 tests passed

- [ ] **Step 5: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/kafka/KafkaAppQueryService.java was/src/test/java/com/osstem/kafkaadmin/kafka/KafkaAppQueryServiceIT.java
git commit -m "feat(kafka-app): SCRAM·ACL·메타데이터 병합 조회 서비스"
```

---
### Task 6: KafkaAppCommandService (변경)

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/ops/KafkaAppCommandService.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/ops/KafkaAppCommandServiceTest.java` (mock Admin, 흐름·보정 규칙)
- Test: `was/src/test/java/com/osstem/kafkaadmin/ops/KafkaAppCommandServiceIT.java` (실브로커 왕복 + 실제 producer/consumer 허용·거부)

**Interfaces:**
- Consumes: `KafkaAppRepository`, `AclMapping`, `PermissionMode`, `PasswordGenerator`, `OpsFutures`, `KafkaAppExistsException`, `KafkaAppNotFoundException`
- Produces:
  - `String create(String name, String owner, String description)` → 비밀번호
  - `void register(String name, String owner, String description)`
  - `String resetPassword(String name)` → 비밀번호
  - `void delete(String name)`
  - `void setTopicPermission(String name, String topic, PermissionMode mode)`
  - `void revokeTopicPermission(String name, String topic)`
- 감사 로그는 컨트롤러(Task 7)가 `AuditRecorder` 로 감싼다. 서비스 자체는 기록하지 않는다(기존 `TopicCommandService` 관례).

- [ ] **Step 1: 실패하는 단위 테스트 작성**

```java
package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.AclMapping;
import com.osstem.kafkaadmin.kafka.PermissionMode;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// 브로커 호출 순서와 그룹 ACL 보정 규칙을 mock Admin 으로 검증한다. 실브로커 왕복은 IT 에서.
class KafkaAppCommandServiceTest {

    private final Admin admin = mock(Admin.class);
    private final KafkaAppRepository repository = mock(KafkaAppRepository.class);
    private final KafkaAppCommandService service = new KafkaAppCommandService(admin, repository);

    private final KafkaApp app = new KafkaApp("order-api", "dev1", null, Instant.now());

    @BeforeEach
    void stubs() {
        AlterUserScramCredentialsResult alter = mock(AlterUserScramCredentialsResult.class);
        when(alter.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.alterUserScramCredentials(anyList())).thenReturn(alter);

        CreateAclsResult created = mock(CreateAclsResult.class);
        when(created.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createAcls(anyCollection())).thenReturn(created);

        DeleteAclsResult deleted = mock(DeleteAclsResult.class);
        when(deleted.all()).thenReturn(KafkaFuture.completedFuture(List.of()));
        when(admin.deleteAcls(anyCollection())).thenReturn(deleted);

        DescribeTopicsResult topics = mock(DescribeTopicsResult.class);
        when(topics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(
                Map.of("orders", mock(TopicDescription.class))));
        when(admin.describeTopics(anyCollection())).thenReturn(topics);

        scramUsers(Map.of());
        acls(List.of());
    }

    private void scramUsers(Map<String, UserScramCredentialsDescription> users) {
        DescribeUserScramCredentialsResult r = mock(DescribeUserScramCredentialsResult.class);
        when(r.all()).thenReturn(KafkaFuture.completedFuture(users));
        when(admin.describeUserScramCredentials()).thenReturn(r);
    }

    private void acls(Collection<AclBinding> bindings) {
        DescribeAclsResult r = mock(DescribeAclsResult.class);
        when(r.values()).thenReturn(KafkaFuture.completedFuture(bindings));
        when(admin.describeAcls(any(AclBindingFilter.class))).thenReturn(r);
    }

    @Test
    void 생성은_SCRAM_upsert_후_메타데이터를_저장하고_비밀번호를_돌려준다() {
        when(repository.existsByName("order-api")).thenReturn(false);
        String pw = service.create("order-api", "dev1", "주문");
        assertThat(pw).matches("[A-Za-z0-9]{24}");
        ArgumentCaptor<List<org.apache.kafka.clients.admin.UserScramCredentialAlteration>> cap =
                ArgumentCaptor.forClass(List.class);
        verify(admin).alterUserScramCredentials(cap.capture());
        UserScramCredentialUpsertion up = (UserScramCredentialUpsertion) cap.getValue().get(0);
        assertThat(up.user()).isEqualTo("order-api");
        assertThat(new String(up.password())).isEqualTo(pw);
        verify(repository).save(argThat(a -> a.getName().equals("order-api") && "dev1".equals(a.getOwnerUsername())));
    }

    @Test
    void 브로커에_이미_있는_SCRAM_계정은_409() {
        scramUsers(Map.of("order-api", mock(UserScramCredentialsDescription.class)));
        assertThatThrownBy(() -> service.create("order-api", null, null))
                .isInstanceOf(KafkaAppExistsException.class);
        verify(admin, never()).alterUserScramCredentials(anyList());
    }

    @Test
    void 이름_패턴_위반은_400() {
        assertThatThrownBy(() -> service.create("bad name!", null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.create("a".repeat(65), null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 등록은_브로커에_있고_메타데이터가_없을_때만() {
        scramUsers(Map.of("order-api", mock(UserScramCredentialsDescription.class)));
        when(repository.existsByName("order-api")).thenReturn(false);
        service.register("order-api", "dev1", "기존 계정");
        verify(repository).save(any(KafkaApp.class));
        verify(admin, never()).alterUserScramCredentials(anyList());

        when(repository.existsByName("order-api")).thenReturn(true);
        assertThatThrownBy(() -> service.register("order-api", null, null))
                .isInstanceOf(KafkaAppExistsException.class);

        scramUsers(Map.of());
        when(repository.existsByName("ghost")).thenReturn(false);
        assertThatThrownBy(() -> service.register("ghost", null, null))
                .isInstanceOf(KafkaAppNotFoundException.class);
    }

    @Test
    void 미등록_앱의_재발급_삭제_권한변경은_404() {
        when(repository.findByName("ghost")).thenReturn(Optional.empty());
        assertThatThrownBy(() -> service.resetPassword("ghost")).isInstanceOf(KafkaAppNotFoundException.class);
        assertThatThrownBy(() -> service.delete("ghost")).isInstanceOf(KafkaAppNotFoundException.class);
        assertThatThrownBy(() -> service.setTopicPermission("ghost", "orders", PermissionMode.PRODUCE))
                .isInstanceOf(KafkaAppNotFoundException.class);
        assertThatThrownBy(() -> service.revokeTopicPermission("ghost", "orders"))
                .isInstanceOf(KafkaAppNotFoundException.class);
        verify(admin, never()).createAcls(anyCollection());
        verify(admin, never()).deleteAcls(anyCollection());
    }

    @Test
    void 삭제는_ACL_전부_제거_후_SCRAM_삭제_후_메타데이터_삭제() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        service.delete("order-api");
        var order = inOrder(admin, repository);
        order.verify(admin).deleteAcls(List.of(AclMapping.principalFilter("order-api")));
        order.verify(admin).alterUserScramCredentials(argThat(l ->
                l.get(0) instanceof UserScramCredentialDeletion d && d.user().equals("order-api")));
        order.verify(repository).deleteByName("order-api");
    }

    @Test
    void consume_부여는_토픽_ACL을_갈아끼우고_그룹_ACL을_만든다() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        // 부여 후 조회에서 READ 가 보이도록: describeAcls 는 부여 결과를 돌려준다
        acls(AclMapping.topicBindings("order-api", "orders", PermissionMode.CONSUME));
        service.setTopicPermission("order-api", "orders", PermissionMode.CONSUME);
        var order = inOrder(admin);
        order.verify(admin).deleteAcls(List.of(AclMapping.topicFilter("order-api", "orders")));
        order.verify(admin).createAcls(AclMapping.topicBindings("order-api", "orders", PermissionMode.CONSUME));
        order.verify(admin).createAcls(List.of(AclMapping.groupBinding("order-api")));
        verify(admin, never()).deleteAcls(List.of(AclMapping.groupFilter("order-api")));
    }

    @Test
    void 마지막_consume을_produce로_바꾸면_그룹_ACL을_지운다() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        acls(AclMapping.topicBindings("order-api", "orders", PermissionMode.PRODUCE));
        service.setTopicPermission("order-api", "orders", PermissionMode.PRODUCE);
        verify(admin).deleteAcls(List.of(AclMapping.groupFilter("order-api")));
        verify(admin, never()).createAcls(List.of(AclMapping.groupBinding("order-api")));
    }

    @Test
    void 회수_후_다른_consume이_남아있으면_그룹_ACL을_유지한다() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        acls(AclMapping.topicBindings("order-api", "events", PermissionMode.CONSUME));
        service.revokeTopicPermission("order-api", "orders");
        verify(admin).deleteAcls(List.of(AclMapping.topicFilter("order-api", "orders")));
        verify(admin).createAcls(List.of(AclMapping.groupBinding("order-api")));
        verify(admin, never()).deleteAcls(List.of(AclMapping.groupFilter("order-api")));
    }

    @Test
    void 없는_토픽에_권한을_주면_UnknownTopic() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        DescribeTopicsResult topics = mock(DescribeTopicsResult.class);
        org.apache.kafka.common.internals.KafkaFutureImpl<Map<String, TopicDescription>> f =
                new org.apache.kafka.common.internals.KafkaFutureImpl<>();
        f.completeExceptionally(new org.apache.kafka.common.errors.UnknownTopicOrPartitionException("no"));
        when(topics.allTopicNames()).thenReturn(f);
        when(admin.describeTopics(anyCollection())).thenReturn(topics);
        assertThatThrownBy(() -> service.setTopicPermission("order-api", "ghost", PermissionMode.PRODUCE))
                .isInstanceOf(org.apache.kafka.common.errors.UnknownTopicOrPartitionException.class);
        verify(admin, never()).createAcls(anyCollection());
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.ops.KafkaAppCommandServiceTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 구현**

```java
package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.AclMapping;
import com.osstem.kafkaadmin.kafka.PermissionMode;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

// Kafka 앱 계정 변경. 브로커(SCRAM·ACL)가 원본이고 메타데이터는 H2. 감사 로그는 컨트롤러가 AuditRecorder 로 감싼다.
// 비밀번호는 여기서 생성해 반환값으로만 나간다 — 입력 DTO 에도, 로그에도 두지 않는다.
@Service
public class KafkaAppCommandService {

    private static final Pattern APP_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,64}");
    private static final ScramCredentialInfo SCRAM = new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096);
    private static final Logger log = LoggerFactory.getLogger(KafkaAppCommandService.class);

    private final Admin admin;
    private final KafkaAppRepository repository;

    public KafkaAppCommandService(Admin admin, KafkaAppRepository repository) {
        this.admin = admin;
        this.repository = repository;
    }

    public String create(String name, String owner, String description) {
        validateName(name);
        if (repository.existsByName(name) || scramExists(name)) {
            throw new KafkaAppExistsException(name);
        }
        String password = PasswordGenerator.generate();
        upsertScram(name, password);
        repository.save(new KafkaApp(name, blankToNull(owner), blankToNull(description), Instant.now()));
        return password;
    }

    // 브로커에만 있는 계정에 메타데이터를 붙인다 (생성 도중 실패 복구, 수동 생성 계정 편입). 비밀번호는 바꾸지 않는다.
    public void register(String name, String owner, String description) {
        validateName(name);
        if (repository.existsByName(name)) throw new KafkaAppExistsException(name);
        if (!scramExists(name)) throw new KafkaAppNotFoundException(name);
        repository.save(new KafkaApp(name, blankToNull(owner), blankToNull(description), Instant.now()));
    }

    public String resetPassword(String name) {
        requireRegistered(name);
        String password = PasswordGenerator.generate();
        upsertScram(name, password);
        return password;
    }

    // ACL 전부 제거 -> SCRAM 삭제 -> 메타데이터 삭제. SCRAM 이 이미 없어도(수동 삭제) 나머지는 진행한다.
    public void delete(String name) {
        requireRegistered(name);
        OpsFutures.await(admin.deleteAcls(List.of(AclMapping.principalFilter(name))).all());
        try {
            OpsFutures.await(admin.alterUserScramCredentials(
                    List.of(new UserScramCredentialDeletion(name, ScramMechanism.SCRAM_SHA_512))).all());
        } catch (ResourceNotFoundException e) {
            log.warn("SCRAM 계정 {} 이 브로커에 없어 메타데이터만 삭제한다", name);
        }
        repository.deleteByName(name);
    }

    public void setTopicPermission(String name, String topic, PermissionMode mode) {
        requireRegistered(name);
        if (mode == null) throw new IllegalArgumentException("mode 는 produce, consume, both 중 하나여야 합니다");
        OpsFutures.await(admin.describeTopics(List.of(topic)).allTopicNames()); // 없는 토픽 -> UnknownTopicOrPartition
        OpsFutures.await(admin.deleteAcls(List.of(AclMapping.topicFilter(name, topic))).all());
        OpsFutures.await(admin.createAcls(AclMapping.topicBindings(name, topic, mode)).all());
        reconcileGroupAcl(name);
    }

    public void revokeTopicPermission(String name, String topic) {
        requireRegistered(name);
        OpsFutures.await(admin.deleteAcls(List.of(AclMapping.topicFilter(name, topic))).all());
        reconcileGroupAcl(name);
    }

    // consume 이 하나라도 남아 있으면 그룹 READ 를 보장(중복 생성은 브로커가 무시), 없으면 제거
    private void reconcileGroupAcl(String name) {
        Collection<AclBinding> acls = OpsFutures.await(admin.describeAcls(AclMapping.principalFilter(name)).values());
        if (AclMapping.derive(name, acls).hasConsume()) {
            OpsFutures.await(admin.createAcls(List.of(AclMapping.groupBinding(name))).all());
        } else {
            OpsFutures.await(admin.deleteAcls(List.of(AclMapping.groupFilter(name))).all());
        }
    }

    private void upsertScram(String name, String password) {
        OpsFutures.await(admin.alterUserScramCredentials(
                List.of(new UserScramCredentialUpsertion(name, SCRAM, password))).all());
    }

    private boolean scramExists(String name) {
        return OpsFutures.await(admin.describeUserScramCredentials().all()).containsKey(name);
    }

    private void requireRegistered(String name) {
        repository.findByName(name).orElseThrow(() -> new KafkaAppNotFoundException(name));
    }

    private static void validateName(String name) {
        if (name == null || !APP_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("앱 이름은 영문·숫자·'.', '_', '-' 만 사용해 64자 이하로 지정합니다");
        }
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s.trim();
    }
}
```

- [ ] **Step 4: 단위 테스트 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.ops.KafkaAppCommandServiceTest'`
Expected: 10 tests passed

- [ ] **Step 5: 실브로커 IT 작성**

```java
package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.KafkaAppQueryService;
import com.osstem.kafkaadmin.kafka.KafkaSecureIntegrationTestBase;
import com.osstem.kafkaadmin.kafka.PermissionMode;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicPermission;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;

class KafkaAppCommandServiceIT extends KafkaSecureIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired KafkaAppCommandService commands;
    @Autowired KafkaAppQueryService queries;
    @Autowired KafkaAppRepository repository;

    private static void produce(Map<String, Object> props, String topic) throws Exception {
        try (KafkaProducer<String, String> p = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            p.send(new ProducerRecord<>(topic, "k", "v")).get();
        }
    }

    private static ConsumerRecords<String, String> consume(Map<String, Object> props, String topic, String group) {
        Map<String, Object> c = new java.util.HashMap<>(props);
        c.put("group.id", group);
        c.put("auto.offset.reset", "earliest");
        try (KafkaConsumer<String, String> k = new KafkaConsumer<>(c, new StringDeserializer(), new StringDeserializer())) {
            k.subscribe(List.of(topic));
            return k.poll(Duration.ofSeconds(5));
        }
    }

    @Test
    void 생성_권한부여_변경_회수_재발급_삭제_왕복() throws Exception {
        admin.createTopics(List.of(new NewTopic("cmd-t-orders", 1, (short) 1))).all().get();

        String pw = commands.create("cmd-t-app", "dev1", "왕복");
        assertThat(pw).hasSize(24);
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.scramUsers()).contains("cmd-t-app"));
        Map<String, Object> client = saslClientProps("cmd-t-app", pw);

        // 권한 없음 -> produce 거부
        assertThatThrownBy(() -> produce(client, "cmd-t-orders"))
                .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(TopicAuthorizationException.class);

        // produce 부여 -> 성공, consume 은 거부
        commands.setTopicPermission("cmd-t-app", "cmd-t-orders", PermissionMode.PRODUCE);
        await().atMost(ofSeconds(10)).untilAsserted(() -> produce(client, "cmd-t-orders"));
        assertThatThrownBy(() -> consume(client, "cmd-t-orders", "cmd-t-app-reader"))
                .isInstanceOfAny(TopicAuthorizationException.class, GroupAuthorizationException.class);

        // both 로 변경 -> 앱 이름 접두어 그룹으로 consume 성공
        commands.setTopicPermission("cmd-t-app", "cmd-t-orders", PermissionMode.BOTH);
        await().atMost(ofSeconds(15)).untilAsserted(() ->
                assertThat(consume(client, "cmd-t-orders", "cmd-t-app-reader").count()).isGreaterThan(0));
        KafkaAppDetail d = queries.describeApp("cmd-t-app");
        assertThat(d.permissions()).containsExactly(new TopicPermission("cmd-t-orders", "both"));

        // 접두어가 다른 그룹은 거부
        assertThatThrownBy(() -> consume(client, "cmd-t-orders", "other-reader"))
                .isInstanceOf(GroupAuthorizationException.class);

        // 회수 -> 권한·그룹 ACL 모두 사라짐
        commands.revokeTopicPermission("cmd-t-app", "cmd-t-orders");
        await().atMost(ofSeconds(10)).untilAsserted(() -> {
            KafkaAppDetail after = queries.describeApp("cmd-t-app");
            assertThat(after.permissions()).isEmpty();
            assertThat(after.otherAcls()).isEmpty();
        });
        assertThat(admin.describeAcls(com.osstem.kafkaadmin.kafka.AclMapping.principalFilter("cmd-t-app")).values().get()).isEmpty();

        // 재발급 -> 옛 비밀번호 실패, 새 비밀번호 성공(권한 다시 부여)
        String pw2 = commands.resetPassword("cmd-t-app");
        assertThat(pw2).isNotEqualTo(pw);
        commands.setTopicPermission("cmd-t-app", "cmd-t-orders", PermissionMode.PRODUCE);
        await().atMost(ofSeconds(10)).untilAsserted(() -> produce(saslClientProps("cmd-t-app", pw2), "cmd-t-orders"));
        // 인증 실패는 kafka-clients 버전에 따라 send() 에서 바로 던지거나 future 에 실린다
        assertThatThrownBy(() -> produce(saslClientProps("cmd-t-app", pw), "cmd-t-orders"))
                .isInstanceOfAny(ExecutionException.class,
                        org.apache.kafka.common.errors.AuthenticationException.class,
                        org.apache.kafka.common.errors.TimeoutException.class);

        // 삭제 -> SCRAM·ACL·메타데이터 모두 제거
        commands.delete("cmd-t-app");
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.scramUsers()).doesNotContain("cmd-t-app"));
        assertThat(repository.existsByName("cmd-t-app")).isFalse();
        assertThat(admin.describeAcls(com.osstem.kafkaadmin.kafka.AclMapping.principalFilter("cmd-t-app")).values().get()).isEmpty();
    }

    @Test
    void 중복_생성은_409이고_미등록_계정은_등록으로_편입한다() throws Exception {
        commands.create("cmd-t-dup", null, null);
        assertThatThrownBy(() -> commands.create("cmd-t-dup", null, null))
                .isInstanceOf(KafkaAppExistsException.class);

        // 브로커에만 있는 계정 (메타데이터 삭제로 흉내)
        repository.deleteByName("cmd-t-dup");
        assertThatThrownBy(() -> commands.resetPassword("cmd-t-dup"))
                .isInstanceOf(KafkaAppNotFoundException.class);
        commands.register("cmd-t-dup", "dev2", "편입");
        assertThat(queries.describeApp("cmd-t-dup").registered()).isTrue();
        assertThat(queries.describeApp("cmd-t-dup").owner()).isEqualTo("dev2");
    }
}
```

- [ ] **Step 6: IT 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.ops.KafkaAppCommandServiceIT'`
Expected: 2 tests passed. 컨슈머 거부 예외가 `poll` 이 아니라 `subscribe` 이후 첫 `poll` 에서 나는데, 어느 예외인지는 브로커 상태에 따라 Topic/Group 둘 중 하나라 `isInstanceOfAny` 로 받는다.

- [ ] **Step 7: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/ops/KafkaAppCommandService.java was/src/test/java/com/osstem/kafkaadmin/ops/KafkaAppCommandServiceTest.java was/src/test/java/com/osstem/kafkaadmin/ops/KafkaAppCommandServiceIT.java
git commit -m "feat(kafka-app): 계정 생성·등록·재발급·삭제·토픽 권한 부여/회수"
```

---
### Task 7: 컨트롤러, 예외 매핑, 운영 문서

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/api/KafkaAppQueryController.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/api/KafkaAppOpsController.java`
- Modify: `was/src/main/java/com/osstem/kafkaadmin/api/ApiExceptionHandler.java`
- Modify: `docs/deploy-troubleshooting.md` (6번 항목 아래에 선결 ACL 추가)
- Modify: `README.md` (문서 목록에 설계 링크, 메뉴 설명)
- Test: `was/src/test/java/com/osstem/kafkaadmin/api/KafkaAppQueryControllerTest.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/api/KafkaAppOpsControllerTest.java`

**Interfaces:**
- Consumes: `KafkaAppQueryService.listApps/describeApp`, `KafkaAppCommandService.*`, `AuditRecorder.record`
- Produces HTTP:
  - `GET /api/kafka-apps` → `KafkaAppSummary[]`; `GET /api/kafka-apps/{name}` → `KafkaAppDetail`
  - `POST /api/ops/kafka-apps` `{name, owner?, description?}` → 201 `{name, password}`
  - `POST /api/ops/kafka-apps/{name}/register` `{owner?, description?}` → 200 `KafkaAppDetail`
  - `POST /api/ops/kafka-apps/{name}/password` → 200 `{name, password}`
  - `DELETE /api/ops/kafka-apps/{name}` → 204
  - `PUT /api/ops/kafka-apps/{name}/topics/{topic}` `{mode}` → 200 `KafkaAppDetail`
  - `DELETE /api/ops/kafka-apps/{name}/topics/{topic}` → 200 `KafkaAppDetail`
- 에러: `KafkaAppExistsException` 409, `KafkaAppNotFoundException` 404, `ResourceNotFoundException` 404 "브로커에 없는 Kafka 계정입니다", `ClusterAuthorizationException` 403 "kafka-admin 계정에 Cluster Alter 권한이 필요합니다"

- [ ] **Step 1: 실패하는 슬라이스 테스트 작성**

`KafkaAppQueryControllerTest.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.config.SecurityConfig;
import com.osstem.kafkaadmin.kafka.KafkaAppQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppSummary;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicPermission;
import com.osstem.kafkaadmin.ops.KafkaAppNotFoundException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.time.Instant;
import java.util.List;
import static org.mockito.BDDMockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(KafkaAppQueryController.class)
@Import(SecurityConfig.class)
class KafkaAppQueryControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean KafkaAppQueryService queries;

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void DEVELOPER도_목록과_상세를_본다() throws Exception {
        given(queries.listApps()).willReturn(List.of(
                new KafkaAppSummary("order-api", "dev1", "주문", true, 2),
                new KafkaAppSummary("kafka-admin", null, null, false, 0)));
        given(queries.describeApp("order-api")).willReturn(new KafkaAppDetail("order-api", "dev1", "주문",
                Instant.parse("2026-09-04T00:00:00Z"), true,
                List.of(new TopicPermission("orders", "both")), List.of()));
        mvc.perform(get("/api/kafka-apps"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("order-api"))
                .andExpect(jsonPath("$[1].registered").value(false));
        mvc.perform(get("/api/kafka-apps/order-api"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.permissions[0].mode").value("both"));
    }

    @Test
    void 비로그인은_401() throws Exception {
        mvc.perform(get("/api/kafka-apps")).andExpect(status().isUnauthorized());
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 없는_앱은_404() throws Exception {
        given(queries.describeApp("ghost")).willThrow(new KafkaAppNotFoundException("ghost"));
        mvc.perform(get("/api/kafka-apps/ghost"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("등록되지 않은 앱입니다: ghost"));
    }
}
```

`KafkaAppOpsControllerTest.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.config.SecurityConfig;
import com.osstem.kafkaadmin.kafka.KafkaAppQueryService;
import com.osstem.kafkaadmin.kafka.PermissionMode;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.ops.KafkaAppCommandService;
import com.osstem.kafkaadmin.ops.KafkaAppExistsException;
import com.osstem.kafkaadmin.ops.KafkaAppNotFoundException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.util.List;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(KafkaAppOpsController.class)
@Import(SecurityConfig.class)
class KafkaAppOpsControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean KafkaAppCommandService commands;
    @MockitoBean KafkaAppQueryService queries;
    @MockitoBean AuditRecorder recorder;

    private static final KafkaAppDetail DETAIL =
            new KafkaAppDetail("order-api", "dev1", null, null, true, List.of(), List.of());

    @BeforeEach
    void recorderRunsOperation() {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        given(queries.describeApp(any())).willReturn(DETAIL);
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 생성은_201에_비밀번호를_한_번_담고_감사_params에는_없다() throws Exception {
        given(commands.create("order-api", "dev1", "주문")).willReturn("Secret123Secret123Secret");
        mvc.perform(post("/api/ops/kafka-apps").contentType("application/json")
                        .content("{\"name\":\"order-api\",\"owner\":\"dev1\",\"description\":\"주문\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("order-api"))
                .andExpect(jsonPath("$.password").value("Secret123Secret123Secret"));
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_CREATE"), eq("order-api"),
                argThat(p -> p.contains("dev1") && !p.contains("Secret123")), any());
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void DEVELOPER의_변경은_403() throws Exception {
        mvc.perform(post("/api/ops/kafka-apps").contentType("application/json")
                        .content("{\"name\":\"order-api\"}"))
                .andExpect(status().isForbidden());
        mvc.perform(delete("/api/ops/kafka-apps/order-api")).andExpect(status().isForbidden());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 중복은_409() throws Exception {
        given(commands.create(any(), any(), any())).willThrow(new KafkaAppExistsException("order-api"));
        mvc.perform(post("/api/ops/kafka-apps").contentType("application/json")
                        .content("{\"name\":\"order-api\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("이미 존재하는 Kafka 계정입니다: order-api"));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 등록은_200에_상세를_돌려준다() throws Exception {
        mvc.perform(post("/api/ops/kafka-apps/order-api/register").contentType("application/json")
                        .content("{\"owner\":\"dev1\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.registered").value(true));
        then(commands).should().register("order-api", "dev1", null);
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_REGISTER"), eq("order-api"), any(), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 비밀번호_재발급은_params_없이_기록한다() throws Exception {
        given(commands.resetPassword("order-api")).willReturn("NewPw123NewPw123NewPw123");
        mvc.perform(post("/api/ops/kafka-apps/order-api/password"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.password").value("NewPw123NewPw123NewPw123"));
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_RESET_PASSWORD"), eq("order-api"), eq("{}"), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 삭제는_204() throws Exception {
        mvc.perform(delete("/api/ops/kafka-apps/order-api")).andExpect(status().isNoContent());
        then(commands).should().delete("order-api");
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_DELETE"), eq("order-api"), eq("{}"), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 권한_부여는_mode를_파싱하고_상세를_돌려준다() throws Exception {
        mvc.perform(put("/api/ops/kafka-apps/order-api/topics/orders").contentType("application/json")
                        .content("{\"mode\":\"consume\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("order-api"));
        then(commands).should().setTopicPermission("order-api", "orders", PermissionMode.CONSUME);
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_GRANT"), eq("order-api"),
                argThat(p -> p.contains("orders") && p.contains("consume")), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 이상한_mode는_400() throws Exception {
        mvc.perform(put("/api/ops/kafka-apps/order-api/topics/orders").contentType("application/json")
                        .content("{\"mode\":\"admin\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("mode 는 produce, consume, both 중 하나여야 합니다"));
        then(commands).shouldHaveNoInteractions();
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 회수는_200에_상세() throws Exception {
        mvc.perform(delete("/api/ops/kafka-apps/order-api/topics/orders"))
                .andExpect(status().isOk());
        then(commands).should().revokeTopicPermission("order-api", "orders");
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_REVOKE"), eq("order-api"),
                argThat(p -> p.contains("orders")), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 미등록_앱은_404_인가거부는_403() throws Exception {
        willThrow(new KafkaAppNotFoundException("ghost")).given(commands).delete("ghost");
        mvc.perform(delete("/api/ops/kafka-apps/ghost")).andExpect(status().isNotFound());

        willThrow(new ClusterAuthorizationException("denied")).given(commands).delete("order-api");
        mvc.perform(delete("/api/ops/kafka-apps/order-api"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error").value("kafka-admin 계정에 Cluster Alter 권한이 필요합니다"));
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.api.KafkaAppQueryControllerTest' --tests 'com.osstem.kafkaadmin.api.KafkaAppOpsControllerTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 컨트롤러 구현**

`KafkaAppQueryController.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.KafkaAppQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppSummary;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

// Kafka 앱 계정 조회. /api/** 인증만 요구 — DEVELOPER 도 본다. 비밀번호는 어떤 응답에도 없다.
@RestController
@RequestMapping("/api/kafka-apps")
public class KafkaAppQueryController {

    private final KafkaAppQueryService queries;

    public KafkaAppQueryController(KafkaAppQueryService queries) {
        this.queries = queries;
    }

    @GetMapping
    public List<KafkaAppSummary> list() {
        return queries.listApps();
    }

    @GetMapping("/{name}")
    public KafkaAppDetail describe(@PathVariable String name) {
        return queries.describeApp(name);
    }
}
```

`KafkaAppOpsController.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.KafkaAppQueryService;
import com.osstem.kafkaadmin.kafka.PermissionMode;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.ops.KafkaAppCommandService;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

// Kafka 앱 계정 변경(ADMIN 전용). /api/ops/** 는 SecurityConfig 에서 ADMIN 으로 제한된다.
// 비밀번호는 응답 본문에만 담고 감사 params 에는 넣지 않는다 (params 는 여기서 직접 조립).
@RestController
@RequestMapping("/api/ops/kafka-apps")
public class KafkaAppOpsController {

    public record CreateRequest(String name, String owner, String description) {}
    public record RegisterRequest(String owner, String description) {}
    public record PermissionRequest(String mode) {}
    public record PasswordResponse(String name, String password) {}

    private final KafkaAppCommandService commands;
    private final KafkaAppQueryService queries;
    private final AuditRecorder recorder;

    public KafkaAppOpsController(KafkaAppCommandService commands, KafkaAppQueryService queries,
                                 AuditRecorder recorder) {
        this.commands = commands;
        this.queries = queries;
        this.recorder = recorder;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public PasswordResponse create(@RequestBody CreateRequest req, Authentication auth) {
        String[] holder = new String[1];
        recorder.record(auth.getName(), "KAFKA_APP_CREATE", req.name(),
                metaJson(req.owner(), req.description()),
                () -> holder[0] = commands.create(req.name(), req.owner(), req.description()));
        return new PasswordResponse(req.name(), holder[0]);
    }

    @PostMapping("/{name}/register")
    public KafkaAppDetail register(@PathVariable String name, @RequestBody RegisterRequest req,
                                   Authentication auth) {
        recorder.record(auth.getName(), "KAFKA_APP_REGISTER", name,
                metaJson(req.owner(), req.description()),
                () -> commands.register(name, req.owner(), req.description()));
        return queries.describeApp(name);
    }

    @PostMapping("/{name}/password")
    public PasswordResponse resetPassword(@PathVariable String name, Authentication auth) {
        String[] holder = new String[1];
        recorder.record(auth.getName(), "KAFKA_APP_RESET_PASSWORD", name, "{}",
                () -> holder[0] = commands.resetPassword(name));
        return new PasswordResponse(name, holder[0]);
    }

    @DeleteMapping("/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String name, Authentication auth) {
        recorder.record(auth.getName(), "KAFKA_APP_DELETE", name, "{}", () -> commands.delete(name));
    }

    @PutMapping("/{name}/topics/{topic}")
    public KafkaAppDetail setPermission(@PathVariable String name, @PathVariable String topic,
                                        @RequestBody PermissionRequest req, Authentication auth) {
        PermissionMode mode = PermissionMode.parse(req.mode()); // 400 은 record 이전에
        recorder.record(auth.getName(), "KAFKA_APP_GRANT", name,
                "{\"topic\":\"%s\",\"mode\":\"%s\"}".formatted(topic, mode.value()),
                () -> commands.setTopicPermission(name, topic, mode));
        return queries.describeApp(name);
    }

    @DeleteMapping("/{name}/topics/{topic}")
    public KafkaAppDetail revoke(@PathVariable String name, @PathVariable String topic,
                                 Authentication auth) {
        recorder.record(auth.getName(), "KAFKA_APP_REVOKE", name,
                "{\"topic\":\"%s\"}".formatted(topic),
                () -> commands.revokeTopicPermission(name, topic));
        return queries.describeApp(name);
    }

    private static String metaJson(String owner, String description) {
        return "{\"owner\":%s,\"description\":%s}".formatted(quote(owner), quote(description));
    }

    private static String quote(String s) {
        if (s == null) return "null";
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
```

- [ ] **Step 4: 예외 매핑 추가**

`ApiExceptionHandler.java` import 추가:

```java
import com.osstem.kafkaadmin.ops.KafkaAppExistsException;
import com.osstem.kafkaadmin.ops.KafkaAppNotFoundException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.ResourceNotFoundException;
```

클래스 끝(마지막 `}` 앞)에 핸들러 추가:

```java
    @ExceptionHandler(KafkaAppExistsException.class)
    public ResponseEntity<Map<String, String>> kafkaAppExists(KafkaAppExistsException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(KafkaAppNotFoundException.class)
    public ResponseEntity<Map<String, String>> kafkaAppNotFound(KafkaAppNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
    }

    // SCRAM 삭제 대상이 브로커에 없음
    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<Map<String, String>> resourceNotFound(ResourceNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "브로커에 없는 Kafka 계정입니다"));
    }

    // 사이트의 kafka-admin SCRAM 계정에 Cluster ALTER 가 없으면 SCRAM/ACL 변경이 거부된다 (선결 작업: 배포 문서)
    @ExceptionHandler(ClusterAuthorizationException.class)
    public ResponseEntity<Map<String, String>> clusterAuthorization(ClusterAuthorizationException e) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(Map.of("error", "kafka-admin 계정에 Cluster Alter 권한이 필요합니다"));
    }
```

- [ ] **Step 5: 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.api.KafkaAppQueryControllerTest' --tests 'com.osstem.kafkaadmin.api.KafkaAppOpsControllerTest'`
Expected: 13 tests passed

- [ ] **Step 6: 전체 백엔드 테스트**

Run: `cd was && ./gradlew test`
Expected: BUILD SUCCESSFUL (기존 테스트 포함 전부 통과). `OpsControllerTest` 등 기존 슬라이스는 새 컨트롤러를 스캔하지 않으므로 영향 없음.

- [ ] **Step 7: 운영 문서**

`docs/deploy-troubleshooting.md` 의 6번 항목(ACL 인가 거부) 코드 블록 바로 뒤에 추가:

```markdown
   **Kafka 앱 계정 관리 화면(SCRAM·ACL 변경)을 쓰려면** `kafka-admin` 에 Cluster `Alter`/`AlterConfigs`/`Describe`/`DescribeConfigs` 가
   필요하다. 위처럼 `--operation All --cluster` 를 이미 줬다면 추가 작업 없음. 부분 권한만 줬다면:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-acls.sh \
     --bootstrap-server 10.10.10.19:9094 --command-config /etc/kafka/secrets/admin.properties \
     --add --allow-principal User:kafka-admin \
     --operation Alter --operation AlterConfigs --operation Describe --operation DescribeConfigs --cluster
   ```
   미부여 상태에서는 화면이 403 "kafka-admin 계정에 Cluster Alter 권한이 필요합니다" 로 안내한다.
   확인: `kafka-acls.sh --list --principal User:kafka-admin`.
```

`README.md` 문서 목록에 한 줄 추가 (개선 백로그 줄 앞):

```markdown
- [Kafka 앱 계정 관리 설계](docs/superpowers/specs/2026-09-04-kafka-app-accounts-design.md) — 앱별 SCRAM 계정 + 토픽 produce/consume 권한(ACL). 선결: `kafka-admin` Cluster Alter 권한
```

- [ ] **Step 8: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/api/KafkaAppQueryController.java was/src/main/java/com/osstem/kafkaadmin/api/KafkaAppOpsController.java was/src/main/java/com/osstem/kafkaadmin/api/ApiExceptionHandler.java was/src/test/java/com/osstem/kafkaadmin/api/KafkaAppQueryControllerTest.java was/src/test/java/com/osstem/kafkaadmin/api/KafkaAppOpsControllerTest.java docs/deploy-troubleshooting.md README.md
git commit -m "feat(kafka-app): 조회·변경 API, 예외 매핑, 선결 ACL 문서"
```

---
### Task 8: 프론트 타입·영향 요약 함수

**Files:**
- Create: `web/src/lib/kafkaApps.ts`
- Test: `web/src/lib/__tests__/kafkaApps.spec.ts`

**Interfaces:**
- Produces: `type PermissionMode = 'produce' | 'consume' | 'both'`, `interface KafkaAppSummary`, `interface TopicPermission`, `interface RawAcl`, `interface KafkaAppDetail`, `interface PasswordResponse`, `MODE_LABELS: Record<PermissionMode, string>`, `describePermission(app, topic, mode): string`, `isValidAppName(name): boolean`

- [ ] **Step 1: 실패하는 테스트 작성**

```ts
import { describe, it, expect } from 'vitest'
import { describePermission, isValidAppName, MODE_LABELS } from '../kafkaApps'

describe('kafkaApps', () => {
  it('produce 요약에는 그룹 언급이 없다', () => {
    expect(describePermission('order-api', 'orders', 'produce')).toBe(
      "'order-api' 가 'orders' 토픽에 produce(쓰기)합니다",
    )
  })

  it('consume/both 요약은 앱 이름 접두어 그룹을 안내한다', () => {
    expect(describePermission('order-api', 'orders', 'consume')).toBe(
      "'order-api' 가 'orders' 토픽을 consume(읽기)하며 컨슈머 그룹 'order-api*' 를 사용합니다",
    )
    expect(describePermission('order-api', 'orders', 'both')).toBe(
      "'order-api' 가 'orders' 토픽에 produce(쓰기)하고 consume(읽기)하며 컨슈머 그룹 'order-api*' 를 사용합니다",
    )
  })

  it('앱 이름 규칙: 영숫자 . _ - 64자 이하', () => {
    expect(isValidAppName('order-api')).toBe(true)
    expect(isValidAppName('svc.v2_x')).toBe(true)
    expect(isValidAppName('')).toBe(false)
    expect(isValidAppName('bad name')).toBe(false)
    expect(isValidAppName('a'.repeat(65))).toBe(false)
  })

  it('모드 라벨', () => {
    expect(MODE_LABELS.produce).toBe('produce (쓰기)')
    expect(MODE_LABELS.consume).toBe('consume (읽기)')
    expect(MODE_LABELS.both).toBe('both (쓰기+읽기)')
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/lib/__tests__/kafkaApps.spec.ts`
Expected: FAIL (모듈 없음)

- [ ] **Step 3: 구현**

```ts
// Kafka 앱 계정 화면의 공용 타입과 순수 함수. 서버 DTO(Dtos.KafkaAppSummary/KafkaAppDetail)와 필드가 같다.
export type PermissionMode = 'produce' | 'consume' | 'both'

export interface KafkaAppSummary {
  name: string
  owner: string | null
  description: string | null
  registered: boolean
  topicCount: number
}

export interface TopicPermission { topic: string; mode: PermissionMode }
export interface RawAcl { resourceType: string; patternType: string; name: string; operation: string }

export interface KafkaAppDetail {
  name: string
  owner: string | null
  description: string | null
  createdAt: string | null
  registered: boolean
  permissions: TopicPermission[]
  otherAcls: RawAcl[]
}

export interface PasswordResponse { name: string; password: string }

export const MODE_LABELS: Record<PermissionMode, string> = {
  produce: 'produce (쓰기)',
  consume: 'consume (읽기)',
  both: 'both (쓰기+읽기)',
}

const APP_NAME = /^[a-zA-Z0-9._-]{1,64}$/
export function isValidAppName(name: string): boolean {
  return APP_NAME.test(name)
}

// 제출 전 영향 요약. consume 이 포함되면 앱 이름 접두어 그룹 ACL 이 함께 생긴다는 점을 알린다.
export function describePermission(app: string, topic: string, mode: PermissionMode): string {
  const group = `컨슈머 그룹 '${app}*' 를 사용합니다`
  if (mode === 'produce') return `'${app}' 가 '${topic}' 토픽에 produce(쓰기)합니다`
  if (mode === 'consume') return `'${app}' 가 '${topic}' 토픽을 consume(읽기)하며 ${group}`
  return `'${app}' 가 '${topic}' 토픽에 produce(쓰기)하고 consume(읽기)하며 ${group}`
}
```

- [ ] **Step 4: 통과 확인**

Run: `cd web && npx vitest run src/lib/__tests__/kafkaApps.spec.ts`
Expected: 4 tests passed

- [ ] **Step 5: 커밋**

```bash
git add web/src/lib/kafkaApps.ts web/src/lib/__tests__/kafkaApps.spec.ts
git commit -m "feat(web): Kafka 앱 계정 타입과 영향 요약 함수"
```

---

### Task 9: 라우트·메뉴, 목록 화면, 생성/등록 모달

**Files:**
- Create: `web/src/components/PasswordReveal.vue`
- Create: `web/src/components/KafkaAppCreateModal.vue`
- Create: `web/src/views/KafkaAppsView.vue`
- Modify: `web/src/router/index.ts`
- Modify: `web/src/App.vue`
- Modify (있을 때만): `web/src/views/UsersView.vue` — `<h1>계정 관리</h1>` → `<h1>사이트 계정</h1>`
- Test: `web/src/components/__tests__/KafkaAppCreateModal.spec.ts`
- Test: `web/src/views/__tests__/KafkaAppsView.spec.ts`

**Interfaces:**
- Consumes: `lib/kafkaApps.ts`, `ModalDialog`, `useSession`, `api`
- Produces: `PasswordReveal` props `{ name: string; password: string }`
- Produces: `KafkaAppCreateModal` props `{ registerName?: string }` (있으면 등록 모드), emits `close`, `created(name: string)` (등록 모드는 `registered(name)`)
- Produces: 라우트 `/kafka-apps` (상세 라우트 `/kafka-apps/:name` 은 Task 10 에서 뷰와 함께 추가)

- [ ] **Step 1: 실패하는 테스트 작성**

`KafkaAppCreateModal.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import KafkaAppCreateModal from '../KafkaAppCreateModal.vue'

const siteUsers = [
  { id: 1, username: 'admin', role: 'ADMIN' },
  { id: 2, username: 'dev1', role: 'DEVELOPER' },
]

describe('KafkaAppCreateModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('이름이 규칙에 맞기 전엔 생성 버튼 비활성', async () => {
    vi.mocked(api).mockResolvedValueOnce(siteUsers)
    const wrapper = mount(KafkaAppCreateModal)
    await flushPromises()
    expect(wrapper.find('button.primary').attributes('disabled')).toBeDefined()
    await wrapper.find('input[name="name"]').setValue('bad name')
    expect(wrapper.find('button.primary').attributes('disabled')).toBeDefined()
    await wrapper.find('input[name="name"]').setValue('order-api')
    expect(wrapper.find('button.primary').attributes('disabled')).toBeUndefined()
  })

  it('생성 성공 시 비밀번호 단계로 바뀌고 created 를 emit 한다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(siteUsers)
      .mockResolvedValueOnce({ name: 'order-api', password: 'Pw123456Pw123456Pw123456' })
    const wrapper = mount(KafkaAppCreateModal)
    await flushPromises()
    await wrapper.find('input[name="name"]').setValue('order-api')
    await wrapper.find('select[name="owner"]').setValue('dev1')
    await wrapper.find('input[name="description"]').setValue('주문')
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()

    const call = vi.mocked(api).mock.calls[1]!
    expect(call[0]).toBe('/ops/kafka-apps')
    expect(JSON.parse((call[1] as RequestInit).body as string)).toEqual({
      name: 'order-api', owner: 'dev1', description: '주문',
    })
    expect(wrapper.text()).toContain('Pw123456Pw123456Pw123456')
    expect(wrapper.text()).toContain('닫으면 다시 볼 수 없습니다')
    expect(wrapper.find('input[name="name"]').exists()).toBe(false)
    expect(wrapper.emitted('created')).toEqual([['order-api']])
  })

  it('실패하면 에러를 보여주고 폼에 머문다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(siteUsers)
      .mockRejectedValueOnce(new Error('이미 존재하는 Kafka 계정입니다: order-api'))
    const wrapper = mount(KafkaAppCreateModal)
    await flushPromises()
    await wrapper.find('input[name="name"]').setValue('order-api')
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('이미 존재하는 Kafka 계정입니다')
    expect(wrapper.find('input[name="name"]').exists()).toBe(true)
    expect(wrapper.emitted('created')).toBeUndefined()
  })

  it('등록 모드는 이름을 고정하고 register 를 호출하며 비밀번호 단계가 없다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(siteUsers)
      .mockResolvedValueOnce({ name: 'legacy', registered: true, permissions: [], otherAcls: [] })
    const wrapper = mount(KafkaAppCreateModal, { props: { registerName: 'legacy' } })
    await flushPromises()
    expect(wrapper.find('input[name="name"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('legacy')
    await wrapper.find('select[name="owner"]').setValue('dev1')
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    const call = vi.mocked(api).mock.calls[1]!
    expect(call[0]).toBe('/ops/kafka-apps/legacy/register')
    expect(JSON.parse((call[1] as RequestInit).body as string)).toEqual({ owner: 'dev1', description: '' })
    expect(wrapper.emitted('registered')).toEqual([['legacy']])
    expect(wrapper.text()).not.toContain('닫으면 다시 볼 수 없습니다')
  })
})
```

`KafkaAppsView.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const isAdmin = ref(true)
vi.mock('@/composables/useSession', () => ({
  useSession: () => ({ isAdmin, session: ref({ username: 'admin', role: 'ADMIN' }), load: vi.fn() }),
}))

import { api } from '@/api/client'
import KafkaAppsView from '../KafkaAppsView.vue'

const apps = [
  { name: 'order-api', owner: 'dev1', description: '주문', registered: true, topicCount: 2 },
  { name: 'kafka-admin', owner: null, description: null, registered: false, topicCount: 0 },
]

const stubs = { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } }

describe('KafkaAppsView', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    isAdmin.value = true
  })

  it('등록·미등록 계정을 나열하고 ADMIN 에게 추가·등록 버튼을 보여준다', async () => {
    vi.mocked(api).mockResolvedValueOnce(apps)
    const wrapper = mount(KafkaAppsView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('order-api')
    expect(wrapper.text()).toContain('dev1')
    expect(wrapper.text()).toContain('미등록')
    expect(wrapper.find('button.create-app').exists()).toBe(true)
    expect(wrapper.find('button.register-app[data-name="kafka-admin"]').exists()).toBe(true)
    expect(wrapper.find('a[href="/kafka-apps/order-api"]').exists()).toBe(true)
  })

  it('DEVELOPER 에게는 변경 버튼이 없다', async () => {
    isAdmin.value = false
    vi.mocked(api).mockResolvedValueOnce(apps)
    const wrapper = mount(KafkaAppsView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.find('button.create-app').exists()).toBe(false)
    expect(wrapper.find('button.register-app').exists()).toBe(false)
    expect(wrapper.text()).toContain('order-api')
  })

  it('조회 실패 메시지를 표시한다', async () => {
    vi.mocked(api).mockRejectedValueOnce(new Error('브로커 접속 불가'))
    const wrapper = mount(KafkaAppsView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('브로커 접속 불가')
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/components/__tests__/KafkaAppCreateModal.spec.ts src/views/__tests__/KafkaAppsView.spec.ts`
Expected: FAIL (컴포넌트 없음)

- [ ] **Step 3: PasswordReveal 구현**

`web/src/components/PasswordReveal.vue`:

```vue
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
```

- [ ] **Step 4: KafkaAppCreateModal 구현**

`web/src/components/KafkaAppCreateModal.vue`:

```vue
<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { api } from '@/api/client'
import { isValidAppName, type KafkaAppDetail, type PasswordResponse } from '@/lib/kafkaApps'
import ModalDialog from './ModalDialog.vue'
import PasswordReveal from './PasswordReveal.vue'

// registerName 이 있으면 "미등록 계정 등록" 모드: 이름 고정, 비밀번호 단계 없음.
const props = defineProps<{ registerName?: string }>()
const emit = defineEmits<{ close: []; created: [name: string]; registered: [name: string] }>()

interface SiteUser { id: number; username: string; role: string }

const isRegister = computed(() => !!props.registerName)
const name = ref(props.registerName ?? '')
const owner = ref('')
const description = ref('')
const siteUsers = ref<SiteUser[]>([])
const error = ref('')
const submitting = ref(false)
const result = ref<PasswordResponse | null>(null)

const canSubmit = computed(() => isValidAppName(name.value) && !submitting.value)

onMounted(async () => {
  try {
    siteUsers.value = await api<SiteUser[]>('/ops/users')
  } catch {
    siteUsers.value = [] // 담당자 목록은 부가 정보 — 실패해도 생성은 가능
  }
})

async function submit() {
  error.value = ''
  submitting.value = true
  try {
    if (isRegister.value) {
      await api<KafkaAppDetail>(`/ops/kafka-apps/${name.value}/register`, {
        method: 'POST',
        body: JSON.stringify({ owner: owner.value, description: description.value }),
      })
      emit('registered', name.value)
    } else {
      result.value = await api<PasswordResponse>('/ops/kafka-apps', {
        method: 'POST',
        body: JSON.stringify({ name: name.value, owner: owner.value, description: description.value }),
      })
      emit('created', name.value)
    }
  } catch (e) {
    error.value = e instanceof Error ? e.message : '요청 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog :title="isRegister ? '미등록 계정 등록' : '앱 계정 추가'" @close="emit('close')">
    <PasswordReveal v-if="result" :name="result.name" :password="result.password" />
    <form v-else class="form" @submit.prevent="submit">
      <label v-if="!isRegister">
        앱 이름 (SCRAM 사용자명)
        <input name="name" v-model="name" placeholder="order-api" autocomplete="off" />
        <small>영문·숫자·'.', '_', '-' 만, 64자 이하</small>
      </label>
      <p v-else>브로커에 있는 <strong>{{ name }}</strong> 계정에 담당자·설명을 붙입니다. 비밀번호는 바뀌지 않습니다.</p>
      <label>
        담당 개발자 (선택)
        <select name="owner" v-model="owner">
          <option value="">— 없음 —</option>
          <option v-for="u in siteUsers" :key="u.id" :value="u.username">{{ u.username }}</option>
        </select>
      </label>
      <label>
        설명 (선택)
        <input name="description" v-model="description" placeholder="주문 서비스" />
      </label>
      <p v-if="error" class="error">{{ error }}</p>
    </form>
    <template #footer>
      <button v-if="result" type="button" class="btn primary" @click="emit('close')">닫기</button>
      <template v-else>
        <button type="button" class="btn" @click="emit('close')">취소</button>
        <button type="button" class="btn primary" :disabled="!canSubmit" @click="submit">
          {{ submitting ? '처리 중…' : isRegister ? '등록' : '생성' }}
        </button>
      </template>
    </template>
  </ModalDialog>
</template>

<style scoped>
.form { display: flex; flex-direction: column; gap: 0.75rem; }
label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
small { color: var(--ink-soft); }
.error { color: var(--crit, #c0392b); margin: 0; }
</style>
```

- [ ] **Step 5: KafkaAppsView 구현**

`web/src/views/KafkaAppsView.vue`:

```vue
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
```

- [ ] **Step 6: 라우트·메뉴 수정**

`web/src/router/index.ts` routes 배열의 `/users` 줄 앞에 추가:

```ts
    { path: '/kafka-apps', component: () => import('@/views/KafkaAppsView.vue') },
```

`web/src/App.vue` 의 nav 에서 `계정 관리` 링크 줄을 아래 두 줄로 교체:

```vue
    <RouterLink to="/kafka-apps">Kafka 계정</RouterLink>
    <RouterLink v-if="isAdmin" to="/users">사이트 계정</RouterLink>
```

`web/src/views/UsersView.vue` 가 있으면 `<h1>계정 관리</h1>` → `<h1>사이트 계정</h1>`, 그리고 `hint` 문구 앞에 `이 사이트에 로그인하는 계정입니다. Kafka 접속 계정은 "Kafka 계정" 메뉴에서 관리합니다.` 를 추가한다. 없으면 건너뛴다.

- [ ] **Step 7: 통과 확인**

Run: `cd web && npx vitest run src/components/__tests__/KafkaAppCreateModal.spec.ts src/views/__tests__/KafkaAppsView.spec.ts && npm run type-check`
Expected: 7 tests passed, type-check 통과 (`/kafka-apps/:name` 라우트는 아직 없어도 RouterLink 는 컴파일된다)

- [ ] **Step 8: 커밋**

```bash
git add web/src/components/PasswordReveal.vue web/src/components/KafkaAppCreateModal.vue web/src/views/KafkaAppsView.vue web/src/router/index.ts web/src/App.vue web/src/components/__tests__/KafkaAppCreateModal.spec.ts web/src/views/__tests__/KafkaAppsView.spec.ts
git add web/src/views/UsersView.vue 2>/dev/null || true
git commit -m "feat(web): Kafka 계정 목록 화면과 생성/등록 모달"
```

---
### Task 10: 상세 화면과 권한·삭제·재발급 모달

**Files:**
- Create: `web/src/components/KafkaAppPermissionModal.vue`
- Create: `web/src/components/KafkaAppDeleteModal.vue`
- Create: `web/src/components/KafkaAppResetPasswordModal.vue`
- Create: `web/src/views/KafkaAppDetailView.vue`
- Modify: `web/src/router/index.ts` (`/kafka-apps/:name` 추가)
- Test: `web/src/components/__tests__/KafkaAppPermissionModal.spec.ts`
- Test: `web/src/components/__tests__/KafkaAppDeleteModal.spec.ts`
- Test: `web/src/views/__tests__/KafkaAppDetailView.spec.ts`

**Interfaces:**
- Consumes: `lib/kafkaApps.ts` (`describePermission`, `MODE_LABELS`, 타입), `ModalDialog`, `PasswordReveal`, `useSession`, `api`
- Produces: `KafkaAppPermissionModal` props `{ app: string; topic?: string; mode?: PermissionMode }` (topic 이 있으면 변경 모드), emits `close`, `saved(detail: KafkaAppDetail)`
- Produces: `KafkaAppDeleteModal` props `{ name: string }`, emits `close`, `deleted`
- Produces: `KafkaAppResetPasswordModal` props `{ name: string }`, emits `close`

- [ ] **Step 1: 실패하는 테스트 작성**

`KafkaAppPermissionModal.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import KafkaAppPermissionModal from '../KafkaAppPermissionModal.vue'

const topics = [
  { name: 'orders', partitionCount: 3, replicationFactor: 3 },
  { name: 'events', partitionCount: 1, replicationFactor: 3 },
]
const detail = { name: 'order-api', owner: null, description: null, createdAt: null, registered: true,
  permissions: [{ topic: 'orders', mode: 'consume' }], otherAcls: [] }

describe('KafkaAppPermissionModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('토픽과 모드를 고르면 영향 요약이 바뀌고 PUT 을 호출한다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics).mockResolvedValueOnce(detail)
    const wrapper = mount(KafkaAppPermissionModal, { props: { app: 'order-api' } })
    await flushPromises()
    expect(wrapper.find('button.primary').attributes('disabled')).toBeDefined()
    await wrapper.find('select[name="topic"]').setValue('orders')
    await wrapper.find('select[name="mode"]').setValue('consume')
    expect(wrapper.text()).toContain("컨슈머 그룹 'order-api*'")
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    const call = vi.mocked(api).mock.calls[1]!
    expect(call[0]).toBe('/ops/kafka-apps/order-api/topics/orders')
    expect(call[1]).toMatchObject({ method: 'PUT' })
    expect(JSON.parse((call[1] as RequestInit).body as string)).toEqual({ mode: 'consume' })
    expect(wrapper.emitted('saved')).toEqual([[detail]])
  })

  it('변경 모드는 토픽을 고정하고 현재 모드를 기본값으로 둔다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics)
    const wrapper = mount(KafkaAppPermissionModal, { props: { app: 'order-api', topic: 'orders', mode: 'produce' } })
    await flushPromises()
    expect(wrapper.find('select[name="topic"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('orders')
    expect((wrapper.find('select[name="mode"]').element as HTMLSelectElement).value).toBe('produce')
    expect(wrapper.find('button.primary').attributes('disabled')).toBeUndefined()
  })

  it('실패하면 에러를 표시한다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics).mockRejectedValueOnce(new Error('존재하지 않는 토픽입니다'))
    const wrapper = mount(KafkaAppPermissionModal, { props: { app: 'order-api', topic: 'ghost', mode: 'produce' } })
    await flushPromises()
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('존재하지 않는 토픽입니다')
    expect(wrapper.emitted('saved')).toBeUndefined()
  })
})
```

`KafkaAppDeleteModal.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import KafkaAppDeleteModal from '../KafkaAppDeleteModal.vue'

describe('KafkaAppDeleteModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('앱 이름이 일치하기 전에는 삭제 버튼이 비활성', async () => {
    const wrapper = mount(KafkaAppDeleteModal, { props: { name: 'order-api' } })
    expect(wrapper.find('button.danger').attributes('disabled')).toBeDefined()
    await wrapper.find('input').setValue('order-ap')
    expect(wrapper.find('button.danger').attributes('disabled')).toBeDefined()
  })

  it('일치하면 DELETE 를 호출하고 deleted 를 emit 한다', async () => {
    vi.mocked(api).mockResolvedValueOnce(undefined)
    const wrapper = mount(KafkaAppDeleteModal, { props: { name: 'order-api' } })
    await wrapper.find('input').setValue('order-api')
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/kafka-apps/order-api', { method: 'DELETE' })
    expect(wrapper.emitted('deleted')).toHaveLength(1)
  })

  it('실패하면 에러를 표시하고 deleted 를 emit 하지 않는다', async () => {
    vi.mocked(api).mockRejectedValueOnce(new Error('kafka-admin 계정에 Cluster Alter 권한이 필요합니다'))
    const wrapper = mount(KafkaAppDeleteModal, { props: { name: 'order-api' } })
    await wrapper.find('input').setValue('order-api')
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('Cluster Alter 권한')
    expect(wrapper.emitted('deleted')).toBeUndefined()
  })
})
```

`KafkaAppDetailView.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const isAdmin = ref(true)
vi.mock('@/composables/useSession', () => ({
  useSession: () => ({ isAdmin, session: ref({ username: 'admin', role: 'ADMIN' }), load: vi.fn() }),
}))
vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { name: 'order-api' } }),
  useRouter: () => ({ push: vi.fn() }),
}))

import { api } from '@/api/client'
import KafkaAppDetailView from '../KafkaAppDetailView.vue'

const detail = {
  name: 'order-api', owner: 'dev1', description: '주문', createdAt: '2026-09-04T00:00:00Z', registered: true,
  permissions: [{ topic: 'orders', mode: 'both' }, { topic: 'events', mode: 'consume' }],
  otherAcls: [{ resourceType: 'TOPIC', patternType: 'PREFIXED', name: 'legacy-', operation: 'WRITE' }],
}

describe('KafkaAppDetailView', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    isAdmin.value = true
  })

  it('메타데이터·권한 표·기타 ACL 을 보여주고 ADMIN 에게 변경 버튼을 노출한다', async () => {
    vi.mocked(api).mockResolvedValueOnce(detail)
    const wrapper = mount(KafkaAppDetailView)
    await flushPromises()
    expect(wrapper.text()).toContain('dev1')
    expect(wrapper.text()).toContain('both (쓰기+읽기)')
    expect(wrapper.text()).toContain("'order-api*'")
    expect(wrapper.text()).toContain('legacy-')
    expect(wrapper.find('button.add-permission').exists()).toBe(true)
    expect(wrapper.find('button.revoke[data-topic="orders"]').exists()).toBe(true)
    expect(wrapper.find('button.reset-password').exists()).toBe(true)
    expect(wrapper.find('button.delete-app').exists()).toBe(true)
  })

  it('DEVELOPER 에게는 변경 버튼이 없다', async () => {
    isAdmin.value = false
    vi.mocked(api).mockResolvedValueOnce(detail)
    const wrapper = mount(KafkaAppDetailView)
    await flushPromises()
    expect(wrapper.find('button.add-permission').exists()).toBe(false)
    expect(wrapper.find('button.revoke').exists()).toBe(false)
    expect(wrapper.find('button.delete-app').exists()).toBe(false)
    expect(wrapper.text()).toContain('orders')
  })

  it('회수는 DELETE 를 호출하고 응답으로 표를 갱신한다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(detail)
      .mockResolvedValueOnce({ ...detail, permissions: [{ topic: 'events', mode: 'consume' }] })
    const wrapper = mount(KafkaAppDetailView)
    await flushPromises()
    await wrapper.find('button.revoke[data-topic="orders"]').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/kafka-apps/order-api/topics/orders', { method: 'DELETE' })
    expect(wrapper.find('button.revoke[data-topic="orders"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('events')
  })

  it('미등록 계정은 변경 대신 등록 안내를 보여준다', async () => {
    vi.mocked(api).mockResolvedValueOnce({ ...detail, registered: false, owner: null, permissions: [] })
    const wrapper = mount(KafkaAppDetailView)
    await flushPromises()
    expect(wrapper.text()).toContain('미등록')
    expect(wrapper.find('button.add-permission').exists()).toBe(false)
    expect(wrapper.find('button.register-app').exists()).toBe(true)
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/components/__tests__/KafkaAppPermissionModal.spec.ts src/components/__tests__/KafkaAppDeleteModal.spec.ts src/views/__tests__/KafkaAppDetailView.spec.ts`
Expected: FAIL (컴포넌트 없음)

- [ ] **Step 3: KafkaAppPermissionModal 구현**

```vue
<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { api } from '@/api/client'
import { describePermission, MODE_LABELS, type KafkaAppDetail, type PermissionMode } from '@/lib/kafkaApps'
import ModalDialog from './ModalDialog.vue'

// topic 이 주어지면 "변경" 모드(토픽 고정), 없으면 "추가" 모드(토픽 선택)
const props = defineProps<{ app: string; topic?: string; mode?: PermissionMode }>()
const emit = defineEmits<{ close: []; saved: [detail: KafkaAppDetail] }>()

interface TopicSummary { name: string; partitionCount: number; replicationFactor: number }

const topics = ref<TopicSummary[]>([])
const topic = ref(props.topic ?? '')
const mode = ref<PermissionMode>(props.mode ?? 'produce')
const error = ref('')
const submitting = ref(false)
const isEdit = computed(() => !!props.topic)
const canSubmit = computed(() => !!topic.value && !submitting.value)
const summary = computed(() => (topic.value ? describePermission(props.app, topic.value, mode.value) : ''))

onMounted(async () => {
  try {
    topics.value = await api<TopicSummary[]>('/topics')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '토픽 목록 조회 실패'
  }
})

async function submit() {
  error.value = ''
  submitting.value = true
  try {
    const detail = await api<KafkaAppDetail>(`/ops/kafka-apps/${props.app}/topics/${topic.value}`, {
      method: 'PUT',
      body: JSON.stringify({ mode: mode.value }),
    })
    emit('saved', detail)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '권한 설정 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog :title="isEdit ? '권한 변경' : '권한 추가'" @close="emit('close')">
    <div class="form">
      <label v-if="!isEdit">
        토픽
        <select name="topic" v-model="topic">
          <option value="">— 선택 —</option>
          <option v-for="t in topics" :key="t.name" :value="t.name">{{ t.name }}</option>
        </select>
      </label>
      <p v-else>토픽 <strong>{{ topic }}</strong></p>
      <label>
        모드
        <select name="mode" v-model="mode">
          <option v-for="(label, value) in MODE_LABELS" :key="value" :value="value">{{ label }}</option>
        </select>
      </label>
      <p v-if="summary" class="summary">{{ summary }}</p>
      <p v-if="error" class="error">{{ error }}</p>
    </div>
    <template #footer>
      <button type="button" class="btn" @click="emit('close')">취소</button>
      <button type="button" class="btn primary" :disabled="!canSubmit" @click="submit">
        {{ submitting ? '적용 중…' : '적용' }}
      </button>
    </template>
  </ModalDialog>
</template>

<style scoped>
.form { display: flex; flex-direction: column; gap: 0.75rem; }
label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
.summary { margin: 0; padding: 0.5rem 0.75rem; background: var(--surface-2); border-radius: 6px; }
.error { color: var(--crit, #c0392b); margin: 0; }
</style>
```

- [ ] **Step 4: KafkaAppDeleteModal 구현**

```vue
<script setup lang="ts">
import { ref, computed } from 'vue'
import { api } from '@/api/client'
import ModalDialog from './ModalDialog.vue'

const props = defineProps<{ name: string }>()
const emit = defineEmits<{ close: []; deleted: [] }>()

const confirmText = ref('')
const error = ref('')
const submitting = ref(false)
const canDelete = computed(() => confirmText.value === props.name)

async function remove() {
  error.value = ''
  submitting.value = true
  try {
    await api(`/ops/kafka-apps/${props.name}`, { method: 'DELETE' })
    emit('deleted')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '삭제 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog title="Kafka 계정 삭제" @close="emit('close')">
    <p>
      <strong>'{{ name }}'</strong> 계정과 이 계정의 모든 토픽 권한(ACL)이 함께 삭제됩니다.
      이 계정으로 접속 중인 앱은 즉시 끊깁니다. 되돌릴 수 없습니다.
    </p>
    <label>
      계속하려면 앱 이름을 입력하세요
      <input v-model="confirmText" :placeholder="name" />
    </label>
    <p v-if="error" class="error">{{ error }}</p>
    <template #footer>
      <button type="button" class="btn" @click="emit('close')">취소</button>
      <button type="button" class="btn danger" :disabled="!canDelete || submitting" @click="remove">삭제</button>
    </template>
  </ModalDialog>
</template>

<style scoped>
label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
.error { color: var(--crit, #c0392b); margin: 0.5rem 0 0; }
</style>
```

- [ ] **Step 5: KafkaAppResetPasswordModal 구현**

```vue
<script setup lang="ts">
import { ref } from 'vue'
import { api } from '@/api/client'
import type { PasswordResponse } from '@/lib/kafkaApps'
import ModalDialog from './ModalDialog.vue'
import PasswordReveal from './PasswordReveal.vue'

const props = defineProps<{ name: string }>()
const emit = defineEmits<{ close: [] }>()

const result = ref<PasswordResponse | null>(null)
const error = ref('')
const submitting = ref(false)

async function reset() {
  error.value = ''
  submitting.value = true
  try {
    result.value = await api<PasswordResponse>(`/ops/kafka-apps/${props.name}/password`, { method: 'POST' })
  } catch (e) {
    error.value = e instanceof Error ? e.message : '재발급 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog title="비밀번호 재발급" @close="emit('close')">
    <PasswordReveal v-if="result" :name="result.name" :password="result.password" />
    <template v-else>
      <p>
        <strong>'{{ name }}'</strong> 의 비밀번호를 새로 만듭니다. 기존 비밀번호로 접속 중인 앱은
        재접속 시 인증에 실패하므로, 새 비밀번호를 앱 설정에 반영한 뒤 재기동해야 합니다.
      </p>
      <p v-if="error" class="error">{{ error }}</p>
    </template>
    <template #footer>
      <button v-if="result" type="button" class="btn primary" @click="emit('close')">닫기</button>
      <template v-else>
        <button type="button" class="btn" @click="emit('close')">취소</button>
        <button type="button" class="btn danger" :disabled="submitting" @click="reset">재발급</button>
      </template>
    </template>
  </ModalDialog>
</template>

<style scoped>
.error { color: var(--crit, #c0392b); margin: 0.5rem 0 0; }
</style>
```

- [ ] **Step 6: KafkaAppDetailView 구현**

```vue
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
```

- [ ] **Step 7: 라우트 추가**

`web/src/router/index.ts` 의 `/kafka-apps` 줄 바로 뒤에:

```ts
    { path: '/kafka-apps/:name', component: () => import('@/views/KafkaAppDetailView.vue') },
```

- [ ] **Step 8: 통과 확인**

Run: `cd web && npx vitest run && npm run type-check`
Expected: 새 스펙 10건 포함 전부 통과, type-check 통과. `KafkaAppDetailView.spec` 은 `RouterLink` 를 전역 등록하지 않으므로 Vue 가 unknown element 경고를 낼 수 있다. 경고가 테스트를 깨지는 않지만 거슬리면 `KafkaAppsView.spec` 과 같은 `stubs` 를 `mount` 옵션에 넣는다.

- [ ] **Step 9: 커밋**

```bash
git add web/src/components/KafkaAppPermissionModal.vue web/src/components/KafkaAppDeleteModal.vue web/src/components/KafkaAppResetPasswordModal.vue web/src/views/KafkaAppDetailView.vue web/src/router/index.ts web/src/components/__tests__/KafkaAppPermissionModal.spec.ts web/src/components/__tests__/KafkaAppDeleteModal.spec.ts web/src/views/__tests__/KafkaAppDetailView.spec.ts
git commit -m "feat(web): Kafka 계정 상세 화면과 권한·삭제·재발급 모달"
```

---

### Task 11: 로컬 실브로커 검증

**Files:** 없음 (검증만). 실패 시 해당 Task 로 돌아가 고친다.

- [ ] **Step 1: 백엔드 전체 테스트와 프론트 빌드**

Run: `cd was && ./gradlew test && cd ../web && npm run build`
Expected: 둘 다 성공

- [ ] **Step 2: 로컬 기동 (운영 클러스터, `was/config/application-local.yml`)**

Run (터미널 1): `cd was && ./gradlew bootRun`
Run (터미널 2): `cd web && npm run dev`

메모리가 부족한 머신에서는 `./gradlew bootJar -x test && java -Xmx512m -Dspring.profiles.active=local -jar build/libs/kafka-admin-was-0.0.1-SNAPSHOT.jar` 로 대신 띄운다.

- [ ] **Step 3: API 로 왕복 확인**

```bash
S=/tmp/kafka-app-check; mkdir -p $S
curl -s -c $S/ck -H 'Content-Type: application/json' -d '{"username":"admin","password":"devpw"}' localhost:8080/api/auth/login
curl -s -b $S/ck localhost:8080/api/kafka-apps | head -c 400; echo
curl -s -b $S/ck -H 'Content-Type: application/json' -d '{"name":"check-app","owner":"admin","description":"검증"}' localhost:8080/api/ops/kafka-apps
curl -s -b $S/ck -X PUT -H 'Content-Type: application/json' -d '{"mode":"consume"}' localhost:8080/api/ops/kafka-apps/check-app/topics/smoke-test
curl -s -b $S/ck localhost:8080/api/kafka-apps/check-app
curl -s -b $S/ck -X DELETE localhost:8080/api/ops/kafka-apps/check-app -w '%{http_code}\n'
```

Expected: 목록에 `kafka-admin` 이 `registered:false` 로 보임. 생성은 201 에 24자 비밀번호. 권한 부여 응답의 `permissions` 에 `{"topic":"smoke-test","mode":"consume"}`. 삭제 204.
403 "kafka-admin 계정에 Cluster Alter 권한이 필요합니다" 가 나오면 `docs/deploy-troubleshooting.md` 의 선결 ACL 을 운영 브로커에 적용한 뒤 다시 시도한다(브로커 재시작 불필요).

- [ ] **Step 4: 화면 확인**

http://localhost:5173/kafka-apps 에서 "앱 계정 추가" → 비밀번호 1회 표시 → 상세에서 "권한 추가"(smoke-test, consume) → 영향 요약 문구 확인 → 회수 → "삭제"(이름 타이핑) 순으로 눌러 본다. 상단 메뉴에 "Kafka 계정", "사이트 계정"(ADMIN) 이 보이는지 확인한다.

- [ ] **Step 5: 감사 로그 확인**

```bash
curl -s -b $S/ck 'localhost:8080/api/ops/audit-logs?size=10'
```

Expected: `KAFKA_APP_CREATE`, `KAFKA_APP_GRANT`, `KAFKA_APP_DELETE` 항목이 있고 `params` 어디에도 비밀번호가 없다.

- [ ] **Step 6: 마무리**

`superpowers:finishing-a-development-branch` 스킬로 브랜치 정리(`feature/kafka-app-accounts` → `production` 머지 또는 PR).
