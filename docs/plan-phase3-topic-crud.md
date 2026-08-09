# 토픽 생성·수정·삭제 구현 계획 (3단계 첫 슬라이스)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ops 모듈 골격(감사 로그 포함)과 토픽 생성·수정·삭제 API + 우측 상단 버튼/팝업 UI를 구현한다.

**Architecture:** 쓰기 작업 전용 `ops` 패키지를 신설하고(조회 전용 `kafka/` 불변), 모든 조치를 `AuditRecorder`로 감싸 감사 로그를 강제한다. `/api/ops/**`는 ADMIN 전용. 프론트는 `useSession`으로 role을 읽어 버튼을 노출하고, 공용 `ModalDialog` 위에 팝업 3종을 얹는다. 스펙: `docs/phase3-topic-crud-design.md`.

**Tech Stack:** Spring Boot 4.1 / Java 21 / kafka-clients Admin / JPA+H2 / Testcontainers / Vue 3 + TS / vitest.

## Global Constraints

- **실행 전 정리**: 작업 트리에 미커밋 변경(모니터 화면·LagHelp 등)이 있다. 실행 시작 전에 사용자와 커밋/스태시로 정리하고, 이 기능은 별도 브랜치(`feature/topic-crud`)에서 진행한다. 뷰 수정 코드의 앵커가 실제 파일과 다르면 의미를 유지한 채 맞춘다.
- 커밋 메시지: 기존 관례(`feat:`/`fix:`/`docs:` + 영어 소문자 명령형).
- 산문·UI 문구는 한국어. 코드 주석은 "왜"를 설명하는 경우에만.
- 조회 전용 `kafka/` 패키지는 수정하지 않는다 (`ApiExceptionHandler`는 `api/` 소속이라 수정 대상).
- Bean Validation 의존성을 추가하지 않는다 — 검증은 서비스에서 `IllegalArgumentException`으로 수행(→ 400).
- H2 예약어 컬럼명 금지 (기존 `alert_value` 사례 참조). 테스트 데이터는 고유 접두어 사용.
- Testcontainers는 싱글턴 컨테이너 패턴(`KafkaIntegrationTestBase` 상속), `@Container` 금지.
- 백엔드 테스트: `cd was && ./gradlew test`, 프론트: `cd web && npm test` (vitest), 타입 검사: `npm run type-check` (있는 경우 — package.json 확인).

---

### Task 1: AuditLog 엔티티 + AuditRecorder

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/ops/AuditLog.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/ops/AuditLogRepository.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/ops/AuditRecorder.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/ops/AuditRecorderTest.java`

**Interfaces:**
- Consumes: 없음 (첫 태스크)
- Produces: `AuditRecorder.record(String actor, String action, String target, String paramsJson, Runnable operation)` — 성공/실패 모두 로그 저장 후, 실패면 원래 예외 재던짐. `AuditLogRepository.findAllByOrderByExecutedAtDesc(Pageable)` — Task 3 이 사용.

- [ ] **Step 1: 실패하는 테스트 작성**

`was/src/test/java/com/osstem/kafkaadmin/ops/AuditRecorderTest.java`:

```java
package com.osstem.kafkaadmin.ops;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import static org.assertj.core.api.Assertions.*;

// H2 인메모리로 JPA 저장까지 검증한다. named DB 공유를 피하려고 고유 이름을 쓴다.
@SpringBootTest
@TestPropertySource(properties = "spring.datasource.url=jdbc:h2:mem:audit-recorder-test;DB_CLOSE_DELAY=-1")
class AuditRecorderTest {

    @Autowired AuditRecorder recorder;
    @Autowired AuditLogRepository repository;

    @Test
    void 성공한_조치는_SUCCESS로_기록된다() {
        recorder.record("audit-t-admin", "TOPIC_CREATE", "audit-t-orders", "{\"partitions\":3}",
                () -> {});
        AuditLog log = repository.findAll().stream()
                .filter(l -> l.getTarget().equals("audit-t-orders")).findFirst().orElseThrow();
        assertThat(log.getActor()).isEqualTo("audit-t-admin");
        assertThat(log.getResult()).isEqualTo("SUCCESS");
        assertThat(log.getErrorMessage()).isNull();
        assertThat(log.getExecutedAt()).isNotNull();
    }

    @Test
    void 실패한_조치는_FAILED로_기록되고_예외는_다시_던진다() {
        assertThatThrownBy(() ->
                recorder.record("audit-t-admin", "TOPIC_DELETE", "audit-t-broken", "{}",
                        () -> { throw new IllegalStateException("boom"); }))
                .isInstanceOf(IllegalStateException.class);
        AuditLog log = repository.findAll().stream()
                .filter(l -> l.getTarget().equals("audit-t-broken")).findFirst().orElseThrow();
        assertThat(log.getResult()).isEqualTo("FAILED");
        assertThat(log.getErrorMessage()).isEqualTo("boom");
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests AuditRecorderTest`
Expected: 컴파일 실패 (AuditRecorder 없음)

- [ ] **Step 3: 구현**

`was/src/main/java/com/osstem/kafkaadmin/ops/AuditLog.java`:

```java
package com.osstem.kafkaadmin.ops;

import jakarta.persistence.*;
import java.time.Instant;

// 조치 감사 로그 1건. action: TOPIC_CREATE | TOPIC_UPDATE | TOPIC_DELETE (이후 조치가 값 추가)
@Entity
@Table(name = "audit_log", indexes = @Index(name = "idx_audit_executed", columnList = "executedAt"))
public class AuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String actor;
    private String action;
    private String target;
    @Column(length = 2000)
    private String params;
    @Column(name = "audit_result") // RESULT는 H2 키워드라 회피 (기존 alert_value 관례)
    private String result;
    @Column(length = 2000)
    private String errorMessage;
    private Instant executedAt;

    protected AuditLog() {}

    public AuditLog(String actor, String action, String target, String params,
                    String result, String errorMessage, Instant executedAt) {
        this.actor = actor;
        this.action = action;
        this.target = target;
        this.params = params;
        this.result = result;
        this.errorMessage = errorMessage;
        this.executedAt = executedAt;
    }

    public Long getId() { return id; }
    public String getActor() { return actor; }
    public String getAction() { return action; }
    public String getTarget() { return target; }
    public String getParams() { return params; }
    public String getResult() { return result; }
    public String getErrorMessage() { return errorMessage; }
    public Instant getExecutedAt() { return executedAt; }
}
```

`was/src/main/java/com/osstem/kafkaadmin/ops/AuditLogRepository.java`:

```java
package com.osstem.kafkaadmin.ops;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface AuditLogRepository extends JpaRepository<AuditLog, Long> {
    List<AuditLog> findAllByOrderByExecutedAtDesc(Pageable pageable);
}
```

`was/src/main/java/com/osstem/kafkaadmin/ops/AuditRecorder.java`:

```java
package com.osstem.kafkaadmin.ops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.time.Instant;

// 모든 조치는 이 래퍼를 지나야 한다: 성공/실패 관계없이 감사 로그를 남긴다.
// 컨트롤러 경로에는 트랜잭션이 없으므로 save()는 자체 트랜잭션으로 즉시 커밋된다.
// 감사 저장 실패가 조치 결과를 삼키지 않도록 저장은 별도 try/catch 로 격리한다.
@Component
public class AuditRecorder {

    private static final Logger log = LoggerFactory.getLogger(AuditRecorder.class);
    private final AuditLogRepository repository;

    public AuditRecorder(AuditLogRepository repository) {
        this.repository = repository;
    }

    public void record(String actor, String action, String target, String paramsJson,
                       Runnable operation) {
        try {
            operation.run();
            save(new AuditLog(actor, action, target, paramsJson, "SUCCESS", null, Instant.now()));
        } catch (RuntimeException e) {
            save(new AuditLog(actor, action, target, paramsJson, "FAILED",
                    truncate(e.getMessage()), Instant.now()));
            throw e;
        }
    }

    private void save(AuditLog entry) {
        try {
            repository.save(entry);
        } catch (RuntimeException e) {
            log.error("감사 로그 저장 실패: action={} target={}", entry.getAction(), entry.getTarget(), e);
        }
    }

    private String truncate(String message) {
        if (message == null) return null;
        return message.length() > 2000 ? message.substring(0, 2000) : message;
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `cd was && ./gradlew test --tests AuditRecorderTest`
Expected: 2 tests PASS. (H2 예약어 충돌이 있으면 스키마 생성 단계에서 SQL 오류가 난다 — `audit_result` 컬럼명으로 이미 회피했는지 확인)

- [ ] **Step 5: Commit**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/ops was/src/test/java/com/osstem/kafkaadmin/ops
git commit -m "feat: add ops audit log and recorder"
```

---

### Task 2: OpsFutures + TopicCommandService (+실브로커 IT)

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/ops/OpsFutures.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/ops/TopicCommandService.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/ops/TopicCommandServiceIT.java`

**Interfaces:**
- Consumes: `Admin` 빈 (기존 KafkaClientConfig), `KafkaIntegrationTestBase` (기존, kafka 패키지 — public abstract라 상속 가능), `KafkaUnavailableException` (기존)
- Produces: `TopicCommandService.createTopic(String name, int partitions, short replicationFactor, Map<String,String> configs)` / `updateTopic(String name, Integer partitions, Map<String,String> configs)` / `deleteTopic(String name)` — Task 3 이 호출. 검증 실패는 `IllegalArgumentException`.

**배경:** 기존 `KafkaFutures.await`는 모든 실행 예외를 `KafkaUnavailableException`(→503)으로 감싼다. 조치는 409/404/400 구분이 필요하므로, 알려진 Kafka 예외는 그대로 통과시키는 ops 전용 await 를 만든다. (`TimeoutException`도 `ApiException`의 하위라 "ApiException 전부 통과" 방식은 쓰면 안 된다 — 명시 목록으로 통과시킨다.)

- [ ] **Step 1: 실패하는 IT 작성**

`was/src/test/java/com/osstem/kafkaadmin/ops/TopicCommandServiceIT.java`:

```java
package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.KafkaIntegrationTestBase;
import com.osstem.kafkaadmin.kafka.TopicQueryService;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;

class TopicCommandServiceIT extends KafkaIntegrationTestBase {

    @Autowired TopicCommandService commands;
    @Autowired TopicQueryService queries;

    @Test
    void 생성_수정_삭제_왕복() {
        String topic = "ops-t-roundtrip";
        commands.createTopic(topic, 3, (short) 1, Map.of("retention.ms", "3600000"));
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.describeTopic(topic).partitions()).hasSize(3));
        assertThat(queries.describeTopic(topic).configs()).containsEntry("retention.ms", "3600000");

        commands.updateTopic(topic, 6, Map.of("retention.ms", "7200000"));
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.describeTopic(topic).partitions()).hasSize(6));
        assertThat(queries.describeTopic(topic).configs()).containsEntry("retention.ms", "7200000");

        commands.deleteTopic(topic);
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.listTopics()).noneMatch(t -> t.name().equals(topic)));
    }

    @Test
    void 중복_생성은_TopicExists_예외() {
        commands.createTopic("ops-t-dup", 1, (short) 1, null);
        assertThatThrownBy(() -> commands.createTopic("ops-t-dup", 1, (short) 1, null))
                .isInstanceOf(TopicExistsException.class);
    }

    @Test
    void 없는_토픽_수정은_UnknownTopic_예외() {
        assertThatThrownBy(() -> commands.updateTopic("ops-t-missing", null, Map.of("retention.ms", "1000")))
                .isInstanceOf(UnknownTopicOrPartitionException.class);
    }

    @Test
    void 파티션_감소는_거부된다() {
        commands.createTopic("ops-t-shrink", 3, (short) 1, null);
        assertThatThrownBy(() -> commands.updateTopic("ops-t-shrink", 2, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("감소");
    }

    @Test
    void 잘못된_토픽명은_거부된다() {
        assertThatThrownBy(() -> commands.createTopic("bad name!", 1, (short) 1, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 수정_요청에_변경_내용이_없으면_거부된다() {
        assertThatThrownBy(() -> commands.updateTopic("ops-t-any", null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
```

(awaitility가 클래스패스에 없으면 — `./gradlew dependencies --configuration testRuntimeClasspath | grep awaitility`로 확인 — `testImplementation 'org.awaitility:awaitility'`를 build.gradle에 추가한다. spring-boot-starter-test 에 보통 포함되어 있다.)

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests TopicCommandServiceIT`
Expected: 컴파일 실패 (TopicCommandService 없음)

- [ ] **Step 3: 구현**

`was/src/main/java/com/osstem/kafkaadmin/ops/OpsFutures.java`:

```java
package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import java.util.concurrent.ExecutionException;

// 조치용 await: 409/404/400 으로 구분해야 하는 예외는 그대로 통과시키고,
// 나머지(타임아웃·접속 실패 등)는 기존과 같이 KafkaUnavailableException(→503)으로 감싼다.
// TimeoutException 도 ApiException 하위라 "ApiException 전부 통과"로 하면 안 된다.
public final class OpsFutures {
    private OpsFutures() {}

    public static <T> T await(KafkaFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUnavailableException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TopicExistsException
                    || cause instanceof UnknownTopicOrPartitionException
                    || cause instanceof InvalidPartitionsException
                    || cause instanceof InvalidReplicationFactorException
                    || cause instanceof InvalidConfigurationException
                    || cause instanceof InvalidTopicException
                    || cause instanceof PolicyViolationException) {
                throw (RuntimeException) cause;
            }
            throw new KafkaUnavailableException(cause);
        }
    }
}
```

`was/src/main/java/com/osstem/kafkaadmin/ops/TopicCommandService.java`:

```java
package com.osstem.kafkaadmin.ops;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Service
public class TopicCommandService {

    // Kafka 토픽명 규칙: 영숫자 . _ - 만, 249자 이하
    private static final Pattern TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,249}");

    private final Admin admin;

    public TopicCommandService(Admin admin) {
        this.admin = admin;
    }

    public void createTopic(String name, int partitions, short replicationFactor,
                            Map<String, String> configs) {
        validateName(name);
        if (partitions < 1) throw new IllegalArgumentException("파티션 수는 1 이상이어야 합니다");
        if (replicationFactor < 1) throw new IllegalArgumentException("복제 팩터는 1 이상이어야 합니다");
        NewTopic topic = new NewTopic(name, partitions, replicationFactor);
        if (configs != null && !configs.isEmpty()) {
            topic.configs(configs);
        }
        OpsFutures.await(admin.createTopics(List.of(topic)).all());
    }

    public void updateTopic(String name, Integer partitions, Map<String, String> configs) {
        validateName(name);
        boolean hasConfigs = configs != null && !configs.isEmpty();
        if (partitions == null && !hasConfigs) {
            throw new IllegalArgumentException("변경할 파티션 수 또는 설정을 지정해야 합니다");
        }
        if (partitions != null) {
            int current = OpsFutures.await(
                    admin.describeTopics(List.of(name)).allTopicNames()).get(name)
                    .partitions().size();
            if (partitions <= current) {
                throw new IllegalArgumentException(
                        "파티션은 감소할 수 없습니다 (현재 " + current + ")");
            }
            OpsFutures.await(admin.createPartitions(
                    Map.of(name, NewPartitions.increaseTo(partitions))).all());
        }
        if (hasConfigs) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
            List<AlterConfigOp> ops = configs.entrySet().stream()
                    .map(e -> new AlterConfigOp(
                            new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                    .toList();
            OpsFutures.await(admin.incrementalAlterConfigs(Map.of(resource, ops)).all());
        }
    }

    public void deleteTopic(String name) {
        validateName(name);
        OpsFutures.await(admin.deleteTopics(List.of(name)).all());
    }

    private void validateName(String name) {
        if (name == null || !TOPIC_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "토픽명은 영문·숫자·'.', '_', '-' 만 사용해 249자 이하로 지정합니다");
        }
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `cd was && ./gradlew test --tests TopicCommandServiceIT`
Expected: 6 tests PASS

- [ ] **Step 5: Commit**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/ops was/src/test/java/com/osstem/kafkaadmin/ops
git commit -m "feat: add topic command service with ops error mapping"
```

---

### Task 3: OpsController + 권한 + 에러 매핑

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/api/OpsController.java`
- Modify: `was/src/main/java/com/osstem/kafkaadmin/config/SecurityConfig.java` (`/api/ops/**` ADMIN 규칙)
- Modify: `was/src/main/java/com/osstem/kafkaadmin/api/ApiExceptionHandler.java` (409/404/400 매핑)
- Test: `was/src/test/java/com/osstem/kafkaadmin/api/OpsControllerTest.java`

**Interfaces:**
- Consumes: Task 1 `AuditRecorder.record(...)`, `AuditLogRepository.findAllByOrderByExecutedAtDesc(Pageable)`; Task 2 `TopicCommandService` 3개 메서드
- Produces: REST API — `POST /api/ops/topics`(201), `PATCH /api/ops/topics/{name}`(204), `DELETE /api/ops/topics/{name}`(204), `GET /api/ops/audit-logs?page=0&size=50`(200, AuditLog 배열). Task 5·6 프론트가 호출.

- [ ] **Step 1: 실패하는 테스트 작성**

`was/src/test/java/com/osstem/kafkaadmin/api/OpsControllerTest.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.ops.AuditLogRepository;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.ops.TopicCommandService;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import com.osstem.kafkaadmin.config.SecurityConfig;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

// SecurityConfig 를 Import 해 /api/ops/** ADMIN 규칙까지 슬라이스에서 검증한다.
@WebMvcTest(OpsController.class)
@Import(SecurityConfig.class)
class OpsControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean TopicCommandService commands;
    @MockitoBean AuditRecorder recorder;
    @MockitoBean AuditLogRepository auditLogs;

    private static final String CREATE_BODY =
            "{\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}";

    @Test
    @WithMockUser(roles = "ADMIN")
    void 생성_성공은_201이고_감사_로그를_거친다() throws Exception {
        // AuditRecorder mock 이 operation 을 실제로 실행해야 서비스 호출을 검증할 수 있다
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        mvc.perform(post("/api/ops/topics").contentType("application/json").content(CREATE_BODY))
                .andExpect(status().isCreated());
        then(recorder).should().record(eq("user"), eq("TOPIC_CREATE"), eq("orders"), any(), any());
        then(commands).should().createTopic("orders", 3, (short) 1, null);
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void DEVELOPER는_403() throws Exception {
        mvc.perform(post("/api/ops/topics").contentType("application/json").content(CREATE_BODY))
                .andExpect(status().isForbidden());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 중복_토픽은_409() throws Exception {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        willThrow(new TopicExistsException("exists")).given(commands)
                .createTopic(any(), anyInt(), anyShort(), any());
        mvc.perform(post("/api/ops/topics").contentType("application/json").content(CREATE_BODY))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").exists());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 없는_토픽_삭제는_404() throws Exception {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        willThrow(new UnknownTopicOrPartitionException("missing")).given(commands)
                .deleteTopic("ghost");
        mvc.perform(delete("/api/ops/topics/ghost"))
                .andExpect(status().isNotFound());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 검증_실패는_400() throws Exception {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        willThrow(new IllegalArgumentException("파티션은 감소할 수 없습니다 (현재 3)"))
                .given(commands).updateTopic(eq("orders"), eq(2), any());
        mvc.perform(patch("/api/ops/topics/orders")
                        .contentType("application/json").content("{\"partitions\":2}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("파티션은 감소할 수 없습니다 (현재 3)"));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 감사_로그_조회() throws Exception {
        given(auditLogs.findAllByOrderByExecutedAtDesc(any())).willReturn(java.util.List.of());
        mvc.perform(get("/api/ops/audit-logs"))
                .andExpect(status().isOk());
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests OpsControllerTest`
Expected: 컴파일 실패 (OpsController 없음)

- [ ] **Step 3: 구현**

`was/src/main/java/com/osstem/kafkaadmin/api/OpsController.java`:

```java
package com.osstem.kafkaadmin.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.osstem.kafkaadmin.ops.AuditLog;
import com.osstem.kafkaadmin.ops.AuditLogRepository;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.ops.TopicCommandService;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

// 조치(쓰기) 전용 API. /api/ops/** 는 SecurityConfig 에서 ADMIN 으로 제한된다.
@RestController
@RequestMapping("/api/ops")
public class OpsController {

    public record CreateTopicRequest(String name, Integer partitions, Short replicationFactor,
                                     Map<String, String> configs) {}
    public record UpdateTopicRequest(Integer partitions, Map<String, String> configs) {}

    private final TopicCommandService commands;
    private final AuditRecorder recorder;
    private final AuditLogRepository auditLogs;
    private final ObjectMapper objectMapper;

    public OpsController(TopicCommandService commands, AuditRecorder recorder,
                         AuditLogRepository auditLogs, ObjectMapper objectMapper) {
        this.commands = commands;
        this.recorder = recorder;
        this.auditLogs = auditLogs;
        this.objectMapper = objectMapper;
    }

    // 201 에 빈 본문을 주면 기존 web/src/api/client.ts 가 res.json() 에서 실패한다
    // (204만 빈 응답 처리 — 이월 하드닝 #4). 그래서 생성 응답은 본문을 담는다.
    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, String> createTopic(@RequestBody CreateTopicRequest req,
                                           Authentication auth) {
        if (req.partitions() == null || req.replicationFactor() == null) {
            throw new IllegalArgumentException("partitions 와 replicationFactor 는 필수입니다");
        }
        recorder.record(auth.getName(), "TOPIC_CREATE", req.name(), toJson(req), () ->
                commands.createTopic(req.name(), req.partitions(), req.replicationFactor(),
                        req.configs()));
        return Map.of("name", req.name());
    }

    @PatchMapping("/topics/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateTopic(@PathVariable String name, @RequestBody UpdateTopicRequest req,
                            Authentication auth) {
        recorder.record(auth.getName(), "TOPIC_UPDATE", name, toJson(req), () ->
                commands.updateTopic(name, req.partitions(), req.configs()));
    }

    @DeleteMapping("/topics/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteTopic(@PathVariable String name, Authentication auth) {
        recorder.record(auth.getName(), "TOPIC_DELETE", name, "{}", () ->
                commands.deleteTopic(name));
    }

    @GetMapping("/audit-logs")
    public List<AuditLog> listAuditLogs(@RequestParam(defaultValue = "0") int page,
                                        @RequestParam(defaultValue = "50") int size) {
        return auditLogs.findAllByOrderByExecutedAtDesc(
                PageRequest.of(page, Math.min(size, 200)));
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }
}
```

`SecurityConfig.java` — `authorizeHttpRequests` 블록에서 `/api/**` 규칙 **위에** 한 줄 추가:

```java
                .requestMatchers("/api/auth/login").permitAll()
                .requestMatchers("/api/ops/**").hasRole("ADMIN")
                .requestMatchers("/api/**").authenticated()
```

`ApiExceptionHandler.java` — 기존 핸들러 아래에 추가 (import: `org.apache.kafka.common.errors.*`):

```java
    @ExceptionHandler(TopicExistsException.class)
    public ResponseEntity<Map<String, String>> topicExists(TopicExistsException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", "이미 존재하는 토픽입니다"));
    }

    @ExceptionHandler(UnknownTopicOrPartitionException.class)
    public ResponseEntity<Map<String, String>> unknownTopic(UnknownTopicOrPartitionException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "존재하지 않는 토픽입니다"));
    }

    // 서비스 검증 실패(파티션 감소 등)와 잘못된 클러스터 제약 요청을 400 으로
    @ExceptionHandler({IllegalArgumentException.class, InvalidPartitionsException.class,
            InvalidReplicationFactorException.class, InvalidConfigurationException.class,
            InvalidTopicException.class, PolicyViolationException.class})
    public ResponseEntity<Map<String, String>> badRequest(RuntimeException e) {
        return ResponseEntity.badRequest()
                .body(Map.of("error", e.getMessage() == null ? "잘못된 요청입니다" : e.getMessage()));
    }
```

- [ ] **Step 4: 통과 확인**

Run: `cd was && ./gradlew test --tests OpsControllerTest`
Expected: 6 tests PASS

- [ ] **Step 5: 전체 백엔드 테스트 회귀 확인**

Run: `cd was && ./gradlew test`
Expected: 기존 29건 + 신규 전부 PASS. (`@Import(SecurityConfig.class)`가 기존 QueryControllerTest 등과 충돌하지 않는지 확인 — 실패 시 원인 파악 후 수정, 기존 테스트를 고치지는 말 것)

- [ ] **Step 6: Commit**

```bash
git add was/src
git commit -m "feat: add ops API with admin-only access and error mapping"
```

---

### Task 4: useSession + ModalDialog 베이스

**Files:**
- Create: `web/src/composables/useSession.ts`
- Create: `web/src/components/ModalDialog.vue`
- Modify: `web/src/App.vue` (마운트 시 세션 로드 1회)
- Test: `web/src/components/__tests__/ModalDialog.spec.ts`, `web/src/composables/__tests__/useSession.spec.ts`

**Interfaces:**
- Consumes: 기존 `api<T>(path, init)` (`@/api/client`), `/api/auth/me` 응답 `{username, role}`
- Produces: `useSession()` → `{ session: Ref<Session|null>, isAdmin: ComputedRef<boolean>, load(): Promise<void> }` (모듈 스코프 싱글턴 상태); `<ModalDialog :title="...">` — `close` emit, 기본 슬롯(본문)과 `#footer` 슬롯. Task 5·6 이 사용.

- [ ] **Step 1: 실패하는 테스트 작성**

`web/src/composables/__tests__/useSession.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import { useSession } from '../useSession'

describe('useSession', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
  })

  it('ADMIN 세션이면 isAdmin 이 true', async () => {
    vi.mocked(api).mockResolvedValue({ username: 'a', role: 'ADMIN' })
    const { load, isAdmin } = useSession()
    await load()
    expect(isAdmin.value).toBe(true)
  })

  it('DEVELOPER 세션이면 isAdmin 이 false', async () => {
    vi.mocked(api).mockResolvedValue({ username: 'd', role: 'DEVELOPER' })
    const { load, isAdmin } = useSession()
    await load()
    expect(isAdmin.value).toBe(false)
  })

  it('조회 실패면 세션 없음으로 처리', async () => {
    vi.mocked(api).mockRejectedValue(new Error('401'))
    const { load, session, isAdmin } = useSession()
    await load()
    expect(session.value).toBeNull()
    expect(isAdmin.value).toBe(false)
  })
})
```

`web/src/components/__tests__/ModalDialog.spec.ts`:

```ts
import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import ModalDialog from '../ModalDialog.vue'

describe('ModalDialog', () => {
  it('제목과 본문 슬롯을 렌더링한다', () => {
    const wrapper = mount(ModalDialog, {
      props: { title: '토픽 생성' },
      slots: { default: '<p>본문</p>' },
    })
    expect(wrapper.find('[role="dialog"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('토픽 생성')
    expect(wrapper.text()).toContain('본문')
  })

  it('오버레이 클릭과 닫기 버튼이 close 를 emit 한다', async () => {
    const wrapper = mount(ModalDialog, { props: { title: 't' } })
    await wrapper.find('.overlay').trigger('click')
    await wrapper.find('.close-btn').trigger('click')
    expect(wrapper.emitted('close')).toHaveLength(2)
  })

  it('다이얼로그 내부 클릭은 close 를 emit 하지 않는다', async () => {
    const wrapper = mount(ModalDialog, { props: { title: 't' } })
    await wrapper.find('.dialog').trigger('click')
    expect(wrapper.emitted('close')).toBeUndefined()
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npm test`
Expected: 신규 spec 2개 실패 (모듈 없음)

- [ ] **Step 3: 구현**

`web/src/composables/useSession.ts`:

```ts
import { ref, computed } from 'vue'
import { api } from '@/api/client'

export interface Session {
  username: string
  role: 'ADMIN' | 'DEVELOPER'
}

// 모듈 스코프 싱글턴: 어느 컴포넌트에서 불러도 같은 세션 상태를 본다
const session = ref<Session | null>(null)

export function useSession() {
  async function load() {
    try {
      session.value = await api<Session>('/auth/me')
    } catch {
      session.value = null
    }
  }
  const isAdmin = computed(() => session.value?.role === 'ADMIN')
  return { session, isAdmin, load }
}
```

`web/src/components/ModalDialog.vue` (오버레이/다이얼로그 마크업·스타일은 기존 LagHelp 관례를 따른다):

```vue
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
```

`web/src/App.vue` — `<script setup>`에 세션 로드 추가 (기존 코드 유지, 아래만 삽입. `onMounted`가 이미 있으면 그 안에 `load()` 호출만 추가):

```ts
import { onMounted } from 'vue'
import { useSession } from '@/composables/useSession'

const { load } = useSession()
onMounted(load)
```

- [ ] **Step 4: 통과 확인**

Run: `cd web && npm test`
Expected: 신규 6 tests 포함 전부 PASS

- [ ] **Step 5: Commit**

```bash
git add web/src/composables web/src/components/ModalDialog.vue web/src/components/__tests__/ModalDialog.spec.ts web/src/App.vue
git commit -m "feat: add session composable and modal dialog base"
```

---

### Task 5: 토픽 생성 팝업 + 목록 화면 버튼

**Files:**
- Create: `web/src/components/TopicCreateModal.vue`
- Modify: `web/src/views/TopicsView.vue` (우측 상단 버튼 + 팝업 + 재조회)
- Test: `web/src/components/__tests__/TopicCreateModal.spec.ts`

**Interfaces:**
- Consumes: Task 4 `ModalDialog`, `useSession().isAdmin`; 백엔드 `POST /api/ops/topics` (본문 `{name, partitions, replicationFactor, configs?}`)
- Produces: `<TopicCreateModal @close @created>` — Task 6 과 동일한 팝업 패턴

- [ ] **Step 1: 실패하는 테스트 작성**

`web/src/components/__tests__/TopicCreateModal.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import TopicCreateModal from '../TopicCreateModal.vue'

describe('TopicCreateModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('입력값으로 영향 요약을 보여준다', async () => {
    const wrapper = mount(TopicCreateModal)
    await wrapper.find('input[name="name"]').setValue('orders')
    await wrapper.find('input[name="partitions"]').setValue('6')
    await wrapper.find('input[name="replicationFactor"]').setValue('3')
    expect(wrapper.text()).toContain('파티션 6, 복제 팩터 3으로 생성합니다')
  })

  it('제출하면 POST /ops/topics 를 호출하고 created 를 emit 한다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(TopicCreateModal)
    await wrapper.find('input[name="name"]').setValue('orders')
    await wrapper.find('input[name="partitions"]').setValue('6')
    await wrapper.find('input[name="replicationFactor"]').setValue('3')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/topics', expect.objectContaining({ method: 'POST' }))
    expect(wrapper.emitted('created')).toHaveLength(1)
  })

  it('retention 을 입력하면 configs 로 전송한다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(TopicCreateModal)
    await wrapper.find('input[name="name"]').setValue('orders')
    await wrapper.find('input[name="partitions"]').setValue('3')
    await wrapper.find('input[name="replicationFactor"]').setValue('1')
    await wrapper.find('input[name="retentionMs"]').setValue('86400000')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    const body = JSON.parse(vi.mocked(api).mock.calls[0][1]!.body as string)
    expect(body.configs).toEqual({ 'retention.ms': '86400000' })
  })

  it('서버 에러 메시지를 팝업 안에 표시한다', async () => {
    vi.mocked(api).mockRejectedValue(new Error('이미 존재하는 토픽입니다'))
    const wrapper = mount(TopicCreateModal)
    await wrapper.find('input[name="name"]').setValue('dup')
    await wrapper.find('input[name="partitions"]').setValue('1')
    await wrapper.find('input[name="replicationFactor"]').setValue('1')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    expect(wrapper.text()).toContain('이미 존재하는 토픽입니다')
    expect(wrapper.emitted('created')).toBeUndefined()
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npm test`
Expected: 신규 spec 실패 (컴포넌트 없음)

- [ ] **Step 3: 구현**

`web/src/components/TopicCreateModal.vue`:

```vue
<script setup lang="ts">
import { ref, computed } from 'vue'
import { api } from '@/api/client'
import ModalDialog from './ModalDialog.vue'

const emit = defineEmits<{ close: []; created: [] }>()

const name = ref('')
const partitions = ref('3')
const replicationFactor = ref('3')
const retentionMs = ref('')
const error = ref('')
const submitting = ref(false)

const summary = computed(() =>
  name.value
    ? `'${name.value}' 토픽을 파티션 ${partitions.value}, 복제 팩터 ${replicationFactor.value}으로 생성합니다`
    : '',
)

async function submit() {
  error.value = ''
  submitting.value = true
  try {
    await api('/ops/topics', {
      method: 'POST',
      body: JSON.stringify({
        name: name.value,
        partitions: Number(partitions.value),
        replicationFactor: Number(replicationFactor.value),
        configs: retentionMs.value ? { 'retention.ms': retentionMs.value } : undefined,
      }),
    })
    emit('created')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '생성 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog title="토픽 생성" @close="emit('close')">
    <form id="topic-create-form" @submit.prevent="submit">
      <label>토픽명 <input name="name" v-model="name" required /></label>
      <label>파티션 수 <input name="partitions" v-model="partitions" type="number" min="1" required /></label>
      <label>복제 팩터 <input name="replicationFactor" v-model="replicationFactor" type="number" min="1" required /></label>
      <label>retention.ms (선택) <input name="retentionMs" v-model="retentionMs" type="number" min="1" /></label>
      <p v-if="summary" class="summary">{{ summary }}</p>
      <p v-if="error" class="error">{{ error }}</p>
    </form>
    <template #footer>
      <button type="button" @click="emit('close')">취소</button>
      <button type="submit" form="topic-create-form" :disabled="submitting">생성</button>
    </template>
  </ModalDialog>
</template>

<style scoped>
form {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}
label {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  font-size: 0.85rem;
}
.summary {
  background: var(--surface-2);
  border-radius: 6px;
  padding: 0.5rem 0.75rem;
  margin: 0;
  font-size: 0.85rem;
}
.error {
  color: var(--crit);
  margin: 0;
}
</style>
```

`web/src/views/TopicsView.vue` — 전체를 아래로 교체 (조회 로직은 기존 유지, 헤더/버튼/팝업 추가):

```vue
<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import TopicCreateModal from '@/components/TopicCreateModal.vue'

interface TopicSummary { name: string; partitionCount: number; replicationFactor: number }

const topics = ref<TopicSummary[]>([])
const error = ref('')
const showCreate = ref(false)
const { isAdmin } = useSession()

async function loadTopics() {
  try {
    topics.value = await api<TopicSummary[]>('/topics')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(loadTopics)

function onCreated() {
  showCreate.value = false
  loadTopics()
}
</script>

<template>
  <main>
    <div class="head-row">
      <h1>토픽</h1>
      <button v-if="isAdmin" type="button" @click="showCreate = true">토픽 생성</button>
    </div>
    <p v-if="error" class="error">{{ error }}</p>
    <table v-else>
      <thead><tr><th>이름</th><th>파티션</th><th>복제 팩터</th></tr></thead>
      <tbody>
        <tr v-for="t in topics" :key="t.name">
          <td><RouterLink :to="`/topics/${t.name}`">{{ t.name }}</RouterLink></td>
          <td>{{ t.partitionCount }}</td>
          <td>{{ t.replicationFactor }}</td>
        </tr>
      </tbody>
    </table>
    <TopicCreateModal v-if="showCreate" @close="showCreate = false" @created="onCreated" />
  </main>
</template>

<style scoped>
.head-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
</style>
```

- [ ] **Step 4: 통과 확인**

Run: `cd web && npm test`
Expected: 전부 PASS

- [ ] **Step 5: Commit**

```bash
git add web/src/components/TopicCreateModal.vue web/src/components/__tests__/TopicCreateModal.spec.ts web/src/views/TopicsView.vue
git commit -m "feat: add topic create modal and list header button"
```

---

### Task 6: 수정·삭제 팝업 + 상세 화면 버튼

**Files:**
- Create: `web/src/components/TopicEditModal.vue`
- Create: `web/src/components/TopicDeleteModal.vue`
- Modify: `web/src/views/TopicDetailView.vue` (우측 상단 버튼 2개 + 팝업 + 재조회/이동)
- Test: `web/src/components/__tests__/TopicDeleteModal.spec.ts`, `web/src/components/__tests__/TopicEditModal.spec.ts`

**Interfaces:**
- Consumes: Task 4 `ModalDialog`, `useSession().isAdmin`; 백엔드 `PATCH /api/ops/topics/{name}`, `DELETE /api/ops/topics/{name}`
- Produces: `<TopicEditModal :name :currentPartitions :configs @close @updated>`, `<TopicDeleteModal :name @close @deleted>`

- [ ] **Step 1: 실패하는 테스트 작성**

`web/src/components/__tests__/TopicDeleteModal.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import TopicDeleteModal from '../TopicDeleteModal.vue'

describe('TopicDeleteModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('토픽명이 일치하기 전에는 삭제 버튼이 비활성', async () => {
    const wrapper = mount(TopicDeleteModal, { props: { name: 'orders' } })
    const btn = wrapper.find('button.danger')
    expect(btn.attributes('disabled')).toBeDefined()
    await wrapper.find('input').setValue('order')
    expect(wrapper.find('button.danger').attributes('disabled')).toBeDefined()
  })

  it('토픽명이 일치하면 활성화되고 DELETE 를 호출한다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(TopicDeleteModal, { props: { name: 'orders' } })
    await wrapper.find('input').setValue('orders')
    expect(wrapper.find('button.danger').attributes('disabled')).toBeUndefined()
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/topics/orders', { method: 'DELETE' })
    expect(wrapper.emitted('deleted')).toHaveLength(1)
  })

  it('실패하면 에러를 표시하고 deleted 를 emit 하지 않는다', async () => {
    vi.mocked(api).mockRejectedValue(new Error('존재하지 않는 토픽입니다'))
    const wrapper = mount(TopicDeleteModal, { props: { name: 'orders' } })
    await wrapper.find('input').setValue('orders')
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('존재하지 않는 토픽입니다')
    expect(wrapper.emitted('deleted')).toBeUndefined()
  })
})
```

`web/src/components/__tests__/TopicEditModal.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import TopicEditModal from '../TopicEditModal.vue'

const props = { name: 'orders', currentPartitions: 3, configs: { 'retention.ms': '86400000' } }

describe('TopicEditModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('감소 불가 경고와 현재 값을 보여준다', () => {
    const wrapper = mount(TopicEditModal, { props })
    expect(wrapper.text()).toContain('줄일 수 없습니다')
    expect((wrapper.find('input[name="partitions"]').element as HTMLInputElement).value).toBe('3')
  })

  it('변경한 필드만 PATCH 본문에 담는다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(TopicEditModal, { props })
    await wrapper.find('input[name="partitions"]').setValue('6')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    const body = JSON.parse(vi.mocked(api).mock.calls[0][1]!.body as string)
    expect(body.partitions).toBe(6)
    expect(body.configs).toBeUndefined()
    expect(wrapper.emitted('updated')).toHaveLength(1)
  })

  it('retention 변경은 configs 로 담는다', async () => {
    vi.mocked(api).mockResolvedValue(undefined)
    const wrapper = mount(TopicEditModal, { props })
    await wrapper.find('input[name="retentionMs"]').setValue('7200000')
    await wrapper.find('form').trigger('submit')
    await flushPromises()
    const body = JSON.parse(vi.mocked(api).mock.calls[0][1]!.body as string)
    expect(body.partitions).toBeUndefined()
    expect(body.configs).toEqual({ 'retention.ms': '7200000' })
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npm test`
Expected: 신규 spec 2개 실패

- [ ] **Step 3: 구현**

`web/src/components/TopicDeleteModal.vue`:

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
    await api(`/ops/topics/${props.name}`, { method: 'DELETE' })
    emit('deleted')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '삭제 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog title="토픽 삭제" @close="emit('close')">
    <p>
      <strong>'{{ name }}'</strong> 토픽과 모든 메시지가 삭제됩니다. 되돌릴 수 없습니다.
    </p>
    <label>
      계속하려면 토픽명을 입력하세요
      <input v-model="confirmText" :placeholder="name" />
    </label>
    <p v-if="error" class="error">{{ error }}</p>
    <template #footer>
      <button type="button" @click="emit('close')">취소</button>
      <button type="button" class="danger" :disabled="!canDelete || submitting" @click="remove">
        삭제
      </button>
    </template>
  </ModalDialog>
</template>

<style scoped>
label {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  font-size: 0.85rem;
}
.danger {
  background: var(--crit);
  color: white;
  border: none;
  border-radius: 6px;
  padding: 0.35rem 0.9rem;
}
.danger:disabled {
  opacity: 0.4;
}
.error {
  color: var(--crit);
  margin: 0.5rem 0 0;
}
</style>
```

`web/src/components/TopicEditModal.vue`:

```vue
<script setup lang="ts">
import { ref } from 'vue'
import { api } from '@/api/client'
import ModalDialog from './ModalDialog.vue'

const props = defineProps<{
  name: string
  currentPartitions: number
  configs: Record<string, string>
}>()
const emit = defineEmits<{ close: []; updated: [] }>()

const partitions = ref(String(props.currentPartitions))
const retentionMs = ref(props.configs['retention.ms'] ?? '')
const error = ref('')
const submitting = ref(false)

async function submit() {
  error.value = ''
  const body: { partitions?: number; configs?: Record<string, string> } = {}
  const nextPartitions = Number(partitions.value)
  if (nextPartitions !== props.currentPartitions) body.partitions = nextPartitions
  const currentRetention = props.configs['retention.ms'] ?? ''
  if (retentionMs.value !== currentRetention && retentionMs.value !== '') {
    body.configs = { 'retention.ms': retentionMs.value }
  }
  if (body.partitions === undefined && body.configs === undefined) {
    error.value = '변경된 내용이 없습니다'
    return
  }
  submitting.value = true
  try {
    await api(`/ops/topics/${props.name}`, { method: 'PATCH', body: JSON.stringify(body) })
    emit('updated')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '수정 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog :title="`토픽 설정 수정: ${name}`" @close="emit('close')">
    <form id="topic-edit-form" @submit.prevent="submit">
      <label>
        파티션 수 (현재 {{ currentPartitions }})
        <input name="partitions" v-model="partitions" type="number" :min="currentPartitions" />
      </label>
      <p class="note">파티션은 늘릴 수만 있고 줄일 수 없습니다.</p>
      <label>retention.ms <input name="retentionMs" v-model="retentionMs" type="number" min="1" /></label>
      <p v-if="error" class="error">{{ error }}</p>
    </form>
    <template #footer>
      <button type="button" @click="emit('close')">취소</button>
      <button type="submit" form="topic-edit-form" :disabled="submitting">적용</button>
    </template>
  </ModalDialog>
</template>

<style scoped>
form {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}
label {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  font-size: 0.85rem;
}
.note {
  margin: 0;
  padding: 0.5rem 0.75rem;
  background: var(--warn-soft);
  border-radius: 6px;
  color: var(--warn);
  font-size: 0.85rem;
}
.error {
  color: var(--crit);
  margin: 0;
}
</style>
```

`web/src/views/TopicDetailView.vue` — `<script setup>`에 추가:

```ts
import { useRouter } from 'vue-router'
import { useSession } from '@/composables/useSession'
import TopicEditModal from '@/components/TopicEditModal.vue'
import TopicDeleteModal from '@/components/TopicDeleteModal.vue'

const router = useRouter()
const { isAdmin } = useSession()
const showEdit = ref(false)
const showDelete = ref(false)

async function reload() {
  showEdit.value = false
  detail.value = await api<TopicDetail>(`/topics/${route.params.name}`)
}
function onDeleted() {
  router.push('/topics')
}
```

템플릿의 `<h1>` 줄을 헤더 행으로 교체하고, `</main>` 직전에 팝업을 추가:

```vue
    <div class="head-row">
      <h1>토픽: {{ route.params.name }}</h1>
      <div v-if="isAdmin && detail" class="actions">
        <button type="button" @click="showEdit = true">설정 수정</button>
        <button type="button" class="danger-outline" @click="showDelete = true">삭제</button>
      </div>
    </div>
```

```vue
    <TopicEditModal
      v-if="showEdit && detail"
      :name="detail.name"
      :current-partitions="detail.partitions.length"
      :configs="detail.configs"
      @close="showEdit = false"
      @updated="reload"
    />
    <TopicDeleteModal
      v-if="showDelete && detail"
      :name="detail.name"
      @close="showDelete = false"
      @deleted="onDeleted"
    />
```

`<style scoped>`에 추가:

```css
.head-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.actions {
  display: flex;
  gap: 0.5rem;
}
.danger-outline {
  border: 1px solid var(--crit);
  color: var(--crit);
  background: none;
  border-radius: 6px;
  padding: 0.35rem 0.9rem;
}
```

- [ ] **Step 4: 통과 확인**

Run: `cd web && npm test`
Expected: 전부 PASS. 타입 검사 스크립트가 있으면 `npm run type-check`도 통과 확인.

- [ ] **Step 5: Commit**

```bash
git add web/src/components web/src/views/TopicDetailView.vue
git commit -m "feat: add topic edit and delete modals on detail view"
```

---

### Task 7: 실브로커 수동 E2E + 문서 링크

**Files:**
- Modify: `README.md` (문서 목록에 스펙·계획 링크 추가)

**Interfaces:**
- Consumes: Task 1~6 전체. 로컬 단일 노드 SASL_SSL 클러스터(`devops-note/kafka/examples/compose-1node-kraft`, 기동 상태) 또는 PLAINTEXT 로컬 브로커.

- [ ] **Step 1: 백엔드·프론트 전체 테스트 재확인**

Run: `cd was && ./gradlew test && cd ../web && npm test`
Expected: 전부 PASS

- [ ] **Step 2: 실브로커로 수동 E2E**

로컬 1노드 SASL_SSL 클러스터가 떠 있다는 전제 (아니면 README의 PLAINTEXT 로컬 실행 절차 사용):

```bash
cd was
ADMIN_INITIAL_PASSWORD=devpw \
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 \
KAFKA_SECURITY_PROTOCOL=SASL_SSL \
KAFKA_SASL_JAAS='org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="<admin 비밀번호>";' \
KAFKA_TRUSTSTORE_LOCATION=<truststore.jks 절대경로> \
KAFKA_TRUSTSTORE_PASSWORD=<truststore 비밀번호> \
./gradlew bootRun
```

```bash
cd web && npm run dev
```

브라우저에서 확인 (각 항목 실패 시 원인 수정 후 재확인):

1. admin 로그인 → 토픽 목록 우측 상단에 `토픽 생성` 버튼 노출
2. 생성 팝업에서 `e2e-check` (파티션 3, RF 1) 생성 → 목록에 나타남
3. 상세 화면 → `설정 수정`으로 파티션 6, retention 변경 → 반영 확인
4. 파티션을 3으로 줄이려 하면 400 에러 메시지가 팝업에 표시
5. `삭제` 팝업 — 토픽명 오입력 시 버튼 비활성, 정확히 입력 후 삭제 → 목록으로 이동
6. `GET /api/ops/audit-logs` 호출(브라우저 devtools 또는 curl) → 위 조치들이 SUCCESS/FAILED로 기록됨
7. DEVELOPER 계정이 있으면 로그인 → 버튼 미노출 확인 (없으면 curl로 403만 확인)

- [ ] **Step 3: README 문서 링크 추가**

`README.md`의 "## 문서" 목록에 한 줄 추가:

```markdown
- [토픽 CUD 설계](docs/phase3-topic-crud-design.md) · [구현 계획](docs/plan-phase3-topic-crud.md) — 3단계 첫 슬라이스 (ops 모듈 골격)
```

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: link topic crud design and plan"
```
