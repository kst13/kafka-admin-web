# Schema Registry 화면 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 관리자 사이트에서 Confluent Schema Registry 의 스키마를 토픽 기준으로 조회하고, 로그인 사용자는 새 버전을 등록(호환성 검사 통과 시)하며, ADMIN 은 호환성 모드 변경과 서브젝트 soft delete 를 수행한다.

**Architecture:** 백엔드 `schema` 패키지가 Spring `RestClient` 로 Registry REST API 를 직접 호출한다(연결 실패 시 다음 URL 로 페일오버). 서브젝트 이름 규칙(`<토픽>-key|value`)은 순수 클래스 `SubjectName` 한 곳에 둔다. 조회는 `SchemaQueryService`, 변경은 `SchemaCommandService`(등록 전 호환성 검사, 감사 로그는 컨트롤러가 `AuditRecorder` 로). 조회·등록 API 는 `/api/schemas/**`(인증만), 위험 조치는 `/api/ops/schemas/**`(ADMIN). 프론트는 기존 `ModalDialog`, `useSession`, `api` 관례를 따르고, Registry 미설정이면 메뉴와 섹션을 숨긴다.

**Tech Stack:** Spring Boot 4.1 / Spring Framework 7 (`RestClient`, Jackson 3 `tools.jackson`) / Java 21 / Testcontainers 2.0.5 (`apache/kafka:4.0.0` + `confluentinc/cp-schema-registry:7.7.1`) / Vue 3.5 + TS / vitest 4

**Spec:** `docs/superpowers/specs/2026-09-08-schema-registry-design.md`

## Global Constraints

- 서브젝트 규칙 `^(?<topic>[a-zA-Z0-9._-]{1,249})-(?<kind>key|value)$`. 등록은 규칙에 맞는 서브젝트만. 규칙 밖은 kind `other`, topic null 로 조회만.
- Registry 요청/응답 `Content-Type: application/vnd.schemaregistry.v1+json`. `schemaType` 응답 필드가 없으면 `AVRO`.
- 페일오버: 연결 실패(`ResourceAccessException`)에만 다음 URL. HTTP 4xx/5xx 는 그대로 매핑. 모든 URL 실패 → `SchemaRegistryUnavailableException`(503 "Schema Registry 접속 불가"). 미설정(urls 빈 값) → `SchemaRegistryNotConfiguredException`(503 "Schema Registry 가 설정되지 않았습니다").
- 오류 매핑: 40401/40402 → `SubjectNotFoundException` 404 "존재하지 않는 서브젝트/버전입니다: <subject>"; 409 → `IncompatibleSchemaException(messages)` 409 `{"error":"호환성 검사에 실패했습니다","details":[…]}`; 42201/42202 → `InvalidSchemaException` 400 (Registry message 포함); 그 외 5xx → Unavailable 503. 40408(서브젝트 호환성 미설정)은 조회에서 전역 상속으로 처리. 호환성 검사 대상 서브젝트가 없어(40401) 검사가 불가능하면 "호환"으로 취급.
- 호환성 값 `BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE`. 형식 `AVRO, JSON, PROTOBUF`. 이상값은 `IllegalArgumentException`(400).
- 스키마 본문 최대 1 MB(`1_048_576` bytes UTF-8). 감사 params 에 본문을 넣지 않는다: `SCHEMA_REGISTER` params = `{"subject","schemaType","schemaBytes"}` (version/id 는 등록 후에야 알 수 있어 `AuditRecorder.record` 의 사전 params 에 넣을 수 없다 — 응답 본문에만 담는다; 설계 문서의 params 목록에서 의도적으로 뺀 항목).
- 감사 action: `SCHEMA_REGISTER`(actor = 로그인 사용자, DEVELOPER 포함) / `SCHEMA_SET_COMPATIBILITY` / `SCHEMA_SET_GLOBAL_COMPATIBILITY` / `SCHEMA_DELETE_SUBJECT`.
- 타임아웃: connect/read 모두 `app.schema-registry.timeout-ms`(기본 5000).
- `SecurityConfig` 변경 없음 (`/api/ops/**` ADMIN, `/api/**` authenticated).
- Boot 4 테스트 슬라이스 import: `org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest`, `org.springframework.test.context.bean.override.mockito.MockitoBean`. Testcontainers 는 싱글턴 패턴(`static { start(); }`), `@Container` 금지. H2 테스트 DB 이름은 고유하게.
- 실행 위치: git worktree(브랜치 `feature/schema-registry`, base `production`). 백엔드 테스트 `cd was && ./gradlew test --tests '<FQCN>'`(IT 는 Docker 필요, 전체 실행은 마지막에 1회). 프론트 `cd web && npx vitest run <path>`, `npm run type-check`.
- 커밋 메시지 끝:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01VqsEEP7LjxWJAg8cwH1o35
  ```

## File Structure

백엔드 (`was/src/main/java/com/osstem/kafkaadmin/`):

| 파일 | 책임 |
|---|---|
| `schema/SubjectName.java`, `schema/SubjectKind.java`, `schema/SchemaType.java`, `schema/CompatibilityLevel.java` | 이름 규칙·enum (순수) |
| `schema/dto/SchemaDtos.java` | API 응답 레코드 |
| `schema/SchemaRegistryProperties.java`, `schema/SchemaRegistryConfig.java` | 설정 + `RestClient.Builder` 빈(타임아웃) |
| `schema/SchemaRegistryClient.java` | REST 호출·페일오버·오류 매핑 |
| `schema/*Exception.java` (5개) | 매핑용 예외 |
| `schema/SchemaQueryService.java` | 조회 조합(요약·상세·버전·토픽별) |
| `schema/SchemaCommandService.java` | 검증·호환성 검사·등록·호환성 변경·삭제 |
| `api/SchemaController.java`, `api/SchemaOpsController.java` | HTTP |
| `api/ApiExceptionHandler.java` (수정) | 5개 핸들러 추가 |
| `deploy/.env.example`, `README.md` (수정) | 설정·문서 |

백엔드 테스트: `schema/SubjectNameTest`, `schema/SchemaRegistryClientTest`(MockRestServiceServer), `schema/SchemaCommandServiceTest`, `schema/SchemaQueryServiceTest`, `api/SchemaControllerTest`, `api/SchemaOpsControllerTest`, `schema/SchemaRegistryIntegrationTestBase`, `schema/SchemaRegistryIT`.

프론트 (`web/src/`):

| 파일 | 책임 |
|---|---|
| `lib/schemas.ts` | 타입·상수·`groupByTopic`·`formatSchema` |
| `composables/useSchemaRegistry.ts` | `/schemas/status` 1회 로드, `configured` |
| `components/SchemaRegisterModal.vue` | 등록(검사 → 등록) |
| `components/SchemaDeleteModal.vue` | 서브젝트 삭제 |
| `components/CompatibilityModal.vue` | 전역/서브젝트 호환성 변경 |
| `components/TopicSchemaSection.vue` | 토픽 상세의 스키마 섹션 |
| `views/SchemasView.vue`, `views/SchemaSubjectView.vue` | 목록·상세 |
| `router/index.ts`, `App.vue`, `views/TopicDetailView.vue` (수정) | 라우트·메뉴·섹션 삽입 |

---

### Task 1: 이름 규칙·enum·DTO (순수)

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/schema/SubjectKind.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/schema/SubjectName.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/schema/SchemaType.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/schema/CompatibilityLevel.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/schema/dto/SchemaDtos.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/schema/SubjectNameTest.java`

**Interfaces:**
- Produces: `enum SubjectKind { KEY, VALUE, OTHER; String value() /* "key"|"value"|"other" */; static SubjectKind parse(String) /* key|value 만 허용, 아니면 IllegalArgumentException */ }`
- Produces: `record SubjectName(String subject, String topic, SubjectKind kind) { static SubjectName parse(String subject); static String of(String topic, SubjectKind kind); boolean isTopicBound() }`
- Produces: `enum SchemaType { AVRO, JSON, PROTOBUF; static SchemaType parse(String) }`, `enum CompatibilityLevel { BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE; static CompatibilityLevel parse(String) }`
- Produces DTO (모두 `SchemaDtos` 안의 public record): `SchemaRegistryStatus(boolean configured, List<String> urls, String globalCompatibility)`, `SubjectSummary(String subject, String topic, String kind, int latestVersion, String schemaType, String compatibility, String compatibilitySource)`, `SchemaVersionSummary(int version, int id, String schemaType)`, `SubjectDetail(String subject, String topic, String kind, String compatibility, String compatibilitySource, List<SchemaVersionSummary> versions)`, `SchemaReference(String name, String subject, int version)`, `SchemaVersion(String subject, int version, int id, String schemaType, String schema, List<SchemaReference> references)`, `TopicSchemas(String topic, SubjectSummary key, SubjectSummary value)`, `CompatibilityResult(boolean compatible, List<String> messages)`, `RegisteredSchema(String subject, int id, int version)`. `compatibilitySource` 는 `"SUBJECT"` 또는 `"GLOBAL"`.

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.schema;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SubjectNameTest {

    @Test
    void 토픽_value_서브젝트를_파싱한다() {
        SubjectName n = SubjectName.parse("orders-value");
        assertThat(n.topic()).isEqualTo("orders");
        assertThat(n.kind()).isEqualTo(SubjectKind.VALUE);
        assertThat(n.isTopicBound()).isTrue();
        assertThat(SubjectName.parse("order.events_v2-key").topic()).isEqualTo("order.events_v2");
    }

    @Test
    void 규칙_밖_이름은_other이고_topic이_null() {
        SubjectName n = SubjectName.parse("com.example.Order");
        assertThat(n.kind()).isEqualTo(SubjectKind.OTHER);
        assertThat(n.topic()).isNull();
        assertThat(n.isTopicBound()).isFalse();
        assertThat(SubjectName.parse("orders-VALUE").kind()).isEqualTo(SubjectKind.OTHER);
        assertThat(SubjectName.parse("-value").kind()).isEqualTo(SubjectKind.OTHER);
    }

    @Test
    void 토픽과_종류로_서브젝트를_만든다() {
        assertThat(SubjectName.of("orders", SubjectKind.KEY)).isEqualTo("orders-key");
        assertThatThrownBy(() -> SubjectName.of("orders", SubjectKind.OTHER)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SubjectName.of("bad topic", SubjectKind.VALUE)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SubjectName.of("", SubjectKind.VALUE)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void enum_파싱은_대소문자를_무시하고_이상값을_거부한다() {
        assertThat(SubjectKind.parse("Key")).isEqualTo(SubjectKind.KEY);
        assertThatThrownBy(() -> SubjectKind.parse("other")).isInstanceOf(IllegalArgumentException.class);
        assertThat(SchemaType.parse("json")).isEqualTo(SchemaType.JSON);
        assertThatThrownBy(() -> SchemaType.parse("xml")).isInstanceOf(IllegalArgumentException.class);
        assertThat(CompatibilityLevel.parse("full_transitive")).isEqualTo(CompatibilityLevel.FULL_TRANSITIVE);
        assertThatThrownBy(() -> CompatibilityLevel.parse(null)).isInstanceOf(IllegalArgumentException.class);
        assertThat(SubjectKind.VALUE.value()).isEqualTo("value");
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.schema.SubjectNameTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 구현**

`SubjectKind.java`:

```java
package com.osstem.kafkaadmin.schema;

// 서브젝트 종류. 화면/API 값은 소문자. OTHER 는 TopicNameStrategy 규칙 밖 이름(조회만).
public enum SubjectKind {
    KEY, VALUE, OTHER;

    public String value() { return name().toLowerCase(); }

    // 등록 입력 파싱: key | value 만
    public static SubjectKind parse(String value) {
        if (value != null) {
            String v = value.trim().toUpperCase();
            if (v.equals("KEY")) return KEY;
            if (v.equals("VALUE")) return VALUE;
        }
        throw new IllegalArgumentException("kind 는 key 또는 value 여야 합니다");
    }
}
```

`SubjectName.java`:

```java
package com.osstem.kafkaadmin.schema;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TopicNameStrategy 서브젝트 규칙의 단일 출처: <토픽>-key | <토픽>-value
public record SubjectName(String subject, String topic, SubjectKind kind) {

    private static final Pattern TOPIC = Pattern.compile("[a-zA-Z0-9._-]{1,249}");
    private static final Pattern SUBJECT = Pattern.compile("^(?<topic>[a-zA-Z0-9._-]{1,249})-(?<kind>key|value)$");

    public static SubjectName parse(String subject) {
        Matcher m = SUBJECT.matcher(subject == null ? "" : subject);
        if (!m.matches()) return new SubjectName(subject, null, SubjectKind.OTHER);
        SubjectKind kind = m.group("kind").equals("key") ? SubjectKind.KEY : SubjectKind.VALUE;
        return new SubjectName(subject, m.group("topic"), kind);
    }

    public static String of(String topic, SubjectKind kind) {
        if (topic == null || !TOPIC.matcher(topic).matches()) {
            throw new IllegalArgumentException("토픽명은 영문·숫자·'.', '_', '-' 만 사용해 249자 이하로 지정합니다");
        }
        if (kind != SubjectKind.KEY && kind != SubjectKind.VALUE) {
            throw new IllegalArgumentException("kind 는 key 또는 value 여야 합니다");
        }
        return topic + "-" + kind.value();
    }

    public boolean isTopicBound() { return kind != SubjectKind.OTHER; }
}
```

`SchemaType.java`:

```java
package com.osstem.kafkaadmin.schema;

public enum SchemaType {
    AVRO, JSON, PROTOBUF;

    public static SchemaType parse(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("schemaType 은 AVRO, JSON, PROTOBUF 중 하나여야 합니다");
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("schemaType 은 AVRO, JSON, PROTOBUF 중 하나여야 합니다");
        }
    }
}
```

`CompatibilityLevel.java`:

```java
package com.osstem.kafkaadmin.schema;

public enum CompatibilityLevel {
    BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE;

    public static CompatibilityLevel parse(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("compatibility 값이 올바르지 않습니다");
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("compatibility 값이 올바르지 않습니다: " + value);
        }
    }
}
```

`dto/SchemaDtos.java`:

```java
package com.osstem.kafkaadmin.schema.dto;

import java.util.List;

// Schema Registry 화면 API 응답 레코드. 프론트 lib/schemas.ts 의 타입과 필드명이 같다.
public final class SchemaDtos {
    private SchemaDtos() {}

    public record SchemaRegistryStatus(boolean configured, List<String> urls, String globalCompatibility) {}
    public record SubjectSummary(String subject, String topic, String kind, int latestVersion, String schemaType,
                                 String compatibility, String compatibilitySource) {}
    public record SchemaVersionSummary(int version, int id, String schemaType) {}
    public record SubjectDetail(String subject, String topic, String kind, String compatibility,
                                String compatibilitySource, List<SchemaVersionSummary> versions) {}
    public record SchemaReference(String name, String subject, int version) {}
    public record SchemaVersion(String subject, int version, int id, String schemaType, String schema,
                                List<SchemaReference> references) {}
    public record TopicSchemas(String topic, SubjectSummary key, SubjectSummary value) {}
    public record CompatibilityResult(boolean compatible, List<String> messages) {}
    public record RegisteredSchema(String subject, int id, int version) {}
}
```

- [ ] **Step 4: 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.schema.SubjectNameTest'`
Expected: 4 tests passed

- [ ] **Step 5: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/schema was/src/test/java/com/osstem/kafkaadmin/schema/SubjectNameTest.java
git commit -m "feat(schema): 서브젝트 규칙·enum·DTO"
```

---
### Task 2: 설정·예외·`SchemaRegistryClient` (REST, 페일오버, 오류 매핑)

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/schema/SchemaRegistryProperties.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/schema/SchemaRegistryConfig.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/schema/SchemaRegistryNotConfiguredException.java`, `SchemaRegistryUnavailableException.java`, `SubjectNotFoundException.java`, `IncompatibleSchemaException.java`, `InvalidSchemaException.java`, `SubjectConfigNotSetException.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/schema/SchemaRegistryClient.java`
- Modify: `was/src/main/resources/application.yml` (`app.schema-registry` 추가)
- Test: `was/src/test/java/com/osstem/kafkaadmin/schema/SchemaRegistryClientTest.java`

**Interfaces:**
- Consumes: `SchemaType`, `CompatibilityLevel`, `SchemaDtos.SchemaVersion/SchemaReference/CompatibilityResult`
- Produces: `SchemaRegistryProperties(String urls, Integer timeoutMs)` + `List<String> urlList()`, `int timeout()`
- Produces: `SchemaRegistryClient` — `boolean configured()`, `List<String> urls()`, `List<String> subjects()`, `List<Integer> versions(String subject)`, `SchemaVersion version(String subject, String version)`, `int register(String subject, SchemaType type, String schema)`, `CompatibilityResult testCompatibility(String subject, SchemaType type, String schema)`, `CompatibilityLevel globalConfig()`, `Optional<CompatibilityLevel> subjectConfig(String subject)`, `void setGlobalConfig(CompatibilityLevel)`, `void setSubjectConfig(String subject, CompatibilityLevel)`, `void deleteSubjectConfig(String subject)`, `List<Integer> deleteSubject(String subject)`
- Produces exceptions: `SchemaRegistryNotConfiguredException()`, `SchemaRegistryUnavailableException(Throwable)`, `SubjectNotFoundException(String subject)` (message `존재하지 않는 서브젝트/버전입니다: <subject>`), `IncompatibleSchemaException(List<String> messages)` + `getMessages()`, `InvalidSchemaException(String message)`

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.schema.dto.SchemaDtos.CompatibilityResult;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SchemaVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import java.net.ConnectException;
import java.util.List;
import java.util.Optional;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

// RestClient 를 MockRestServiceServer 에 묶어 요청 형태·페일오버·오류 매핑을 검증한다 (실서버는 IT).
class SchemaRegistryClientTest {

    private static final MediaType SR = MediaType.valueOf("application/vnd.schemaregistry.v1+json");

    private MockRestServiceServer server;
    private SchemaRegistryClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        client = new SchemaRegistryClient(new SchemaRegistryProperties("http://a:8081,http://b:8081", 5000), builder);
    }

    @Test
    void 서브젝트_목록을_vnd_타입으로_요청한다() {
        server.expect(requestTo("http://a:8081/subjects")).andExpect(header("Accept", SR.toString()))
                .andRespond(withSuccess("[\"orders-value\",\"com.x.Y\"]", SR));
        assertThat(client.subjects()).containsExactly("orders-value", "com.x.Y");
        assertThat(client.configured()).isTrue();
        assertThat(client.urls()).containsExactly("http://a:8081", "http://b:8081");
        server.verify();
    }

    @Test
    void 연결_실패면_다음_URL로_넘어간다() {
        server.expect(requestTo("http://a:8081/subjects")).andRespond(withException(new ConnectException("refused")));
        server.expect(requestTo("http://b:8081/subjects")).andRespond(withSuccess("[\"orders-value\"]", SR));
        assertThat(client.subjects()).containsExactly("orders-value");
        server.verify();
    }

    @Test
    void 모든_URL이_실패하면_접속불가_예외() {
        server.expect(requestTo("http://a:8081/subjects")).andRespond(withException(new ConnectException("refused")));
        server.expect(requestTo("http://b:8081/subjects")).andRespond(withException(new ConnectException("refused")));
        assertThatThrownBy(() -> client.subjects()).isInstanceOf(SchemaRegistryUnavailableException.class);
    }

    @Test
    void HTTP_오류는_페일오버하지_않고_그대로_매핑한다() {
        server.expect(requestTo("http://a:8081/subjects/ghost-value/versions"))
                .andRespond(withStatus(HttpStatus.NOT_FOUND).contentType(SR)
                        .body("{\"error_code\":40401,\"message\":\"Subject 'ghost-value' not found.\"}"));
        assertThatThrownBy(() -> client.versions("ghost-value"))
                .isInstanceOf(SubjectNotFoundException.class).hasMessageContaining("ghost-value");
        server.verify(); // b 로 요청이 가지 않았다
    }

    @Test
    void 호환성_실패_409는_사유를_담아_던진다() {
        server.expect(requestTo("http://a:8081/subjects/orders-value/versions"))
                .andRespond(withStatus(HttpStatus.CONFLICT).contentType(SR)
                        .body("{\"error_code\":409,\"message\":\"Schema being registered is incompatible with an earlier schema\"}"));
        assertThatThrownBy(() -> client.register("orders-value", SchemaType.AVRO, "{}"))
                .isInstanceOf(IncompatibleSchemaException.class)
                .satisfies(e -> assertThat(((IncompatibleSchemaException) e).getMessages()).anyMatch(m -> m.contains("incompatible")));
    }

    @Test
    void 잘못된_스키마_42201은_400용_예외() {
        server.expect(requestTo("http://a:8081/subjects/orders-value/versions"))
                .andRespond(withStatus(HttpStatus.UNPROCESSABLE_ENTITY).contentType(SR)
                        .body("{\"error_code\":42201,\"message\":\"Invalid schema\"}"));
        assertThatThrownBy(() -> client.register("orders-value", SchemaType.AVRO, "not json"))
                .isInstanceOf(InvalidSchemaException.class).hasMessageContaining("Invalid schema");
    }

    @Test
    void 등록은_schema와_schemaType을_보내고_id를_돌려준다() {
        server.expect(requestTo("http://a:8081/subjects/orders-value/versions"))
                .andExpect(method(org.springframework.http.HttpMethod.POST))
                .andExpect(content().contentType(SR))
                .andExpect(content().json("{\"schema\":\"{\\\"type\\\":\\\"string\\\"}\",\"schemaType\":\"JSON\"}"))
                .andRespond(withSuccess("{\"id\":7}", SR));
        assertThat(client.register("orders-value", SchemaType.JSON, "{\"type\":\"string\"}")).isEqualTo(7);
    }

    @Test
    void 호환성_검사는_verbose로_요청하고_서브젝트가_없으면_호환으로_본다() {
        server.expect(requestTo("http://a:8081/compatibility/subjects/orders-value/versions/latest?verbose=true"))
                .andRespond(withSuccess("{\"is_compatible\":false,\"messages\":[\"READER_FIELD_MISSING_DEFAULT_VALUE\"]}", SR));
        CompatibilityResult r = client.testCompatibility("orders-value", SchemaType.AVRO, "{}");
        assertThat(r.compatible()).isFalse();
        assertThat(r.messages()).containsExactly("READER_FIELD_MISSING_DEFAULT_VALUE");

        server.expect(requestTo("http://a:8081/compatibility/subjects/new-value/versions/latest?verbose=true"))
                .andRespond(withStatus(HttpStatus.NOT_FOUND).contentType(SR).body("{\"error_code\":40401,\"message\":\"Subject 'new-value' not found.\"}"));
        assertThat(client.testCompatibility("new-value", SchemaType.AVRO, "{}").compatible()).isTrue();
    }

    @Test
    void 서브젝트_호환성은_미설정이면_empty_전역은_값() {
        server.expect(requestTo("http://a:8081/config/orders-value"))
                .andRespond(withStatus(HttpStatus.NOT_FOUND).contentType(SR).body("{\"error_code\":40408,\"message\":\"Subject 'orders-value' does not have subject-level compatibility configured\"}"));
        assertThat(client.subjectConfig("orders-value")).isEqualTo(Optional.empty());
        server.expect(requestTo("http://a:8081/config/orders-value"))
                .andRespond(withSuccess("{\"compatibilityLevel\":\"FULL\"}", SR));
        assertThat(client.subjectConfig("orders-value")).contains(CompatibilityLevel.FULL);
        server.expect(requestTo("http://a:8081/config")).andRespond(withSuccess("{\"compatibilityLevel\":\"BACKWARD\"}", SR));
        assertThat(client.globalConfig()).isEqualTo(CompatibilityLevel.BACKWARD);
    }

    @Test
    void 버전_상세는_schemaType_기본값_AVRO와_빈_references를_채운다() {
        server.expect(requestTo("http://a:8081/subjects/orders-value/versions/latest"))
                .andRespond(withSuccess("{\"subject\":\"orders-value\",\"version\":3,\"id\":12,\"schema\":\"{\\\"type\\\":\\\"record\\\"}\"}", SR));
        SchemaVersion v = client.version("orders-value", "latest");
        assertThat(v.version()).isEqualTo(3);
        assertThat(v.id()).isEqualTo(12);
        assertThat(v.schemaType()).isEqualTo("AVRO");
        assertThat(v.schema()).isEqualTo("{\"type\":\"record\"}");
        assertThat(v.references()).isEmpty();
    }

    @Test
    void 호환성_변경과_삭제() {
        server.expect(requestTo("http://a:8081/config")).andExpect(method(org.springframework.http.HttpMethod.PUT))
                .andExpect(content().json("{\"compatibility\":\"FULL\"}"))
                .andRespond(withSuccess("{\"compatibility\":\"FULL\"}", SR));
        client.setGlobalConfig(CompatibilityLevel.FULL);
        server.expect(requestTo("http://a:8081/config/orders-value")).andExpect(method(org.springframework.http.HttpMethod.PUT))
                .andExpect(content().json("{\"compatibility\":\"NONE\"}"))
                .andRespond(withSuccess("{\"compatibility\":\"NONE\"}", SR));
        client.setSubjectConfig("orders-value", CompatibilityLevel.NONE);
        server.expect(requestTo("http://a:8081/config/orders-value")).andExpect(method(org.springframework.http.HttpMethod.DELETE))
                .andRespond(withSuccess("\"NONE\"", SR));
        client.deleteSubjectConfig("orders-value");
        server.expect(requestTo("http://a:8081/subjects/orders-value")).andExpect(method(org.springframework.http.HttpMethod.DELETE))
                .andRespond(withSuccess("[1,2,3]", SR));
        assertThat(client.deleteSubject("orders-value")).containsExactly(1, 2, 3);
        server.verify();
    }

    @Test
    void 미설정이면_configured_false이고_호출은_미설정_예외() {
        SchemaRegistryClient none = new SchemaRegistryClient(new SchemaRegistryProperties("", 5000), RestClient.builder());
        assertThat(none.configured()).isFalse();
        assertThat(none.urls()).isEmpty();
        assertThatThrownBy(none::subjects).isInstanceOf(SchemaRegistryNotConfiguredException.class);
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.schema.SchemaRegistryClientTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 설정·예외 구현**

`SchemaRegistryProperties.java`:

```java
package com.osstem.kafkaadmin.schema;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.Arrays;
import java.util.List;

// app.schema-registry.urls 가 비어 있으면 기능 비활성. 타임아웃은 connect/read 공통(네트워크 코드 규약 5초).
@ConfigurationProperties(prefix = "app.schema-registry")
public record SchemaRegistryProperties(String urls, Integer timeoutMs) {

    public List<String> urlList() {
        if (urls == null || urls.isBlank()) return List.of();
        return Arrays.stream(urls.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .map(s -> s.endsWith("/") ? s.substring(0, s.length() - 1) : s).toList();
    }

    public int timeout() { return timeoutMs == null ? 5000 : timeoutMs; }
}
```

`SchemaRegistryConfig.java`:

```java
package com.osstem.kafkaadmin.schema;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import java.time.Duration;

@Configuration
@EnableConfigurationProperties(SchemaRegistryProperties.class)
public class SchemaRegistryConfig {

    // 전용 Builder 빈: Boot 가 제공하는 기본 RestClient.Builder 와 구분하기 위해 이름으로 주입한다
    @Bean("schemaRegistryRestClientBuilder")
    public RestClient.Builder schemaRegistryRestClientBuilder(SchemaRegistryProperties props) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(props.timeout()));
        factory.setReadTimeout(Duration.ofMillis(props.timeout()));
        return RestClient.builder().requestFactory(factory);
    }
}
```

`application.yml` 의 `app:` 아래(`admin-initial-password` 줄 뒤)에 추가:

```yaml
  schema-registry:
    urls: ${SCHEMA_REGISTRY_URLS:}
    timeout-ms: ${SCHEMA_REGISTRY_TIMEOUT_MS:5000}
```

예외 6개 (각각 별도 파일, 모두 `package com.osstem.kafkaadmin.schema;`):

```java
public class SchemaRegistryNotConfiguredException extends RuntimeException {
    public SchemaRegistryNotConfiguredException() { super("Schema Registry 가 설정되지 않았습니다"); }
}
```
```java
public class SchemaRegistryUnavailableException extends RuntimeException {
    public SchemaRegistryUnavailableException(Throwable cause) { super("Schema Registry 접속 불가", cause); }
}
```
```java
public class SubjectNotFoundException extends RuntimeException {
    public SubjectNotFoundException(String subject) { super("존재하지 않는 서브젝트/버전입니다: " + subject); }
}
```
```java
import java.util.List;
public class IncompatibleSchemaException extends RuntimeException {
    private final List<String> messages;
    public IncompatibleSchemaException(List<String> messages) {
        super("호환성 검사에 실패했습니다");
        this.messages = messages == null ? List.of() : List.copyOf(messages);
    }
    public List<String> getMessages() { return messages; }
}
```
```java
public class InvalidSchemaException extends RuntimeException {
    public InvalidSchemaException(String message) { super(message == null ? "스키마가 올바르지 않습니다" : message); }
}
```
```java
// GET /config/{subject} 가 40408 — 서브젝트 단위 호환성이 없어 전역을 상속한다는 뜻. 클라이언트 내부에서만 쓴다.
class SubjectConfigNotSetException extends RuntimeException {
    SubjectConfigNotSetException() { super("subject-level compatibility not set"); }
}
```

- [ ] **Step 4: 클라이언트 구현**

```java
package com.osstem.kafkaadmin.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.CompatibilityResult;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SchemaReference;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SchemaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

// Confluent Schema Registry REST 클라이언트. URL 목록을 순서대로 시도하되, 연결 실패에만 다음 URL 로 넘어간다.
// HTTP 오류 응답은 error_code 로 도메인 예외에 매핑한다 (설계 문서 "오류 매핑" 표).
@Component
public class SchemaRegistryClient {

    static final MediaType SR_JSON = MediaType.valueOf("application/vnd.schemaregistry.v1+json");
    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryClient.class);

    // Registry 응답 형태 (필요한 필드만)
    record VersionResponse(String subject, int version, int id, String schemaType, String schema,
                           List<SchemaReference> references) {}
    record IdResponse(int id) {}
    record CompatibilityResponse(@JsonProperty("is_compatible") boolean isCompatible, List<String> messages) {}
    record ConfigResponse(String compatibilityLevel) {}
    record ErrorResponse(@JsonProperty("error_code") int errorCode, String message) {}

    private final List<String> urls;
    private final List<RestClient> clients;

    public SchemaRegistryClient(SchemaRegistryProperties props,
                                @Qualifier("schemaRegistryRestClientBuilder") RestClient.Builder builder) {
        this.urls = props.urlList();
        this.clients = urls.stream().map(u -> builder.clone().baseUrl(u).build()).toList();
    }

    public boolean configured() { return !clients.isEmpty(); }
    public List<String> urls() { return urls; }

    public List<String> subjects() {
        return call(null, c -> c.get().uri("/subjects").accept(SR_JSON).retrieve()
                .body(new ParameterizedTypeReference<List<String>>() {}));
    }

    public List<Integer> versions(String subject) {
        return call(subject, c -> c.get().uri("/subjects/{s}/versions", subject).accept(SR_JSON).retrieve()
                .body(new ParameterizedTypeReference<List<Integer>>() {}));
    }

    public SchemaVersion version(String subject, String version) {
        VersionResponse r = call(subject, c -> c.get().uri("/subjects/{s}/versions/{v}", subject, version)
                .accept(SR_JSON).retrieve().body(VersionResponse.class));
        return new SchemaVersion(r.subject(), r.version(), r.id(),
                r.schemaType() == null ? "AVRO" : r.schemaType(), r.schema(),
                r.references() == null ? List.of() : r.references());
    }

    public int register(String subject, SchemaType type, String schema) {
        return call(subject, c -> c.post().uri("/subjects/{s}/versions", subject)
                .contentType(SR_JSON).accept(SR_JSON)
                .body(Map.of("schema", schema, "schemaType", type.name()))
                .retrieve().body(IdResponse.class)).id();
    }

    // 서브젝트가 아직 없으면(40401) 검사할 대상이 없으므로 "호환"으로 본다
    public CompatibilityResult testCompatibility(String subject, SchemaType type, String schema) {
        try {
            CompatibilityResponse r = call(subject, c -> c.post()
                    .uri("/compatibility/subjects/{s}/versions/latest?verbose=true", subject)
                    .contentType(SR_JSON).accept(SR_JSON)
                    .body(Map.of("schema", schema, "schemaType", type.name()))
                    .retrieve().body(CompatibilityResponse.class));
            return new CompatibilityResult(r.isCompatible(), r.messages() == null ? List.of() : r.messages());
        } catch (SubjectNotFoundException e) {
            return new CompatibilityResult(true, List.of());
        }
    }

    public CompatibilityLevel globalConfig() {
        return CompatibilityLevel.parse(call(null, c -> c.get().uri("/config").accept(SR_JSON)
                .retrieve().body(ConfigResponse.class)).compatibilityLevel());
    }

    public Optional<CompatibilityLevel> subjectConfig(String subject) {
        try {
            return Optional.of(CompatibilityLevel.parse(call(subject, c -> c.get().uri("/config/{s}", subject)
                    .accept(SR_JSON).retrieve().body(ConfigResponse.class)).compatibilityLevel()));
        } catch (SubjectConfigNotSetException | SubjectNotFoundException e) {
            return Optional.empty();
        }
    }

    public void setGlobalConfig(CompatibilityLevel level) {
        call(null, c -> c.put().uri("/config").contentType(SR_JSON).accept(SR_JSON)
                .body(Map.of("compatibility", level.name())).retrieve().toBodilessEntity());
    }

    public void setSubjectConfig(String subject, CompatibilityLevel level) {
        call(subject, c -> c.put().uri("/config/{s}", subject).contentType(SR_JSON).accept(SR_JSON)
                .body(Map.of("compatibility", level.name())).retrieve().toBodilessEntity());
    }

    public void deleteSubjectConfig(String subject) {
        call(subject, c -> c.delete().uri("/config/{s}", subject).accept(SR_JSON).retrieve().toBodilessEntity());
    }

    public List<Integer> deleteSubject(String subject) {
        return call(subject, c -> c.delete().uri("/subjects/{s}", subject).accept(SR_JSON).retrieve()
                .body(new ParameterizedTypeReference<List<Integer>>() {}));
    }

    private <T> T call(String subject, Function<RestClient, T> op) {
        if (clients.isEmpty()) throw new SchemaRegistryNotConfiguredException();
        ResourceAccessException last = null;
        for (int i = 0; i < clients.size(); i++) {
            try {
                return op.apply(clients.get(i));
            } catch (ResourceAccessException e) {
                last = e;
                log.warn("Schema Registry {} 연결 실패, 다음 URL 시도: {}", urls.get(i), e.getMessage());
            } catch (RestClientResponseException e) {
                throw map(subject, e);
            }
        }
        throw new SchemaRegistryUnavailableException(last);
    }

    private RuntimeException map(String subject, RestClientResponseException e) {
        ErrorResponse err = null;
        try {
            err = e.getResponseBodyAs(ErrorResponse.class);
        } catch (RuntimeException ignore) {
            // 본문이 JSON 이 아니면 상태 코드만으로 판단
        }
        int code = err == null ? 0 : err.errorCode();
        String message = err == null || err.message() == null ? e.getStatusText() : err.message();
        int status = e.getStatusCode().value();
        if (code == 40408) return new SubjectConfigNotSetException();
        if (code == 40401 || code == 40402 || code == 40403) return new SubjectNotFoundException(subject == null ? message : subject);
        if (status == 409) return new IncompatibleSchemaException(List.of(message));
        if (code == 42201 || code == 42202 || status == 422) return new InvalidSchemaException(message);
        return new SchemaRegistryUnavailableException(e);
    }
}
```

- [ ] **Step 5: 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.schema.SchemaRegistryClientTest'`
Expected: 12 tests passed. 참고: Jackson 3 는 알 수 없는 속성을 기본으로 무시하므로 `VersionResponse` 에 없는 필드가 와도 실패하지 않는다. `getResponseBodyAs` 가 Jackson 3 컨버터를 못 찾아 null 을 주면 `err == null` 경로로 상태 코드 매핑이 동작하지만 40401 테스트가 실패할 것이다 — 그 경우 `map` 에서 `e.getResponseBodyAsString()` 을 `tools.jackson.databind.ObjectMapper`(생성자로 `new ObjectMapper()`) 로 `readValue(body, ErrorResponse.class)` 해 파싱하도록 바꾼다.

- [ ] **Step 6: 앱 기동 확인 (미설정 상태)**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.ops.AuditRecorderTest'`
Expected: `@SpringBootTest` 컨텍스트가 `urls` 빈 값으로도 정상 기동(`SchemaRegistryClient` 빈이 비활성 상태로 생성됨).

- [ ] **Step 7: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/schema was/src/main/resources/application.yml was/src/test/java/com/osstem/kafkaadmin/schema/SchemaRegistryClientTest.java
git commit -m "feat(schema): Schema Registry REST 클라이언트 (페일오버·오류 매핑)"
```

---
### Task 3: `SchemaQueryService` · `SchemaCommandService`

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/schema/SchemaQueryService.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/schema/SchemaCommandService.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/schema/SchemaQueryServiceTest.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/schema/SchemaCommandServiceTest.java`

**Interfaces:**
- Consumes: `SchemaRegistryClient` (Task 2), `SubjectName`/enums/DTOs (Task 1), 기존 `kafka/TopicQueryService.listTopics()` (`List<Dtos.TopicSummary>` with `name()`)
- Produces `SchemaQueryService`: `SchemaRegistryStatus status()`, `List<SubjectSummary> listSubjects()`, `SubjectDetail describeSubject(String subject)`, `SchemaVersion getVersion(String subject, String version)`, `TopicSchemas topicSchemas(String topic)`
- Produces `SchemaCommandService`: `CompatibilityResult checkCompatibility(String topic, SubjectKind kind, SchemaType type, String schema)`, `RegisteredSchema register(String topic, SubjectKind kind, SchemaType type, String schema)`, `void setCompatibility(String subject, CompatibilityLevel levelOrNull)`, `void setGlobalCompatibility(CompatibilityLevel level)`, `List<Integer> deleteSubject(String subject)`; 상수 `MAX_SCHEMA_BYTES = 1_048_576`
- 없는 토픽에 등록하면 `org.apache.kafka.common.errors.UnknownTopicOrPartitionException` (기존 404 "존재하지 않는 토픽입니다" 매핑 재사용)

- [ ] **Step 1: 실패하는 테스트 작성**

`SchemaQueryServiceTest.java`:

```java
package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Optional;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SchemaQueryServiceTest {

    private final SchemaRegistryClient client = mock(SchemaRegistryClient.class);
    private final SchemaQueryService service = new SchemaQueryService(client);

    private static SchemaVersion v(String subject, int version, int id, String type) {
        return new SchemaVersion(subject, version, id, type, "{}", List.of());
    }

    @Test
    void 미설정이면_configured_false() {
        when(client.configured()).thenReturn(false);
        SchemaRegistryStatus s = service.status();
        assertThat(s.configured()).isFalse();
        assertThat(s.urls()).isEmpty();
        assertThat(s.globalCompatibility()).isNull();
        verify(client, never()).globalConfig();
    }

    @Test
    void 목록은_토픽_종류_최신버전_호환성_출처를_채운다() {
        when(client.configured()).thenReturn(true);
        when(client.globalConfig()).thenReturn(CompatibilityLevel.BACKWARD);
        when(client.subjects()).thenReturn(List.of("orders-value", "orders-key", "com.x.Y"));
        when(client.version("orders-value", "latest")).thenReturn(v("orders-value", 3, 12, "AVRO"));
        when(client.version("orders-key", "latest")).thenReturn(v("orders-key", 1, 4, "JSON"));
        when(client.version("com.x.Y", "latest")).thenReturn(v("com.x.Y", 2, 9, "PROTOBUF"));
        when(client.subjectConfig("orders-value")).thenReturn(Optional.of(CompatibilityLevel.FULL));
        when(client.subjectConfig("orders-key")).thenReturn(Optional.empty());
        when(client.subjectConfig("com.x.Y")).thenReturn(Optional.empty());

        List<SubjectSummary> list = service.listSubjects();
        assertThat(list).containsExactly(
                new SubjectSummary("orders-value", "orders", "value", 3, "AVRO", "FULL", "SUBJECT"),
                new SubjectSummary("orders-key", "orders", "key", 1, "JSON", "BACKWARD", "GLOBAL"),
                new SubjectSummary("com.x.Y", null, "other", 2, "PROTOBUF", "BACKWARD", "GLOBAL"));
        verify(client, times(1)).globalConfig();
    }

    @Test
    void 상세는_버전을_최신_우선으로_나열한다() {
        when(client.globalConfig()).thenReturn(CompatibilityLevel.BACKWARD);
        when(client.versions("orders-value")).thenReturn(List.of(1, 2, 3));
        when(client.version("orders-value", "1")).thenReturn(v("orders-value", 1, 4, "AVRO"));
        when(client.version("orders-value", "2")).thenReturn(v("orders-value", 2, 8, "AVRO"));
        when(client.version("orders-value", "3")).thenReturn(v("orders-value", 3, 12, "AVRO"));
        when(client.subjectConfig("orders-value")).thenReturn(Optional.empty());
        SubjectDetail d = service.describeSubject("orders-value");
        assertThat(d.topic()).isEqualTo("orders");
        assertThat(d.kind()).isEqualTo("value");
        assertThat(d.compatibility()).isEqualTo("BACKWARD");
        assertThat(d.compatibilitySource()).isEqualTo("GLOBAL");
        assertThat(d.versions()).extracting(SchemaVersionSummary::version).containsExactly(3, 2, 1);
        assertThat(d.versions().get(0).id()).isEqualTo(12);
    }

    @Test
    void 없는_서브젝트_상세는_클라이언트_예외를_그대로_올린다() {
        when(client.globalConfig()).thenReturn(CompatibilityLevel.BACKWARD);
        when(client.versions("ghost-value")).thenThrow(new SubjectNotFoundException("ghost-value"));
        assertThatThrownBy(() -> service.describeSubject("ghost-value")).isInstanceOf(SubjectNotFoundException.class);
    }

    @Test
    void 토픽별_조회는_없는_쪽을_null로_둔다() {
        when(client.globalConfig()).thenReturn(CompatibilityLevel.BACKWARD);
        when(client.version("orders-key", "latest")).thenThrow(new SubjectNotFoundException("orders-key"));
        when(client.version("orders-value", "latest")).thenReturn(v("orders-value", 2, 8, "AVRO"));
        when(client.subjectConfig("orders-value")).thenReturn(Optional.empty());
        TopicSchemas t = service.topicSchemas("orders");
        assertThat(t.topic()).isEqualTo("orders");
        assertThat(t.key()).isNull();
        assertThat(t.value().latestVersion()).isEqualTo(2);
        assertThatThrownBy(() -> service.topicSchemas("bad topic")).isInstanceOf(IllegalArgumentException.class);
    }
}
```

`SchemaCommandServiceTest.java`:

```java
package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.kafka.TopicQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicSummary;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SchemaCommandServiceTest {

    private final SchemaRegistryClient client = mock(SchemaRegistryClient.class);
    private final TopicQueryService topics = mock(TopicQueryService.class);
    private final SchemaCommandService service = new SchemaCommandService(client, topics);

    @BeforeEach
    void topicsExist() {
        when(topics.listTopics()).thenReturn(List.of(new TopicSummary("orders", 3, 3)));
    }

    @Test
    void 등록은_호환성_검사_후_등록하고_최신_버전을_돌려준다() {
        when(client.testCompatibility("orders-value", SchemaType.AVRO, "{}")).thenReturn(new CompatibilityResult(true, List.of()));
        when(client.register("orders-value", SchemaType.AVRO, "{}")).thenReturn(12);
        when(client.version("orders-value", "latest")).thenReturn(new SchemaVersion("orders-value", 3, 12, "AVRO", "{}", List.of()));
        RegisteredSchema r = service.register("orders", SubjectKind.VALUE, SchemaType.AVRO, "{}");
        assertThat(r).isEqualTo(new RegisteredSchema("orders-value", 12, 3));
        var order = inOrder(client);
        order.verify(client).testCompatibility("orders-value", SchemaType.AVRO, "{}");
        order.verify(client).register("orders-value", SchemaType.AVRO, "{}");
    }

    @Test
    void 호환성_실패면_등록하지_않고_409용_예외() {
        when(client.testCompatibility(any(), any(), any())).thenReturn(new CompatibilityResult(false, List.of("READER_FIELD_MISSING_DEFAULT_VALUE")));
        assertThatThrownBy(() -> service.register("orders", SubjectKind.VALUE, SchemaType.AVRO, "{}"))
                .isInstanceOf(IncompatibleSchemaException.class)
                .satisfies(e -> assertThat(((IncompatibleSchemaException) e).getMessages()).containsExactly("READER_FIELD_MISSING_DEFAULT_VALUE"));
        verify(client, never()).register(any(), any(), any());
    }

    @Test
    void 없는_토픽이면_Registry를_호출하지_않고_UnknownTopic() {
        assertThatThrownBy(() -> service.register("ghost", SubjectKind.VALUE, SchemaType.AVRO, "{}"))
                .isInstanceOf(UnknownTopicOrPartitionException.class);
        verifyNoInteractions(client);
    }

    @Test
    void 본문_검증_빈값과_1MB_초과는_400용_예외() {
        assertThatThrownBy(() -> service.register("orders", SubjectKind.VALUE, SchemaType.AVRO, " "))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("본문");
        String big = "x".repeat(SchemaCommandService.MAX_SCHEMA_BYTES + 1);
        assertThatThrownBy(() -> service.checkCompatibility("orders", SubjectKind.VALUE, SchemaType.JSON, big))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1 MB");
        verifyNoInteractions(client);
    }

    @Test
    void 호환성_검사는_토픽_존재를_요구하지_않는다() {
        when(client.testCompatibility("new-value", SchemaType.JSON, "{}")).thenReturn(new CompatibilityResult(true, List.of()));
        assertThat(service.checkCompatibility("new", SubjectKind.VALUE, SchemaType.JSON, "{}").compatible()).isTrue();
        verify(topics, never()).listTopics();
    }

    @Test
    void 서브젝트_호환성은_존재_확인_후_설정하거나_해제한다() {
        when(client.versions("orders-value")).thenReturn(List.of(1));
        service.setCompatibility("orders-value", CompatibilityLevel.NONE);
        verify(client).setSubjectConfig("orders-value", CompatibilityLevel.NONE);
        service.setCompatibility("orders-value", null);
        verify(client).deleteSubjectConfig("orders-value");

        when(client.versions("ghost-value")).thenThrow(new SubjectNotFoundException("ghost-value"));
        assertThatThrownBy(() -> service.setCompatibility("ghost-value", CompatibilityLevel.FULL))
                .isInstanceOf(SubjectNotFoundException.class);
        verify(client, never()).setSubjectConfig(eq("ghost-value"), any());
    }

    @Test
    void 전역_호환성_변경과_삭제는_클라이언트에_위임한다() {
        service.setGlobalCompatibility(CompatibilityLevel.FULL_TRANSITIVE);
        verify(client).setGlobalConfig(CompatibilityLevel.FULL_TRANSITIVE);
        when(client.deleteSubject("orders-value")).thenReturn(List.of(1, 2));
        assertThat(service.deleteSubject("orders-value")).containsExactly(1, 2);
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.schema.SchemaQueryServiceTest' --tests 'com.osstem.kafkaadmin.schema.SchemaCommandServiceTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 구현**

`SchemaQueryService.java`:

```java
package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

// Schema Registry 조회 조합. 호환성은 서브젝트 지정(SUBJECT)이 있으면 그것, 없으면 전역(GLOBAL)을 상속한다.
@Service
public class SchemaQueryService {

    private final SchemaRegistryClient client;

    public SchemaQueryService(SchemaRegistryClient client) {
        this.client = client;
    }

    public SchemaRegistryStatus status() {
        if (!client.configured()) return new SchemaRegistryStatus(false, List.of(), null);
        return new SchemaRegistryStatus(true, client.urls(), client.globalConfig().name());
    }

    public List<SubjectSummary> listSubjects() {
        CompatibilityLevel global = client.globalConfig();
        List<SubjectSummary> out = new ArrayList<>();
        for (String subject : client.subjects()) {
            out.add(summary(subject, global));
        }
        return out;
    }

    public SubjectDetail describeSubject(String subject) {
        CompatibilityLevel global = client.globalConfig();
        List<Integer> versions = client.versions(subject); // 없으면 SubjectNotFoundException
        List<SchemaVersionSummary> summaries = versions.stream()
                .map(v -> client.version(subject, String.valueOf(v)))
                .map(v -> new SchemaVersionSummary(v.version(), v.id(), v.schemaType()))
                .sorted(Comparator.comparingInt(SchemaVersionSummary::version).reversed())
                .toList();
        SubjectName name = SubjectName.parse(subject);
        Optional<CompatibilityLevel> cfg = client.subjectConfig(subject);
        return new SubjectDetail(subject, name.topic(), name.kind().value(),
                cfg.orElse(global).name(), cfg.isPresent() ? "SUBJECT" : "GLOBAL", summaries);
    }

    public SchemaVersion getVersion(String subject, String version) {
        return client.version(subject, version);
    }

    public TopicSchemas topicSchemas(String topic) {
        String keySubject = SubjectName.of(topic, SubjectKind.KEY);     // 토픽명 검증 포함
        String valueSubject = SubjectName.of(topic, SubjectKind.VALUE);
        CompatibilityLevel global = client.globalConfig();
        return new TopicSchemas(topic, summaryOrNull(keySubject, global), summaryOrNull(valueSubject, global));
    }

    private SubjectSummary summaryOrNull(String subject, CompatibilityLevel global) {
        try {
            return summary(subject, global);
        } catch (SubjectNotFoundException e) {
            return null;
        }
    }

    private SubjectSummary summary(String subject, CompatibilityLevel global) {
        SubjectName name = SubjectName.parse(subject);
        SchemaVersion latest = client.version(subject, "latest");
        Optional<CompatibilityLevel> cfg = client.subjectConfig(subject);
        return new SubjectSummary(subject, name.topic(), name.kind().value(), latest.version(), latest.schemaType(),
                cfg.orElse(global).name(), cfg.isPresent() ? "SUBJECT" : "GLOBAL");
    }
}
```

`SchemaCommandService.java`:

```java
package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.kafka.TopicQueryService;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.CompatibilityResult;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.RegisteredSchema;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SchemaVersion;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Service;
import java.nio.charset.StandardCharsets;
import java.util.List;

// Schema Registry 변경. 등록은 호환성 검사를 통과해야 하고, 감사 로그는 컨트롤러가 AuditRecorder 로 감싼다.
@Service
public class SchemaCommandService {

    public static final int MAX_SCHEMA_BYTES = 1_048_576;

    private final SchemaRegistryClient client;
    private final TopicQueryService topics;

    public SchemaCommandService(SchemaRegistryClient client, TopicQueryService topics) {
        this.client = client;
        this.topics = topics;
    }

    public CompatibilityResult checkCompatibility(String topic, SubjectKind kind, SchemaType type, String schema) {
        validateSchema(schema);
        return client.testCompatibility(SubjectName.of(topic, kind), type, schema);
    }

    public RegisteredSchema register(String topic, SubjectKind kind, SchemaType type, String schema) {
        validateSchema(schema);
        String subject = SubjectName.of(topic, kind);
        requireTopic(topic);
        CompatibilityResult check = client.testCompatibility(subject, type, schema);
        if (!check.compatible()) throw new IncompatibleSchemaException(check.messages());
        int id = client.register(subject, type, schema);
        SchemaVersion latest = client.version(subject, "latest");
        return new RegisteredSchema(subject, id, latest.version());
    }

    // level 이 null 이면 서브젝트 설정을 지워 전역을 상속한다. 없는 서브젝트에 고아 설정이 남지 않도록 존재를 먼저 확인.
    public void setCompatibility(String subject, CompatibilityLevel level) {
        client.versions(subject);
        if (level == null) client.deleteSubjectConfig(subject);
        else client.setSubjectConfig(subject, level);
    }

    public void setGlobalCompatibility(CompatibilityLevel level) {
        client.setGlobalConfig(level);
    }

    public List<Integer> deleteSubject(String subject) {
        return client.deleteSubject(subject);
    }

    // listTopics 는 없는 토픽을 조용히 건너뛰므로 여기서 직접 404 용 예외로 바꾼다 (describeTopic 은 접속불가로 감싸 503 이 된다)
    private void requireTopic(String topic) {
        boolean exists = topics.listTopics().stream().anyMatch(t -> t.name().equals(topic));
        if (!exists) throw new UnknownTopicOrPartitionException("topic " + topic + " not found");
    }

    private static void validateSchema(String schema) {
        if (schema == null || schema.isBlank()) throw new IllegalArgumentException("스키마 본문을 입력하세요");
        if (schema.getBytes(StandardCharsets.UTF_8).length > MAX_SCHEMA_BYTES) {
            throw new IllegalArgumentException("스키마 본문은 1 MB 이하여야 합니다");
        }
    }
}
```

- [ ] **Step 4: 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.schema.SchemaQueryServiceTest' --tests 'com.osstem.kafkaadmin.schema.SchemaCommandServiceTest'`
Expected: 12 tests passed

- [ ] **Step 5: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/schema/SchemaQueryService.java was/src/main/java/com/osstem/kafkaadmin/schema/SchemaCommandService.java was/src/test/java/com/osstem/kafkaadmin/schema/SchemaQueryServiceTest.java was/src/test/java/com/osstem/kafkaadmin/schema/SchemaCommandServiceTest.java
git commit -m "feat(schema): 조회·변경 서비스"
```

---

### Task 4: 컨트롤러·예외 매핑·설정 문서

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/api/SchemaController.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/api/SchemaOpsController.java`
- Modify: `was/src/main/java/com/osstem/kafkaadmin/api/ApiExceptionHandler.java`
- Modify: `deploy/.env.example`, `README.md`
- Test: `was/src/test/java/com/osstem/kafkaadmin/api/SchemaControllerTest.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/api/SchemaOpsControllerTest.java`

**Interfaces:**
- Consumes: `SchemaQueryService`, `SchemaCommandService`, `AuditRecorder.record(actor, action, target, paramsJson, runnable)`
- Produces HTTP (프론트 Task 6–9 가 의존):
  - `GET /api/schemas/status` → `SchemaRegistryStatus` (미설정이어도 200)
  - `GET /api/schemas/subjects` → `SubjectSummary[]`; `GET /api/schemas/subjects/{subject}` → `SubjectDetail`; `GET /api/schemas/subjects/{subject}/versions/{version}` → `SchemaVersion`; `GET /api/schemas/topics/{topic}` → `TopicSchemas`
  - `POST /api/schemas/compatibility` `{topic, kind, schemaType, schema}` → 200 `CompatibilityResult`
  - `POST /api/schemas/register` `{topic, kind, schemaType, schema}` → 201 `RegisteredSchema`
  - `PUT /api/ops/schemas/config` `{compatibility}` → 200 `SchemaRegistryStatus`
  - `PUT /api/ops/schemas/subjects/{subject}/config` `{compatibility|null}` → 200 `SubjectDetail`
  - `DELETE /api/ops/schemas/subjects/{subject}` → 200 `{subject, deletedVersions[]}`
- 에러: NotConfigured/Unavailable 503, SubjectNotFound 404, Incompatible 409 `{error, details[]}`, InvalidSchema 400, IllegalArgument 400(기존), UnknownTopicOrPartition 404(기존)

- [ ] **Step 1: 실패하는 슬라이스 테스트 작성**

`SchemaControllerTest.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.config.SecurityConfig;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.schema.*;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
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

@WebMvcTest(SchemaController.class)
@Import(SecurityConfig.class)
class SchemaControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean SchemaQueryService queries;
    @MockitoBean SchemaCommandService commands;
    @MockitoBean AuditRecorder recorder;

    private static final String BODY =
            "{\"topic\":\"orders\",\"kind\":\"value\",\"schemaType\":\"AVRO\",\"schema\":\"{\\\"type\\\":\\\"string\\\"}\"}";

    @BeforeEach
    void recorderRuns() {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 상태와_목록은_DEVELOPER도_본다() throws Exception {
        given(queries.status()).willReturn(new SchemaRegistryStatus(false, List.of(), null));
        mvc.perform(get("/api/schemas/status")).andExpect(status().isOk()).andExpect(jsonPath("$.configured").value(false));
        given(queries.listSubjects()).willReturn(List.of(new SubjectSummary("orders-value", "orders", "value", 2, "AVRO", "BACKWARD", "GLOBAL")));
        mvc.perform(get("/api/schemas/subjects")).andExpect(status().isOk()).andExpect(jsonPath("$[0].kind").value("value"));
        given(queries.topicSchemas("orders")).willReturn(new TopicSchemas("orders", null, null));
        mvc.perform(get("/api/schemas/topics/orders")).andExpect(status().isOk()).andExpect(jsonPath("$.key").doesNotExist());
        given(queries.getVersion("orders-value", "2")).willReturn(new SchemaVersion("orders-value", 2, 9, "AVRO", "{}", List.of()));
        mvc.perform(get("/api/schemas/subjects/orders-value/versions/2")).andExpect(status().isOk()).andExpect(jsonPath("$.id").value(9));
    }

    @Test
    void 비로그인은_401() throws Exception {
        mvc.perform(get("/api/schemas/subjects")).andExpect(status().isUnauthorized());
    }

    @Test
    @WithMockUser(username = "dev1", roles = "DEVELOPER")
    void 등록은_DEVELOPER도_201이고_감사_params에_본문이_없다() throws Exception {
        given(commands.register("orders", SubjectKind.VALUE, SchemaType.AVRO, "{\"type\":\"string\"}"))
                .willReturn(new RegisteredSchema("orders-value", 9, 2));
        mvc.perform(post("/api/schemas/register").contentType("application/json").content(BODY))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.subject").value("orders-value"))
                .andExpect(jsonPath("$.version").value(2));
        then(recorder).should().record(eq("dev1"), eq("SCHEMA_REGISTER"), eq("orders-value"),
                argThat(p -> p.contains("\"schemaType\":\"AVRO\"") && p.contains("\"schemaBytes\":17") && !p.contains("string")), any());
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 호환성_검사는_기록하지_않고_결과를_돌려준다() throws Exception {
        given(commands.checkCompatibility("orders", SubjectKind.VALUE, SchemaType.AVRO, "{\"type\":\"string\"}"))
                .willReturn(new CompatibilityResult(false, List.of("READER_FIELD_MISSING_DEFAULT_VALUE")));
        mvc.perform(post("/api/schemas/compatibility").contentType("application/json").content(BODY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.compatible").value(false))
                .andExpect(jsonPath("$.messages[0]").value("READER_FIELD_MISSING_DEFAULT_VALUE"));
        then(recorder).shouldHaveNoInteractions();
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 호환성_실패_등록은_409에_사유를_담는다() throws Exception {
        given(commands.register(any(), any(), any(), any()))
                .willThrow(new IncompatibleSchemaException(List.of("READER_FIELD_MISSING_DEFAULT_VALUE")));
        mvc.perform(post("/api/schemas/register").contentType("application/json").content(BODY))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("호환성 검사에 실패했습니다"))
                .andExpect(jsonPath("$.details[0]").value("READER_FIELD_MISSING_DEFAULT_VALUE"));
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 잘못된_kind는_400이고_기록하지_않는다() throws Exception {
        mvc.perform(post("/api/schemas/register").contentType("application/json")
                        .content("{\"topic\":\"orders\",\"kind\":\"header\",\"schemaType\":\"AVRO\",\"schema\":\"{}\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("kind 는 key 또는 value 여야 합니다"));
        then(recorder).shouldHaveNoInteractions();
        then(commands).shouldHaveNoInteractions();
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 없는_서브젝트_404_잘못된_스키마_400_미설정_503() throws Exception {
        given(queries.describeSubject("ghost-value")).willThrow(new SubjectNotFoundException("ghost-value"));
        mvc.perform(get("/api/schemas/subjects/ghost-value")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("존재하지 않는 서브젝트/버전입니다: ghost-value"));
        given(commands.register(any(), any(), any(), any())).willThrow(new InvalidSchemaException("Invalid schema"));
        mvc.perform(post("/api/schemas/register").contentType("application/json").content(BODY))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("Invalid schema"));
        given(queries.listSubjects()).willThrow(new SchemaRegistryNotConfiguredException());
        mvc.perform(get("/api/schemas/subjects")).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("Schema Registry 가 설정되지 않았습니다"));
        given(queries.listSubjects()).willThrow(new SchemaRegistryUnavailableException(new RuntimeException("x")));
        mvc.perform(get("/api/schemas/subjects")).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("Schema Registry 접속 불가"));
    }
}
```

`SchemaOpsControllerTest.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.config.SecurityConfig;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.schema.CompatibilityLevel;
import com.osstem.kafkaadmin.schema.SchemaCommandService;
import com.osstem.kafkaadmin.schema.SchemaQueryService;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
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

@WebMvcTest(SchemaOpsController.class)
@Import(SecurityConfig.class)
class SchemaOpsControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean SchemaQueryService queries;
    @MockitoBean SchemaCommandService commands;
    @MockitoBean AuditRecorder recorder;

    @BeforeEach
    void recorderRuns() {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        given(queries.status()).willReturn(new SchemaRegistryStatus(true, List.of("http://sr"), "FULL"));
        given(queries.describeSubject("orders-value")).willReturn(
                new SubjectDetail("orders-value", "orders", "value", "NONE", "SUBJECT", List.of()));
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void DEVELOPER의_호환성_변경과_삭제는_403() throws Exception {
        mvc.perform(put("/api/ops/schemas/config").contentType("application/json").content("{\"compatibility\":\"FULL\"}"))
                .andExpect(status().isForbidden());
        mvc.perform(delete("/api/ops/schemas/subjects/orders-value")).andExpect(status().isForbidden());
        then(commands).shouldHaveNoInteractions();
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 전역_호환성_변경은_기록하고_상태를_돌려준다() throws Exception {
        mvc.perform(put("/api/ops/schemas/config").contentType("application/json").content("{\"compatibility\":\"full\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.globalCompatibility").value("FULL"));
        then(commands).should().setGlobalCompatibility(CompatibilityLevel.FULL);
        then(recorder).should().record(eq("user"), eq("SCHEMA_SET_GLOBAL_COMPATIBILITY"), eq("_global"),
                eq("{\"compatibility\":\"FULL\"}"), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 서브젝트_호환성은_값_또는_null로_전역_상속() throws Exception {
        mvc.perform(put("/api/ops/schemas/subjects/orders-value/config").contentType("application/json")
                        .content("{\"compatibility\":\"NONE\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.compatibility").value("NONE"));
        then(commands).should().setCompatibility("orders-value", CompatibilityLevel.NONE);
        mvc.perform(put("/api/ops/schemas/subjects/orders-value/config").contentType("application/json")
                        .content("{\"compatibility\":null}"))
                .andExpect(status().isOk());
        then(commands).should().setCompatibility("orders-value", null);
        then(recorder).should().record(eq("user"), eq("SCHEMA_SET_COMPATIBILITY"), eq("orders-value"),
                eq("{\"compatibility\":null}"), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 이상한_호환성_값은_400() throws Exception {
        mvc.perform(put("/api/ops/schemas/config").contentType("application/json").content("{\"compatibility\":\"SIDEWAYS\"}"))
                .andExpect(status().isBadRequest());
        then(commands).shouldHaveNoInteractions();
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 삭제는_삭제된_버전을_돌려준다() throws Exception {
        given(commands.deleteSubject("orders-value")).willReturn(List.of(1, 2));
        mvc.perform(delete("/api/ops/schemas/subjects/orders-value"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subject").value("orders-value"))
                .andExpect(jsonPath("$.deletedVersions[1]").value(2));
        then(recorder).should().record(eq("user"), eq("SCHEMA_DELETE_SUBJECT"), eq("orders-value"),
                eq("{\"subject\":\"orders-value\"}"), any());
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.api.SchemaControllerTest' --tests 'com.osstem.kafkaadmin.api.SchemaOpsControllerTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 컨트롤러 구현**

`SchemaController.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.schema.SchemaCommandService;
import com.osstem.kafkaadmin.schema.SchemaQueryService;
import com.osstem.kafkaadmin.schema.SchemaType;
import com.osstem.kafkaadmin.schema.SubjectKind;
import com.osstem.kafkaadmin.schema.SubjectName;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

// 스키마 조회·등록. /api/** 인증만 요구 — DEVELOPER 도 등록할 수 있다(호환성 검사가 소비자를 보호).
@RestController
@RequestMapping("/api/schemas")
public class SchemaController {

    public record SchemaRequest(String topic, String kind, String schemaType, String schema) {}

    private final SchemaQueryService queries;
    private final SchemaCommandService commands;
    private final AuditRecorder recorder;

    public SchemaController(SchemaQueryService queries, SchemaCommandService commands, AuditRecorder recorder) {
        this.queries = queries;
        this.commands = commands;
        this.recorder = recorder;
    }

    @GetMapping("/status")
    public SchemaRegistryStatus status() { return queries.status(); }

    @GetMapping("/subjects")
    public List<SubjectSummary> subjects() { return queries.listSubjects(); }

    @GetMapping("/subjects/{subject}")
    public SubjectDetail subject(@PathVariable String subject) { return queries.describeSubject(subject); }

    @GetMapping("/subjects/{subject}/versions/{version}")
    public SchemaVersion version(@PathVariable String subject, @PathVariable String version) {
        return queries.getVersion(subject, version);
    }

    @GetMapping("/topics/{topic}")
    public TopicSchemas topicSchemas(@PathVariable String topic) { return queries.topicSchemas(topic); }

    @PostMapping("/compatibility")
    public CompatibilityResult compatibility(@RequestBody SchemaRequest req) {
        return commands.checkCompatibility(req.topic(), SubjectKind.parse(req.kind()),
                SchemaType.parse(req.schemaType()), req.schema());
    }

    // 파싱(400)은 record 이전에 — 잘못된 입력이 감사 로그에 남지 않도록. params 에 본문은 넣지 않는다(크기).
    @PostMapping("/register")
    @ResponseStatus(HttpStatus.CREATED)
    public RegisteredSchema register(@RequestBody SchemaRequest req, Authentication auth) {
        SubjectKind kind = SubjectKind.parse(req.kind());
        SchemaType type = SchemaType.parse(req.schemaType());
        String subject = SubjectName.of(req.topic(), kind);
        int bytes = req.schema() == null ? 0 : req.schema().getBytes(StandardCharsets.UTF_8).length;
        String params = "{\"subject\":\"%s\",\"schemaType\":\"%s\",\"schemaBytes\":%d}".formatted(subject, type.name(), bytes);
        RegisteredSchema[] holder = new RegisteredSchema[1];
        recorder.record(auth.getName(), "SCHEMA_REGISTER", subject, params,
                () -> holder[0] = commands.register(req.topic(), kind, type, req.schema()));
        return holder[0];
    }
}
```

`SchemaOpsController.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.schema.CompatibilityLevel;
import com.osstem.kafkaadmin.schema.SchemaCommandService;
import com.osstem.kafkaadmin.schema.SchemaQueryService;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SchemaRegistryStatus;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SubjectDetail;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

// 스키마 위험 조치(ADMIN 전용). /api/ops/** 는 SecurityConfig 에서 ADMIN 으로 제한된다.
@RestController
@RequestMapping("/api/ops/schemas")
public class SchemaOpsController {

    public record CompatibilityRequest(String compatibility) {}

    private final SchemaQueryService queries;
    private final SchemaCommandService commands;
    private final AuditRecorder recorder;

    public SchemaOpsController(SchemaQueryService queries, SchemaCommandService commands, AuditRecorder recorder) {
        this.queries = queries;
        this.commands = commands;
        this.recorder = recorder;
    }

    @PutMapping("/config")
    public SchemaRegistryStatus setGlobal(@RequestBody CompatibilityRequest req, Authentication auth) {
        CompatibilityLevel level = CompatibilityLevel.parse(req.compatibility());
        recorder.record(auth.getName(), "SCHEMA_SET_GLOBAL_COMPATIBILITY", "_global",
                "{\"compatibility\":\"%s\"}".formatted(level.name()),
                () -> commands.setGlobalCompatibility(level));
        return queries.status();
    }

    // compatibility 가 null/빈 값이면 서브젝트 설정을 지워 전역을 상속한다
    @PutMapping("/subjects/{subject}/config")
    public SubjectDetail setSubject(@PathVariable String subject, @RequestBody CompatibilityRequest req,
                                    Authentication auth) {
        boolean inherit = req.compatibility() == null || req.compatibility().isBlank();
        CompatibilityLevel level = inherit ? null : CompatibilityLevel.parse(req.compatibility());
        recorder.record(auth.getName(), "SCHEMA_SET_COMPATIBILITY", subject,
                "{\"compatibility\":%s}".formatted(level == null ? "null" : "\"" + level.name() + "\""),
                () -> commands.setCompatibility(subject, level));
        return queries.describeSubject(subject);
    }

    @DeleteMapping("/subjects/{subject}")
    public Map<String, Object> deleteSubject(@PathVariable String subject, Authentication auth) {
        List<Integer>[] holder = new List[1];
        recorder.record(auth.getName(), "SCHEMA_DELETE_SUBJECT", subject,
                "{\"subject\":\"%s\"}".formatted(subject),
                () -> holder[0] = commands.deleteSubject(subject));
        return Map.of("subject", subject, "deletedVersions", holder[0]);
    }
}
```

- [ ] **Step 4: 예외 매핑 추가**

`ApiExceptionHandler.java` import 추가:

```java
import com.osstem.kafkaadmin.schema.IncompatibleSchemaException;
import com.osstem.kafkaadmin.schema.InvalidSchemaException;
import com.osstem.kafkaadmin.schema.SchemaRegistryNotConfiguredException;
import com.osstem.kafkaadmin.schema.SchemaRegistryUnavailableException;
import com.osstem.kafkaadmin.schema.SubjectNotFoundException;
```

클래스 끝(마지막 `}` 앞)에 핸들러 추가:

```java
    // --- Schema Registry ---
    @ExceptionHandler(SchemaRegistryNotConfiguredException.class)
    public ResponseEntity<Map<String, String>> schemaRegistryNotConfigured(SchemaRegistryNotConfiguredException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(SchemaRegistryUnavailableException.class)
    public ResponseEntity<Map<String, String>> schemaRegistryUnavailable(SchemaRegistryUnavailableException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(SubjectNotFoundException.class)
    public ResponseEntity<Map<String, String>> subjectNotFound(SubjectNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(IncompatibleSchemaException.class)
    public ResponseEntity<Map<String, Object>> incompatibleSchema(IncompatibleSchemaException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", e.getMessage(), "details", e.getMessages()));
    }

    @ExceptionHandler(InvalidSchemaException.class)
    public ResponseEntity<Map<String, String>> invalidSchema(InvalidSchemaException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
```

- [ ] **Step 5: 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.api.SchemaControllerTest' --tests 'com.osstem.kafkaadmin.api.SchemaOpsControllerTest'`
Expected: 12 tests passed. 참고: `@WebMvcTest(SchemaController.class)` 는 컨텍스트에 `SchemaRegistryClient` 빈을 만들지 않는다(서비스가 mock 이므로 불필요). `PathVariable` 의 `.` 은 Spring 6+ 에서 접미어 패턴이 없으므로 그대로 들어온다.

- [ ] **Step 6: 비-IT 백엔드 전체**

Run: `cd was && ./gradlew test --tests '*Test'`
Expected: 전부 통과

- [ ] **Step 7: 설정·문서**

`deploy/.env.example` 의 `ADMIN_INITIAL_PASSWORD` 블록 뒤에 추가:

```
# --- Schema Registry (선택): 비워두면 스키마 메뉴가 숨겨진다. 노드를 쉼표로 나열하면 연결 실패 시 다음 노드로 넘어간다 ---
# SCHEMA_REGISTRY_URLS=http://10.0.0.11:8081,http://10.0.0.12:8081
# SCHEMA_REGISTRY_TIMEOUT_MS=5000
```

`README.md` 문서 목록의 Kafka 앱 계정 관리 설계 줄 뒤에 추가:

```markdown
- [Schema Registry 화면 설계](docs/superpowers/specs/2026-09-08-schema-registry-design.md) — 토픽 기준 스키마 조회·등록(호환성 검사)·호환성 변경·삭제. 설정: `SCHEMA_REGISTRY_URLS`
```

`README.md` "로컬 개발" 절의 `application-local.yml` 문단 끝에 한 문장 추가: `Schema Registry 를 붙이려면 같은 파일에 \`app.schema-registry.urls: http://10.10.10.17:8081,http://10.10.10.18:8081\` 을 적는다(비우면 스키마 메뉴가 숨겨진다).`

- [ ] **Step 8: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/api/SchemaController.java was/src/main/java/com/osstem/kafkaadmin/api/SchemaOpsController.java was/src/main/java/com/osstem/kafkaadmin/api/ApiExceptionHandler.java was/src/test/java/com/osstem/kafkaadmin/api/SchemaControllerTest.java was/src/test/java/com/osstem/kafkaadmin/api/SchemaOpsControllerTest.java deploy/.env.example README.md
git commit -m "feat(schema): 조회·등록·조치 API, 예외 매핑, 설정 문서"
```

---
### Task 5: Testcontainers (Kafka + Schema Registry) 통합 테스트

**Files:**
- Create: `was/src/test/java/com/osstem/kafkaadmin/schema/SchemaRegistryIntegrationTestBase.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/schema/SchemaRegistryIT.java`

**Interfaces:**
- Consumes: `SchemaQueryService`, `SchemaCommandService`, `SchemaRegistryClient`, `SchemaRegistryProperties`, Spring `Admin` 빈
- Produces: `SchemaRegistryIntegrationTestBase` — `KAFKA`(PLAINTEXT, 컨테이너 네트워크 별칭 `kafka:19092`), `REGISTRY`(`confluentinc/cp-schema-registry:7.7.1`, 8081), `static String registryUrl()`. `app.schema-registry.urls` 는 **죽은 주소를 앞에 두고** 실제 주소를 뒤에 둔다(`http://127.0.0.1:1,<registryUrl>`) — 모든 IT 호출이 페일오버 경로를 지난다.
- 사전 준비: `docker pull confluentinc/cp-schema-registry:7.7.1` (약 1 GB, 첫 실행 전 미리 받아둔다). Docker Desktop 실행 필요.

- [ ] **Step 1: 베이스 작성**

```java
package com.osstem.kafkaadmin.schema;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;

// Kafka(PLAINTEXT) + Confluent Schema Registry 를 같은 Docker 네트워크에 띄운다. 기존 KafkaIntegrationTestBase 는
// 네트워크를 지정하지 않으므로 손대지 않고 별도 컨테이너·별도 Spring 컨텍스트를 쓴다. 싱글턴 패턴(@Container 금지).
// Registry 의 Kafka 주소는 컨테이너 내부 별칭(kafka:19092) — withListener 가 리스너와 네트워크 별칭을 함께 만든다.
@SpringBootTest
public abstract class SchemaRegistryIntegrationTestBase {

    static final Network NET = Network.newNetwork();

    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"))
                    .withNetwork(NET)
                    .withListener("kafka:19092");

    protected static final GenericContainer<?> REGISTRY =
            new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.7.1"))
                    .withNetwork(NET)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .waitingFor(Wait.forHttp("/subjects").forPort(8081).forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(3)));

    static {
        KAFKA.start();
        REGISTRY.start();
    }

    protected static String registryUrl() {
        return "http://" + REGISTRY.getHost() + ":" + REGISTRY.getMappedPort(8081);
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("app.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.kafka.security-protocol", () -> "PLAINTEXT");
        // 첫 URL 은 항상 연결 거부 → 모든 호출이 페일오버로 두 번째(실제) 주소에 닿는다
        registry.add("app.schema-registry.urls", () -> "http://127.0.0.1:1," + registryUrl());
        registry.add("app.schema-registry.timeout-ms", () -> "5000");
        registry.add("spring.datasource.url", () -> "jdbc:h2:mem:schema-registry-it;DB_CLOSE_DELAY=-1");
    }
}
```

- [ ] **Step 2: IT 작성**

```java
package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestClient;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class SchemaRegistryIT extends SchemaRegistryIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired SchemaQueryService queries;
    @Autowired SchemaCommandService commands;

    private static final String V1 = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
    private static final String V2_COMPATIBLE = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"qty\",\"type\":\"int\",\"default\":0}]}";
    // BACKWARD 위반: 기본값 없는 필드 추가 (새 스키마로 옛 데이터를 읽을 수 없다)
    private static final String V3_INCOMPATIBLE = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"qty\",\"type\":\"int\",\"default\":0},{\"name\":\"price\",\"type\":\"double\"}]}";

    @Test
    void 등록_조회_호환성_삭제_왕복() throws Exception {
        admin.createTopics(List.of(new NewTopic("sr-t-orders", 1, (short) 1))).all().get();
        String subject = "sr-t-orders-value";

        assertThat(commands.checkCompatibility("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V1).compatible()).isTrue();
        RegisteredSchema r1 = commands.register("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V1);
        assertThat(r1.subject()).isEqualTo(subject);
        assertThat(r1.version()).isEqualTo(1);

        assertThat(queries.status().configured()).isTrue();
        assertThat(queries.listSubjects()).anySatisfy(s -> {
            assertThat(s.subject()).isEqualTo(subject);
            assertThat(s.topic()).isEqualTo("sr-t-orders");
            assertThat(s.kind()).isEqualTo("value");
            assertThat(s.latestVersion()).isEqualTo(1);
            assertThat(s.schemaType()).isEqualTo("AVRO");
            assertThat(s.compatibilitySource()).isEqualTo("GLOBAL");
        });
        assertThat(queries.getVersion(subject, "latest").schema()).contains("\"name\":\"Order\"");
        TopicSchemas ts = queries.topicSchemas("sr-t-orders");
        assertThat(ts.key()).isNull();
        assertThat(ts.value().latestVersion()).isEqualTo(1);

        RegisteredSchema r2 = commands.register("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V2_COMPATIBLE);
        assertThat(r2.version()).isEqualTo(2);

        CompatibilityResult bad = commands.checkCompatibility("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V3_INCOMPATIBLE);
        assertThat(bad.compatible()).isFalse();
        assertThat(bad.messages()).isNotEmpty();
        assertThatThrownBy(() -> commands.register("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V3_INCOMPATIBLE))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertThat(queries.describeSubject(subject).versions()).extracting(SchemaVersionSummary::version).containsExactly(2, 1);

        commands.setCompatibility(subject, CompatibilityLevel.NONE);
        SubjectDetail d = queries.describeSubject(subject);
        assertThat(d.compatibility()).isEqualTo("NONE");
        assertThat(d.compatibilitySource()).isEqualTo("SUBJECT");
        assertThat(commands.register("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V3_INCOMPATIBLE).version()).isEqualTo(3);

        commands.setCompatibility(subject, null);
        assertThat(queries.describeSubject(subject).compatibilitySource()).isEqualTo("GLOBAL");

        commands.setGlobalCompatibility(CompatibilityLevel.FULL);
        assertThat(queries.status().globalCompatibility()).isEqualTo("FULL");
        commands.setGlobalCompatibility(CompatibilityLevel.BACKWARD);

        assertThatThrownBy(() -> commands.register("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, "not a schema"))
                .isInstanceOf(InvalidSchemaException.class);

        assertThat(commands.deleteSubject(subject)).containsExactly(1, 2, 3);
        assertThatThrownBy(() -> queries.describeSubject(subject)).isInstanceOf(SubjectNotFoundException.class);
        assertThat(queries.topicSchemas("sr-t-orders").value()).isNull();
    }

    @Test
    void 없는_토픽_등록은_404용_예외이고_Registry는_호출하지_않는다() {
        assertThatThrownBy(() -> commands.register("sr-t-ghost", SubjectKind.VALUE, SchemaType.AVRO, V1))
                .isInstanceOf(org.apache.kafka.common.errors.UnknownTopicOrPartitionException.class);
        assertThat(queries.listSubjects()).noneMatch(s -> s.subject().startsWith("sr-t-ghost"));
    }

    @Test
    void 모든_URL이_죽어_있으면_접속불가() {
        SchemaRegistryClient dead = new SchemaRegistryClient(
                new SchemaRegistryProperties("http://127.0.0.1:1,http://127.0.0.1:2", 2000), RestClient.builder());
        assertThatThrownBy(dead::subjects).isInstanceOf(SchemaRegistryUnavailableException.class);
    }
}
```

- [ ] **Step 3: 실행**

Run: `docker pull confluentinc/cp-schema-registry:7.7.1` (최초 1회), then `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.schema.SchemaRegistryIT'`
Expected: 3 tests passed. Registry 기동에 30–60초 걸린다.

컨테이너가 뜨지 않으면 테스트에 임시로 `System.out.println(REGISTRY.getLogs())` 를 넣어 확인한다. 흔한 원인: (1) `kafka:19092` 로 못 붙음 — `KAFKA.getEnvMap().get("KAFKA_LISTENERS")` 를 출력해 `kafka:19092` 리스너가 있는지, `docker network inspect` 로 두 컨테이너가 같은 네트워크인지 확인; (2) Registry 가 `_schemas` 토픽 생성에 실패 — 브로커 기동이 늦은 것이므로 `REGISTRY` 에 `.dependsOn(KAFKA)` 를 추가한다. 임시 로그는 커밋 전에 제거한다.

- [ ] **Step 4: 커밋**

```bash
git add was/src/test/java/com/osstem/kafkaadmin/schema/SchemaRegistryIntegrationTestBase.java was/src/test/java/com/osstem/kafkaadmin/schema/SchemaRegistryIT.java
git commit -m "test(schema): Kafka+Schema Registry Testcontainers 왕복 IT"
```

---
### Task 6: 프론트 타입·순수 함수·`useSchemaRegistry`

**Files:**
- Create: `web/src/lib/schemas.ts`
- Create: `web/src/composables/useSchemaRegistry.ts`
- Test: `web/src/lib/__tests__/schemas.spec.ts`
- Test: `web/src/composables/__tests__/useSchemaRegistry.spec.ts`

**Interfaces:**
- Produces (`@/lib/schemas`): types `SubjectKind`, `SchemaType`, `CompatibilityLevel`, `SchemaRegistryStatus`, `SubjectSummary`, `SchemaVersionSummary`, `SubjectDetail`, `SchemaReference`, `SchemaVersion`, `TopicSchemas`, `CompatibilityResult`, `RegisteredSchema`, `TopicRow`; consts `SCHEMA_TYPES`, `COMPATIBILITY_LEVELS` (`{value,label,description}[]`); functions `groupByTopic(list) → {topics: TopicRow[], others: SubjectSummary[]}`, `formatSchema(schemaType, schema)`, `compatibilityLabel(x)`
- Produces (`@/composables/useSchemaRegistry`): `{ status, configured, globalCompatibility, load, setStatus }` — 모듈 스코프 싱글턴(`useSession` 과 같은 패턴)

- [ ] **Step 1: 실패하는 테스트 작성**

`web/src/lib/__tests__/schemas.spec.ts`:

```ts
import { describe, it, expect } from 'vitest'
import { groupByTopic, formatSchema, compatibilityLabel, COMPATIBILITY_LEVELS, type SubjectSummary } from '../schemas'

const s = (subject: string, topic: string | null, kind: 'key' | 'value' | 'other', v = 1): SubjectSummary => ({
  subject, topic, kind, latestVersion: v, schemaType: 'AVRO', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL',
})

describe('schemas lib', () => {
  it('토픽별로 key/value 를 묶고 규칙 밖은 others 로 보낸다', () => {
    const { topics, others } = groupByTopic([
      s('orders-value', 'orders', 'value', 3), s('com.x.Y', null, 'other'), s('orders-key', 'orders', 'key'), s('events-value', 'events', 'value'),
    ])
    expect(topics.map((t) => t.topic)).toEqual(['events', 'orders'])
    expect(topics[1]!.key?.subject).toBe('orders-key')
    expect(topics[1]!.value?.latestVersion).toBe(3)
    expect(topics[0]!.key).toBeNull()
    expect(others.map((o) => o.subject)).toEqual(['com.x.Y'])
  })

  it('JSON/AVRO 는 들여쓰기, 파싱 실패나 PROTOBUF 는 원문', () => {
    expect(formatSchema('AVRO', '{"type":"string"}')).toBe('{\n  "type": "string"\n}')
    expect(formatSchema('JSON', 'not json')).toBe('not json')
    expect(formatSchema('PROTOBUF', 'syntax = "proto3";')).toBe('syntax = "proto3";')
  })

  it('호환성 라벨은 출처를 드러낸다', () => {
    expect(compatibilityLabel({ compatibility: 'FULL', compatibilitySource: 'SUBJECT' })).toBe('FULL')
    expect(compatibilityLabel({ compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL' })).toBe('전역 (BACKWARD)')
  })

  it('호환성 레벨 7개에 설명이 있다', () => {
    expect(COMPATIBILITY_LEVELS.map((l) => l.value)).toEqual([
      'BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE', 'NONE',
    ])
    expect(COMPATIBILITY_LEVELS.every((l) => l.description.length > 0)).toBe(true)
  })
})
```

`web/src/composables/__tests__/useSchemaRegistry.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import { useSchemaRegistry } from '../useSchemaRegistry'

describe('useSchemaRegistry', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    useSchemaRegistry().setStatus(null)
  })

  it('설정되어 있으면 configured 와 전역 호환성을 노출한다', async () => {
    vi.mocked(api).mockResolvedValue({ configured: true, urls: ['http://sr'], globalCompatibility: 'BACKWARD' })
    const { load, configured, globalCompatibility } = useSchemaRegistry()
    await load()
    expect(configured.value).toBe(true)
    expect(globalCompatibility.value).toBe('BACKWARD')
    expect(api).toHaveBeenCalledWith('/schemas/status')
  })

  it('미설정이거나 조회 실패면 configured 가 false', async () => {
    vi.mocked(api).mockResolvedValue({ configured: false, urls: [], globalCompatibility: null })
    const { load, configured } = useSchemaRegistry()
    await load()
    expect(configured.value).toBe(false)
    vi.mocked(api).mockRejectedValue(new Error('401'))
    await load()
    expect(configured.value).toBe(false)
  })

  it('setStatus 로 전역 호환성 변경을 반영한다', () => {
    const { setStatus, globalCompatibility } = useSchemaRegistry()
    setStatus({ configured: true, urls: [], globalCompatibility: 'FULL' })
    expect(globalCompatibility.value).toBe('FULL')
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/lib/__tests__/schemas.spec.ts src/composables/__tests__/useSchemaRegistry.spec.ts`
Expected: FAIL (모듈 없음)

- [ ] **Step 3: 구현**

`web/src/lib/schemas.ts`:

```ts
// Schema Registry 화면의 공용 타입·상수·순수 함수. 서버 SchemaDtos 와 필드명이 같다.
export type SubjectKind = 'key' | 'value' | 'other'
export type SchemaType = 'AVRO' | 'JSON' | 'PROTOBUF'
export type CompatibilityLevel =
  | 'BACKWARD' | 'BACKWARD_TRANSITIVE' | 'FORWARD' | 'FORWARD_TRANSITIVE' | 'FULL' | 'FULL_TRANSITIVE' | 'NONE'

export interface SchemaRegistryStatus { configured: boolean; urls: string[]; globalCompatibility: CompatibilityLevel | null }
export interface SubjectSummary {
  subject: string
  topic: string | null
  kind: SubjectKind
  latestVersion: number
  schemaType: string
  compatibility: string
  compatibilitySource: 'SUBJECT' | 'GLOBAL'
}
export interface SchemaVersionSummary { version: number; id: number; schemaType: string }
export interface SubjectDetail {
  subject: string
  topic: string | null
  kind: SubjectKind
  compatibility: string
  compatibilitySource: 'SUBJECT' | 'GLOBAL'
  versions: SchemaVersionSummary[]
}
export interface SchemaReference { name: string; subject: string; version: number }
export interface SchemaVersion {
  subject: string; version: number; id: number; schemaType: string; schema: string; references: SchemaReference[]
}
export interface TopicSchemas { topic: string; key: SubjectSummary | null; value: SubjectSummary | null }
export interface CompatibilityResult { compatible: boolean; messages: string[] }
export interface RegisteredSchema { subject: string; id: number; version: number }
export interface TopicRow { topic: string; key: SubjectSummary | null; value: SubjectSummary | null }

export const SCHEMA_TYPES: SchemaType[] = ['AVRO', 'JSON', 'PROTOBUF']

export const COMPATIBILITY_LEVELS: { value: CompatibilityLevel; label: string; description: string }[] = [
  { value: 'BACKWARD', label: 'BACKWARD', description: '새 스키마로 직전 버전 데이터를 읽을 수 있어야 함 (기본값, 컨슈머 먼저 배포)' },
  { value: 'BACKWARD_TRANSITIVE', label: 'BACKWARD_TRANSITIVE', description: '새 스키마로 모든 이전 버전 데이터를 읽을 수 있어야 함' },
  { value: 'FORWARD', label: 'FORWARD', description: '직전 스키마로 새 데이터를 읽을 수 있어야 함 (프로듀서 먼저 배포)' },
  { value: 'FORWARD_TRANSITIVE', label: 'FORWARD_TRANSITIVE', description: '모든 이전 스키마로 새 데이터를 읽을 수 있어야 함' },
  { value: 'FULL', label: 'FULL', description: 'BACKWARD + FORWARD (직전 버전과 양방향 호환)' },
  { value: 'FULL_TRANSITIVE', label: 'FULL_TRANSITIVE', description: '모든 버전과 양방향 호환' },
  { value: 'NONE', label: 'NONE', description: '검사 없음 — 소비자가 깨질 수 있음' },
]

// 토픽 이름 오름차순, 규칙 밖(other)은 별도 목록
export function groupByTopic(list: SubjectSummary[]): { topics: TopicRow[]; others: SubjectSummary[] } {
  const byTopic = new Map<string, TopicRow>()
  const others: SubjectSummary[] = []
  for (const s of list) {
    if (s.kind === 'other' || !s.topic) { others.push(s); continue }
    const row = byTopic.get(s.topic) ?? { topic: s.topic, key: null, value: null }
    if (s.kind === 'key') row.key = s
    else row.value = s
    byTopic.set(s.topic, row)
  }
  const topics = [...byTopic.values()].sort((a, b) => a.topic.localeCompare(b.topic))
  return { topics, others }
}

export function formatSchema(schemaType: string, schema: string): string {
  if (schemaType === 'PROTOBUF') return schema
  try {
    return JSON.stringify(JSON.parse(schema), null, 2)
  } catch {
    return schema
  }
}

export function compatibilityLabel(x: { compatibility: string; compatibilitySource: 'SUBJECT' | 'GLOBAL' }): string {
  return x.compatibilitySource === 'SUBJECT' ? x.compatibility : `전역 (${x.compatibility})`
}
```

`web/src/composables/useSchemaRegistry.ts`:

```ts
import { ref, computed } from 'vue'
import { api } from '@/api/client'
import type { SchemaRegistryStatus } from '@/lib/schemas'

// 모듈 스코프 싱글턴: App 진입 시 1회 로드. configured=false 면 메뉴·토픽 섹션을 숨긴다.
const status = ref<SchemaRegistryStatus | null>(null)

export function useSchemaRegistry() {
  async function load() {
    try {
      status.value = await api<SchemaRegistryStatus>('/schemas/status')
    } catch {
      status.value = { configured: false, urls: [], globalCompatibility: null }
    }
  }
  function setStatus(s: SchemaRegistryStatus | null) { status.value = s }
  const configured = computed(() => status.value?.configured === true)
  const globalCompatibility = computed(() => status.value?.globalCompatibility ?? null)
  return { status, configured, globalCompatibility, load, setStatus }
}
```

- [ ] **Step 4: 통과 확인**

Run: `cd web && npx vitest run src/lib/__tests__/schemas.spec.ts src/composables/__tests__/useSchemaRegistry.spec.ts`
Expected: 7 tests passed

- [ ] **Step 5: 커밋**

```bash
git add web/src/lib/schemas.ts web/src/composables/useSchemaRegistry.ts web/src/lib/__tests__/schemas.spec.ts web/src/composables/__tests__/useSchemaRegistry.spec.ts
git commit -m "feat(web): 스키마 타입·순수 함수·Registry 상태 컴포저블"
```

---

### Task 7: 목록 화면 `SchemasView`, 등록 모달, 라우트·메뉴

**Files:**
- Create: `web/src/components/SchemaRegisterModal.vue`
- Create: `web/src/views/SchemasView.vue`
- Modify: `web/src/router/index.ts` (`/schemas` 추가), `web/src/App.vue` (메뉴 + `useSchemaRegistry().load()`)
- Test: `web/src/components/__tests__/SchemaRegisterModal.spec.ts`, `web/src/views/__tests__/SchemasView.spec.ts`

**Interfaces:**
- Consumes: `@/lib/schemas`, `@/composables/useSchemaRegistry`, `useSession`, `ModalDialog`, `api`
- Produces: `SchemaRegisterModal` props `{ topic?: string; kind?: 'key' | 'value' }` (둘 다 있으면 고정), emits `close`, `registered(r: RegisteredSchema)`
- 선택자 계약: `select[name="topic"]`, `input[name="kind"]`(radio, value key|value), `select[name="schemaType"]`, `textarea[name="schema"]`, `button.check`, `button.register`, `.check-ok`, `.check-fail li`, `.registered`; 목록: `button.register-schema`, `table.topics`, `table.others`, `.global-compat`

- [ ] **Step 1: 실패하는 테스트 작성**

`SchemaRegisterModal.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import SchemaRegisterModal from '../SchemaRegisterModal.vue'

const topics = [{ name: 'orders', partitionCount: 3, replicationFactor: 3 }, { name: 'events', partitionCount: 1, replicationFactor: 3 }]

describe('SchemaRegisterModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('검사 통과 전엔 등록 버튼이 비활성이고, 검사 실패 사유를 보여준다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(topics)
      .mockResolvedValueOnce({ compatible: false, messages: ['READER_FIELD_MISSING_DEFAULT_VALUE'] })
    const wrapper = mount(SchemaRegisterModal)
    await flushPromises()
    expect(wrapper.find('button.register').attributes('disabled')).toBeDefined()
    await wrapper.find('select[name="topic"]').setValue('orders')
    await wrapper.find('textarea[name="schema"]').setValue('{"type":"string"}')
    await wrapper.find('button.check').trigger('click')
    await flushPromises()
    const call = vi.mocked(api).mock.calls[1]!
    expect(call[0]).toBe('/schemas/compatibility')
    expect(JSON.parse((call[1] as RequestInit).body as string)).toEqual({
      topic: 'orders', kind: 'value', schemaType: 'AVRO', schema: '{"type":"string"}',
    })
    expect(wrapper.find('.check-fail').text()).toContain('READER_FIELD_MISSING_DEFAULT_VALUE')
    expect(wrapper.find('button.register').attributes('disabled')).toBeDefined()
  })

  it('검사 통과 후 등록하면 registered 를 emit 하고 결과를 표시한다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(topics)
      .mockResolvedValueOnce({ compatible: true, messages: [] })
      .mockResolvedValueOnce({ subject: 'orders-key', id: 5, version: 1 })
    const wrapper = mount(SchemaRegisterModal)
    await flushPromises()
    await wrapper.find('select[name="topic"]').setValue('orders')
    await wrapper.find('input[name="kind"][value="key"]').setValue()
    await wrapper.find('select[name="schemaType"]').setValue('JSON')
    await wrapper.find('textarea[name="schema"]').setValue('{"type":"string"}')
    await wrapper.find('button.check').trigger('click')
    await flushPromises()
    expect(wrapper.find('.check-ok').exists()).toBe(true)
    expect(wrapper.find('button.register').attributes('disabled')).toBeUndefined()
    await wrapper.find('button.register').trigger('click')
    await flushPromises()
    const call = vi.mocked(api).mock.calls[2]!
    expect(call[0]).toBe('/schemas/register')
    expect(JSON.parse((call[1] as RequestInit).body as string)).toMatchObject({ topic: 'orders', kind: 'key', schemaType: 'JSON' })
    expect(wrapper.find('.registered').text()).toContain('orders-key')
    expect(wrapper.find('.registered').text()).toContain('v1')
    expect(wrapper.emitted('registered')).toEqual([[{ subject: 'orders-key', id: 5, version: 1 }]])
  })

  it('입력을 바꾸면 검사 결과가 초기화된다', async () => {
    vi.mocked(api).mockResolvedValueOnce(topics).mockResolvedValueOnce({ compatible: true, messages: [] })
    const wrapper = mount(SchemaRegisterModal)
    await flushPromises()
    await wrapper.find('select[name="topic"]').setValue('orders')
    await wrapper.find('textarea[name="schema"]').setValue('{}')
    await wrapper.find('button.check').trigger('click')
    await flushPromises()
    expect(wrapper.find('button.register').attributes('disabled')).toBeUndefined()
    await wrapper.find('textarea[name="schema"]').setValue('{"a":1}')
    expect(wrapper.find('.check-ok').exists()).toBe(false)
    expect(wrapper.find('button.register').attributes('disabled')).toBeDefined()
  })

  it('토픽·종류가 고정되면 선택 UI 가 없고 검사 본문에 그 값이 들어간다', async () => {
    vi.mocked(api).mockResolvedValueOnce({ compatible: true, messages: [] })
    const wrapper = mount(SchemaRegisterModal, { props: { topic: 'orders', kind: 'value' } })
    await flushPromises()
    expect(wrapper.find('select[name="topic"]').exists()).toBe(false)
    expect(wrapper.find('input[name="kind"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('orders-value')
    await wrapper.find('textarea[name="schema"]').setValue('{}')
    await wrapper.find('button.check').trigger('click')
    await flushPromises()
    expect(JSON.parse((vi.mocked(api).mock.calls[0]![1] as RequestInit).body as string)).toMatchObject({ topic: 'orders', kind: 'value' })
  })

  it('등록 실패 메시지를 표시한다', async () => {
    vi.mocked(api)
      .mockResolvedValueOnce(topics)
      .mockResolvedValueOnce({ compatible: true, messages: [] })
      .mockRejectedValueOnce(new Error('존재하지 않는 토픽입니다'))
    const wrapper = mount(SchemaRegisterModal)
    await flushPromises()
    await wrapper.find('select[name="topic"]').setValue('orders')
    await wrapper.find('textarea[name="schema"]').setValue('{}')
    await wrapper.find('button.check').trigger('click')
    await flushPromises()
    await wrapper.find('button.register').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('존재하지 않는 토픽입니다')
    expect(wrapper.emitted('registered')).toBeUndefined()
  })
})
```

`SchemasView.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const isAdmin = ref(true)
vi.mock('@/composables/useSession', () => ({
  useSession: () => ({ isAdmin, session: ref({ username: 'admin', role: 'ADMIN' }), load: vi.fn() }),
}))
const globalCompatibility = ref<string | null>('BACKWARD')
vi.mock('@/composables/useSchemaRegistry', () => ({
  useSchemaRegistry: () => ({ configured: ref(true), globalCompatibility, status: ref(null), load: vi.fn(), setStatus: vi.fn() }),
}))

import { api } from '@/api/client'
import SchemasView from '../SchemasView.vue'

const subjects = [
  { subject: 'orders-value', topic: 'orders', kind: 'value', latestVersion: 3, schemaType: 'AVRO', compatibility: 'FULL', compatibilitySource: 'SUBJECT' },
  { subject: 'orders-key', topic: 'orders', kind: 'key', latestVersion: 1, schemaType: 'JSON', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL' },
  { subject: 'com.x.Y', topic: null, kind: 'other', latestVersion: 2, schemaType: 'PROTOBUF', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL' },
]
const stubs = { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } }

describe('SchemasView', () => {
  beforeEach(() => { vi.mocked(api).mockReset(); isAdmin.value = true })

  it('토픽별 표와 기타 표, 전역 호환성을 보여준다', async () => {
    vi.mocked(api).mockResolvedValueOnce(subjects)
    const wrapper = mount(SchemasView, { global: { stubs } })
    await flushPromises()
    const rows = wrapper.findAll('table.topics tbody tr')
    expect(rows).toHaveLength(1)
    expect(rows[0]!.text()).toContain('orders')
    expect(rows[0]!.text()).toContain('AVRO')
    expect(rows[0]!.text()).toContain('v3')
    expect(rows[0]!.text()).toContain('JSON')
    expect(wrapper.find('a[href="/schemas/orders-value"]').exists()).toBe(true)
    expect(wrapper.find('a[href="/topics/orders"]').exists()).toBe(true)
    expect(wrapper.find('table.others').text()).toContain('com.x.Y')
    expect(wrapper.find('.global-compat').text()).toContain('BACKWARD')
    expect(wrapper.find('button.register-schema').exists()).toBe(true)
  })

  it('비어 있으면 안내 문구와 등록 버튼', async () => {
    vi.mocked(api).mockResolvedValueOnce([])
    const wrapper = mount(SchemasView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('등록된 스키마가 없습니다')
    expect(wrapper.find('button.register-schema').exists()).toBe(true)
  })

  it('DEVELOPER 도 등록 버튼은 보이고, 조회 실패 메시지를 표시한다', async () => {
    isAdmin.value = false
    vi.mocked(api).mockRejectedValueOnce(new Error('Schema Registry 접속 불가'))
    const wrapper = mount(SchemasView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('Schema Registry 접속 불가')
    expect(wrapper.find('button.register-schema').exists()).toBe(true)
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/components/__tests__/SchemaRegisterModal.spec.ts src/views/__tests__/SchemasView.spec.ts`
Expected: FAIL (컴포넌트 없음)

- [ ] **Step 3: `SchemaRegisterModal.vue`**

```vue
<script setup lang="ts">
import { ref, computed, watch, onMounted } from 'vue'
import { api } from '@/api/client'
import { SCHEMA_TYPES, type CompatibilityResult, type RegisteredSchema, type SchemaType } from '@/lib/schemas'
import ModalDialog from './ModalDialog.vue'

// topic·kind 가 모두 주어지면 고정(상세 화면·토픽 섹션에서 "새 버전 등록"). 등록은 호환성 검사를 통과해야 활성화된다.
const props = defineProps<{ topic?: string; kind?: 'key' | 'value' }>()
const emit = defineEmits<{ close: []; registered: [r: RegisteredSchema] }>()

interface TopicSummary { name: string; partitionCount: number; replicationFactor: number }

const fixed = computed(() => !!props.topic && !!props.kind)
const topics = ref<TopicSummary[]>([])
const topic = ref(props.topic ?? '')
const kind = ref<'key' | 'value'>(props.kind ?? 'value')
const schemaType = ref<SchemaType>('AVRO')
const schema = ref('')
const check = ref<CompatibilityResult | null>(null)
const checking = ref(false)
const submitting = ref(false)
const error = ref('')
const result = ref<RegisteredSchema | null>(null)

const subject = computed(() => (topic.value ? `${topic.value}-${kind.value}` : ''))
const canCheck = computed(() => !!topic.value && schema.value.trim().length > 0 && !checking.value)
const canRegister = computed(() => check.value?.compatible === true && !submitting.value && !result.value)

// 입력이 바뀌면 이전 검사 결과는 무효
watch([topic, kind, schemaType, schema], () => { check.value = null; error.value = '' })

onMounted(async () => {
  if (fixed.value) return
  try {
    topics.value = await api<TopicSummary[]>('/topics')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '토픽 목록 조회 실패'
  }
})

function body() {
  return JSON.stringify({ topic: topic.value, kind: kind.value, schemaType: schemaType.value, schema: schema.value })
}

async function runCheck() {
  if (!canCheck.value) return
  error.value = ''
  checking.value = true
  try {
    check.value = await api<CompatibilityResult>('/schemas/compatibility', { method: 'POST', body: body() })
  } catch (e) {
    error.value = e instanceof Error ? e.message : '호환성 검사 실패'
  } finally {
    checking.value = false
  }
}

async function register() {
  if (!canRegister.value) return
  error.value = ''
  submitting.value = true
  try {
    result.value = await api<RegisteredSchema>('/schemas/register', { method: 'POST', body: body() })
    emit('registered', result.value)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '등록 실패'
    check.value = null // 등록 시점 재검사 실패 등 — 다시 검사하도록
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog :title="fixed ? '새 버전 등록' : '스키마 등록'" @close="emit('close')">
    <div class="form">
      <template v-if="!fixed">
        <label>
          토픽
          <select name="topic" v-model="topic">
            <option value="">— 선택 —</option>
            <option v-for="t in topics" :key="t.name" :value="t.name">{{ t.name }}</option>
          </select>
        </label>
        <fieldset class="kind">
          <legend>종류</legend>
          <label><input type="radio" name="kind" value="key" v-model="kind" /> key</label>
          <label><input type="radio" name="kind" value="value" v-model="kind" /> value</label>
        </fieldset>
      </template>
      <p v-else>서브젝트 <strong>{{ subject }}</strong> 에 새 버전을 등록합니다.</p>
      <label>
        형식
        <select name="schemaType" v-model="schemaType">
          <option v-for="t in SCHEMA_TYPES" :key="t" :value="t">{{ t }}</option>
        </select>
      </label>
      <label>
        스키마 본문 (최대 1 MB)
        <textarea name="schema" v-model="schema" rows="12" spellcheck="false" />
      </label>
      <div class="check-row">
        <button type="button" class="btn check" :disabled="!canCheck" @click="runCheck">
          {{ checking ? '검사 중…' : '호환성 검사' }}
        </button>
        <span v-if="subject" class="subject-hint">→ {{ subject }}</span>
      </div>
      <p v-if="check && check.compatible" class="check-ok">호환성 검사 통과. 등록할 수 있습니다.</p>
      <div v-else-if="check" class="check-fail">
        <p>호환성 검사에 실패했습니다.</p>
        <ul><li v-for="(m, i) in check.messages" :key="i">{{ m }}</li></ul>
      </div>
      <p v-if="result" class="registered">등록됨: {{ result.subject }} v{{ result.version }} (id {{ result.id }})</p>
      <p v-if="error" class="error">{{ error }}</p>
    </div>
    <template #footer>
      <button v-if="result" type="button" class="btn primary" @click="emit('close')">닫기</button>
      <template v-else>
        <button type="button" class="btn" @click="emit('close')">취소</button>
        <button type="button" class="btn primary register" :disabled="!canRegister" @click="register">
          {{ submitting ? '등록 중…' : '등록' }}
        </button>
      </template>
    </template>
  </ModalDialog>
</template>

<style scoped>
.form { display: flex; flex-direction: column; gap: 0.75rem; }
label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
.kind { display: flex; gap: 1rem; border: 1px solid var(--line); border-radius: 6px; padding: 0.4rem 0.75rem; }
.kind label { flex-direction: row; align-items: center; gap: 0.35rem; }
textarea { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.82rem; }
.check-row { display: flex; align-items: center; gap: 0.75rem; }
.subject-hint { color: var(--ink-soft); font-size: 0.85rem; }
.check-ok { color: var(--accent); margin: 0; }
.check-fail { color: var(--crit); margin: 0; }
.check-fail ul { margin: 0.25rem 0 0 1.2rem; font-family: ui-monospace, monospace; font-size: 0.8rem; }
.registered { margin: 0; padding: 0.5rem 0.75rem; background: var(--surface-2); border-radius: 6px; }
.error { color: var(--crit); margin: 0; }
</style>
```

- [ ] **Step 4: `SchemasView.vue`**

```vue
<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import { useSchemaRegistry } from '@/composables/useSchemaRegistry'
import { groupByTopic, compatibilityLabel, type SubjectSummary } from '@/lib/schemas'
import SchemaRegisterModal from '@/components/SchemaRegisterModal.vue'

const subjects = ref<SubjectSummary[]>([])
const error = ref('')
const showRegister = ref(false)
const { isAdmin } = useSession()
const { globalCompatibility } = useSchemaRegistry()

const grouped = computed(() => groupByTopic(subjects.value))

async function load() {
  try {
    subjects.value = await api<SubjectSummary[]>('/schemas/subjects')
    error.value = ''
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(load)

function cell(s: SubjectSummary | null) {
  return s ? `${s.schemaType} · v${s.latestVersion}` : '—'
}
function rowCompat(row: { key: SubjectSummary | null; value: SubjectSummary | null }) {
  const s = row.value ?? row.key
  return s ? compatibilityLabel(s) : '—'
}
</script>

<template>
  <main>
    <div class="head-row">
      <h1>스키마</h1>
      <div class="actions">
        <span class="global-compat">전역 호환성: <strong>{{ globalCompatibility ?? '—' }}</strong></span>
        <slot name="global-actions" />
        <button type="button" class="btn primary register-schema" @click="showRegister = true">스키마 등록</button>
      </div>
    </div>
    <p class="hint">
      Schema Registry 의 서브젝트를 토픽 기준(<code>&lt;토픽&gt;-key</code> / <code>&lt;토픽&gt;-value</code>)으로 보여줍니다.
      새 버전 등록은 호환성 검사를 통과해야 합니다.
      <span v-if="!isAdmin">호환성 변경과 삭제는 ADMIN 만 할 수 있습니다.</span>
    </p>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-else>
      <p v-if="subjects.length === 0" class="hint">등록된 스키마가 없습니다.</p>
      <template v-else>
        <table class="topics">
          <thead><tr><th>토픽</th><th>key 스키마</th><th>value 스키마</th><th>호환성</th></tr></thead>
          <tbody>
            <tr v-for="row in grouped.topics" :key="row.topic">
              <td><RouterLink :to="`/topics/${row.topic}`">{{ row.topic }}</RouterLink></td>
              <td>
                <RouterLink v-if="row.key" :to="`/schemas/${row.key.subject}`">{{ cell(row.key) }}</RouterLink>
                <span v-else>—</span>
              </td>
              <td>
                <RouterLink v-if="row.value" :to="`/schemas/${row.value.subject}`">{{ cell(row.value) }}</RouterLink>
                <span v-else>—</span>
              </td>
              <td>{{ rowCompat(row) }}</td>
            </tr>
          </tbody>
        </table>
        <template v-if="grouped.others.length > 0">
          <h2>기타 서브젝트</h2>
          <p class="hint">토픽 규칙 밖 이름입니다. 조회만 가능하며 등록은 앱/CLI 에서 합니다.</p>
          <table class="others">
            <thead><tr><th>서브젝트</th><th>형식</th><th>최신 버전</th></tr></thead>
            <tbody>
              <tr v-for="s in grouped.others" :key="s.subject">
                <td><RouterLink :to="`/schemas/${s.subject}`">{{ s.subject }}</RouterLink></td>
                <td>{{ s.schemaType }}</td>
                <td>v{{ s.latestVersion }}</td>
              </tr>
            </tbody>
          </table>
        </template>
      </template>
    </template>
    <SchemaRegisterModal v-if="showRegister" @close="showRegister = false" @registered="load" />
  </main>
</template>

<style scoped>
.head-row { display: flex; justify-content: space-between; align-items: center; }
.actions { display: flex; align-items: center; gap: 0.75rem; }
.global-compat { font-size: 0.9rem; color: var(--ink-soft); }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.error { color: var(--crit); }
</style>
```

(`<slot name="global-actions" />` 는 Task 8 에서 ADMIN 전용 "전역 호환성 변경" 버튼으로 교체된다 — 이 태스크에서는 비어 있는 슬롯으로 둔다.)

- [ ] **Step 5: 라우트·메뉴**

`web/src/router/index.ts` 의 `/kafka-apps/:name` 줄 뒤에:

```ts
    { path: '/schemas', component: () => import('@/views/SchemasView.vue') },
```

`web/src/App.vue` script: `import { useSchemaRegistry } from '@/composables/useSchemaRegistry'` 추가, `const { load } = useSession()` 아래에 `const { configured: schemaRegistryConfigured, load: loadSchemaRegistry } = useSchemaRegistry()`, `onMounted(load)` 를 `onMounted(() => { load(); loadSchemaRegistry() })` 로 변경. 템플릿의 `Kafka 계정` 링크 뒤에:

```vue
    <RouterLink v-if="schemaRegistryConfigured" to="/schemas">스키마</RouterLink>
```

- [ ] **Step 6: 통과 확인**

Run: `cd web && npx vitest run && npm run type-check`
Expected: 새 스펙 8건 포함 전부 통과, type-check 통과. (`App.vue` 를 마운트하는 기존 스펙이 있다면 `useSchemaRegistry` 의 `/schemas/status` 호출이 추가되어 `api` mock 순서가 어긋날 수 있다 — 그 스펙에서 `vi.mock('@/composables/useSchemaRegistry', …)` 로 `configured: ref(false), load: vi.fn()` 을 주입한다.)

- [ ] **Step 7: 커밋**

```bash
git add web/src/components/SchemaRegisterModal.vue web/src/views/SchemasView.vue web/src/router/index.ts web/src/App.vue web/src/components/__tests__/SchemaRegisterModal.spec.ts web/src/views/__tests__/SchemasView.spec.ts
git commit -m "feat(web): 스키마 목록 화면·등록 모달·메뉴"
```

---
### Task 8: 상세 화면 `SchemaSubjectView`, 삭제·호환성 모달, 전역 호환성 버튼

**Files:**
- Create: `web/src/components/CompatibilityModal.vue`
- Create: `web/src/components/SchemaDeleteModal.vue`
- Create: `web/src/views/SchemaSubjectView.vue`
- Modify: `web/src/router/index.ts` (`/schemas/:subject`), `web/src/views/SchemasView.vue` (전역 호환성 버튼)
- Test: `web/src/components/__tests__/CompatibilityModal.spec.ts`, `web/src/components/__tests__/SchemaDeleteModal.spec.ts`, `web/src/views/__tests__/SchemaSubjectView.spec.ts`

**Interfaces:**
- Consumes: Task 6 lib/composable, Task 7 `SchemaRegisterModal` (`topic`+`kind` 고정 모드), `ModalDialog`, `useSession`, `api`
- Produces: `CompatibilityModal` props `{ subject?: string; current: string; source?: 'SUBJECT' | 'GLOBAL' }` emits `close`, `saved(payload: SubjectDetail | SchemaRegistryStatus)`; `SchemaDeleteModal` props `{ subject: string }` emits `close`, `deleted`
- 선택자: `select[name="compatibility"]`, `.warn-none`, `button.primary`; 삭제 `input`, `button.danger`; 상세 `button.register-version`, `button.change-compat`, `button.delete-subject`, `select[name="version"]`, `pre.schema`, `input[name="compare"]`, `select[name="compare-version"]`, `pre.schema-compare`; 목록 `button.change-global`

- [ ] **Step 1: 실패하는 테스트 작성**

`CompatibilityModal.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import CompatibilityModal from '../CompatibilityModal.vue'

describe('CompatibilityModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('전역 모드: 레벨을 고르면 PUT /ops/schemas/config 하고 saved 를 emit', async () => {
    vi.mocked(api).mockResolvedValueOnce({ configured: true, urls: [], globalCompatibility: 'FULL' })
    const wrapper = mount(CompatibilityModal, { props: { current: 'BACKWARD' } })
    expect((wrapper.find('select[name="compatibility"]').element as HTMLSelectElement).value).toBe('BACKWARD')
    expect(wrapper.find('option[value=""]').exists()).toBe(false)
    await wrapper.find('select[name="compatibility"]').setValue('FULL')
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/schemas/config', { method: 'PUT', body: JSON.stringify({ compatibility: 'FULL' }) })
    expect(wrapper.emitted('saved')![0]![0]).toMatchObject({ globalCompatibility: 'FULL' })
  })

  it('서브젝트 모드: NONE 경고와 전역 상속(null) 전송', async () => {
    vi.mocked(api).mockResolvedValueOnce({ subject: 'orders-value', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL', versions: [] })
    const wrapper = mount(CompatibilityModal, { props: { subject: 'orders-value', current: 'FULL', source: 'SUBJECT' } })
    expect(wrapper.find('option[value=""]').exists()).toBe(true)
    await wrapper.find('select[name="compatibility"]').setValue('NONE')
    expect(wrapper.find('.warn-none').exists()).toBe(true)
    await wrapper.find('select[name="compatibility"]').setValue('')
    expect(wrapper.find('.warn-none').exists()).toBe(false)
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/schemas/subjects/orders-value/config', { method: 'PUT', body: JSON.stringify({ compatibility: null }) })
    expect(wrapper.emitted('saved')).toHaveLength(1)
  })

  it('실패 메시지를 표시한다', async () => {
    vi.mocked(api).mockRejectedValueOnce(new Error('Schema Registry 접속 불가'))
    const wrapper = mount(CompatibilityModal, { props: { current: 'BACKWARD' } })
    await wrapper.find('select[name="compatibility"]').setValue('FULL')
    await wrapper.find('button.primary').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('Schema Registry 접속 불가')
    expect(wrapper.emitted('saved')).toBeUndefined()
  })
})
```

`SchemaDeleteModal.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import SchemaDeleteModal from '../SchemaDeleteModal.vue'

describe('SchemaDeleteModal', () => {
  beforeEach(() => vi.mocked(api).mockReset())

  it('서브젝트 이름이 일치하기 전에는 삭제 버튼이 비활성', async () => {
    const wrapper = mount(SchemaDeleteModal, { props: { subject: 'orders-value' } })
    expect(wrapper.find('button.danger').attributes('disabled')).toBeDefined()
    await wrapper.find('input').setValue('orders-valu')
    expect(wrapper.find('button.danger').attributes('disabled')).toBeDefined()
    expect(wrapper.text()).toContain('soft delete')
  })

  it('일치하면 DELETE 를 호출하고 deleted 를 emit 한다', async () => {
    vi.mocked(api).mockResolvedValueOnce({ subject: 'orders-value', deletedVersions: [1, 2] })
    const wrapper = mount(SchemaDeleteModal, { props: { subject: 'orders-value' } })
    await wrapper.find('input').setValue('orders-value')
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/ops/schemas/subjects/orders-value', { method: 'DELETE' })
    expect(wrapper.emitted('deleted')).toHaveLength(1)
  })

  it('실패하면 에러를 표시하고 deleted 를 emit 하지 않는다', async () => {
    vi.mocked(api).mockRejectedValueOnce(new Error('존재하지 않는 서브젝트/버전입니다: orders-value'))
    const wrapper = mount(SchemaDeleteModal, { props: { subject: 'orders-value' } })
    await wrapper.find('input').setValue('orders-value')
    await wrapper.find('button.danger').trigger('click')
    await flushPromises()
    expect(wrapper.text()).toContain('존재하지 않는 서브젝트')
    expect(wrapper.emitted('deleted')).toBeUndefined()
  })
})
```

`SchemaSubjectView.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const isAdmin = ref(true)
vi.mock('@/composables/useSession', () => ({
  useSession: () => ({ isAdmin, session: ref({ username: 'admin', role: 'ADMIN' }), load: vi.fn() }),
}))
const subjectParam = ref('orders-value')
vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { subject: subjectParam.value } }),
  useRouter: () => ({ push: vi.fn() }),
}))

import { api } from '@/api/client'
import SchemaSubjectView from '../SchemaSubjectView.vue'

const detail = {
  subject: 'orders-value', topic: 'orders', kind: 'value', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL',
  versions: [{ version: 2, id: 9, schemaType: 'AVRO' }, { version: 1, id: 4, schemaType: 'AVRO' }],
}
const v2 = { subject: 'orders-value', version: 2, id: 9, schemaType: 'AVRO', schema: '{"type":"record","name":"Order","fields":[]}', references: [] }
const v1 = { subject: 'orders-value', version: 1, id: 4, schemaType: 'AVRO', schema: '{"type":"string"}', references: [] }
const stubs = { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } }

function mockApi() {
  vi.mocked(api).mockImplementation((url: string) => {
    if (url === '/schemas/subjects/orders-value') return Promise.resolve(detail)
    if (url === '/schemas/subjects/orders-value/versions/2') return Promise.resolve(v2)
    if (url === '/schemas/subjects/orders-value/versions/1') return Promise.resolve(v1)
    return Promise.reject(new Error(`unexpected ${url}`))
  })
}

describe('SchemaSubjectView', () => {
  beforeEach(() => { vi.mocked(api).mockReset(); isAdmin.value = true; subjectParam.value = 'orders-value' })

  it('메타데이터와 최신 버전 본문을 정렬해 보여주고 ADMIN 버튼을 노출한다', async () => {
    mockApi()
    const wrapper = mount(SchemaSubjectView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('orders-value')
    expect(wrapper.find('a[href="/topics/orders"]').exists()).toBe(true)
    expect(wrapper.text()).toContain('전역 (BACKWARD)')
    expect((wrapper.find('select[name="version"]').element as HTMLSelectElement).value).toBe('2')
    expect(wrapper.find('pre.schema').text()).toContain('"name": "Order"')
    expect(wrapper.find('button.register-version').exists()).toBe(true)
    expect(wrapper.find('button.change-compat').exists()).toBe(true)
    expect(wrapper.find('button.delete-subject').exists()).toBe(true)
  })

  it('버전을 바꾸면 본문을 다시 불러오고, 비교를 켜면 두 번째 본문을 나란히 보여준다', async () => {
    mockApi()
    const wrapper = mount(SchemaSubjectView, { global: { stubs } })
    await flushPromises()
    await wrapper.find('select[name="version"]').setValue('1')
    await flushPromises()
    expect(wrapper.find('pre.schema').text()).toContain('"type": "string"')
    await wrapper.find('input[name="compare"]').setValue(true)
    await wrapper.find('select[name="compare-version"]').setValue('2')
    await flushPromises()
    expect(wrapper.find('pre.schema-compare').text()).toContain('"name": "Order"')
  })

  it('DEVELOPER 는 새 버전 등록만 보이고, 규칙 밖 서브젝트는 등록 버튼도 없다', async () => {
    isAdmin.value = false
    mockApi()
    let wrapper = mount(SchemaSubjectView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.find('button.register-version').exists()).toBe(true)
    expect(wrapper.find('button.change-compat').exists()).toBe(false)
    expect(wrapper.find('button.delete-subject').exists()).toBe(false)

    subjectParam.value = 'com.x.Y'
    vi.mocked(api).mockImplementation((url: string) => {
      if (url === '/schemas/subjects/com.x.Y') return Promise.resolve({ ...detail, subject: 'com.x.Y', topic: null, kind: 'other' })
      return Promise.resolve({ ...v2, subject: 'com.x.Y' })
    })
    wrapper = mount(SchemaSubjectView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.find('button.register-version').exists()).toBe(false)
    expect(wrapper.text()).toContain('기타')
  })

  it('조회 실패 메시지를 표시한다', async () => {
    vi.mocked(api).mockRejectedValue(new Error('존재하지 않는 서브젝트/버전입니다: orders-value'))
    const wrapper = mount(SchemaSubjectView, { global: { stubs } })
    await flushPromises()
    expect(wrapper.text()).toContain('존재하지 않는 서브젝트')
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/components/__tests__/CompatibilityModal.spec.ts src/components/__tests__/SchemaDeleteModal.spec.ts src/views/__tests__/SchemaSubjectView.spec.ts`
Expected: FAIL (컴포넌트 없음)

- [ ] **Step 3: `CompatibilityModal.vue`**

```vue
<script setup lang="ts">
import { ref, computed } from 'vue'
import { api } from '@/api/client'
import { COMPATIBILITY_LEVELS, type SchemaRegistryStatus, type SubjectDetail } from '@/lib/schemas'
import ModalDialog from './ModalDialog.vue'

// subject 가 없으면 전역 호환성, 있으면 서브젝트 호환성. 서브젝트 모드는 "전역 설정 따르기"(빈 값 → null 전송)를 추가로 제공한다.
const props = defineProps<{ subject?: string; current: string; source?: 'SUBJECT' | 'GLOBAL' }>()
const emit = defineEmits<{ close: []; saved: [payload: SubjectDetail | SchemaRegistryStatus] }>()

const isSubject = computed(() => !!props.subject)
const level = ref<string>(isSubject.value && props.source === 'GLOBAL' ? '' : props.current)
const error = ref('')
const submitting = ref(false)
const selected = computed(() => COMPATIBILITY_LEVELS.find((l) => l.value === level.value))

async function save() {
  error.value = ''
  submitting.value = true
  try {
    const payload = isSubject.value
      ? await api<SubjectDetail>(`/ops/schemas/subjects/${props.subject}/config`, {
          method: 'PUT', body: JSON.stringify({ compatibility: level.value || null }),
        })
      : await api<SchemaRegistryStatus>('/ops/schemas/config', {
          method: 'PUT', body: JSON.stringify({ compatibility: level.value }),
        })
    emit('saved', payload)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '변경 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog :title="isSubject ? `호환성 변경 — ${subject}` : '전역 호환성 변경'" @close="emit('close')">
    <div class="form">
      <label>
        호환성 모드
        <select name="compatibility" v-model="level">
          <option v-if="isSubject" value="">전역 설정 따르기</option>
          <option v-for="l in COMPATIBILITY_LEVELS" :key="l.value" :value="l.value">{{ l.label }}</option>
        </select>
      </label>
      <p class="desc">{{ selected ? selected.description : '서브젝트 단위 설정을 지우고 전역 호환성을 상속합니다.' }}</p>
      <p v-if="level === 'NONE'" class="warn-none">
        NONE 은 호환성 검사를 하지 않습니다. 기존 데이터를 읽는 컨슈머가 깨질 수 있습니다.
      </p>
      <p v-if="error" class="error">{{ error }}</p>
    </div>
    <template #footer>
      <button type="button" class="btn" @click="emit('close')">취소</button>
      <button type="button" class="btn primary" :disabled="submitting" @click="save">{{ submitting ? '적용 중…' : '적용' }}</button>
    </template>
  </ModalDialog>
</template>

<style scoped>
.form { display: flex; flex-direction: column; gap: 0.6rem; }
label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
.desc { margin: 0; color: var(--ink-soft); font-size: 0.85rem; }
.warn-none { margin: 0; color: var(--crit); }
.error { color: var(--crit); margin: 0; }
</style>
```

- [ ] **Step 4: `SchemaDeleteModal.vue`**

```vue
<script setup lang="ts">
import { ref, computed } from 'vue'
import { api } from '@/api/client'
import ModalDialog from './ModalDialog.vue'

const props = defineProps<{ subject: string }>()
const emit = defineEmits<{ close: []; deleted: [] }>()

const confirmText = ref('')
const error = ref('')
const submitting = ref(false)
const canDelete = computed(() => confirmText.value === props.subject)

async function remove() {
  error.value = ''
  submitting.value = true
  try {
    await api(`/ops/schemas/subjects/${props.subject}`, { method: 'DELETE' })
    emit('deleted')
  } catch (e) {
    error.value = e instanceof Error ? e.message : '삭제 실패'
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <ModalDialog title="서브젝트 삭제" @close="emit('close')">
    <p>
      <strong>'{{ subject }}'</strong> 의 모든 버전을 삭제합니다. soft delete 라 Schema Registry 에서 복구할 수 있지만,
      이 서브젝트를 쓰는 프로듀서·컨슈머는 즉시 영향을 받습니다.
    </p>
    <label>
      계속하려면 서브젝트 이름을 입력하세요
      <input v-model="confirmText" :placeholder="subject" />
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
.error { color: var(--crit); margin: 0.5rem 0 0; }
</style>
```

- [ ] **Step 5: `SchemaSubjectView.vue`**

```vue
<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import { formatSchema, compatibilityLabel, type SchemaVersion, type SubjectDetail } from '@/lib/schemas'
import SchemaRegisterModal from '@/components/SchemaRegisterModal.vue'
import CompatibilityModal from '@/components/CompatibilityModal.vue'
import SchemaDeleteModal from '@/components/SchemaDeleteModal.vue'

const route = useRoute()
const router = useRouter()
const { isAdmin } = useSession()
const subject = String(route.params.subject)

const detail = ref<SubjectDetail | null>(null)
const error = ref('')
const selected = ref('')          // 선택한 버전 (문자열, select 바인딩)
const current = ref<SchemaVersion | null>(null)
const compare = ref(false)
const compareSelected = ref('')
const compareVersion = ref<SchemaVersion | null>(null)
const showRegister = ref(false)
const showCompat = ref(false)
const showDelete = ref(false)

const isTopicBound = computed(() => detail.value?.kind === 'key' || detail.value?.kind === 'value')
const latestType = computed(() => detail.value?.versions[0]?.schemaType ?? '')

async function loadVersion(version: string): Promise<SchemaVersion> {
  return api<SchemaVersion>(`/schemas/subjects/${subject}/versions/${version}`)
}

async function load() {
  try {
    detail.value = await api<SubjectDetail>(`/schemas/subjects/${subject}`)
    error.value = ''
    const latest = detail.value.versions[0]
    if (latest) {
      selected.value = String(latest.version)
      current.value = await loadVersion(selected.value)
    }
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(load)

watch(selected, async (v) => {
  if (!v || !detail.value) return
  try { current.value = await loadVersion(v) } catch (e) { error.value = e instanceof Error ? e.message : '조회 실패' }
})
watch(compareSelected, async (v) => {
  if (!v) { compareVersion.value = null; return }
  try { compareVersion.value = await loadVersion(v) } catch (e) { error.value = e instanceof Error ? e.message : '조회 실패' }
})
watch(compare, (on) => { if (!on) { compareSelected.value = ''; compareVersion.value = null } })

function onCompatSaved(payload: unknown) {
  detail.value = payload as SubjectDetail
  showCompat.value = false
}
function onRegistered() { showRegister.value = false; load() }
function onDeleted() { router.push('/schemas') }
</script>

<template>
  <main>
    <p v-if="error && !detail" class="error">{{ error }}</p>
    <template v-if="detail">
      <div class="head-row">
        <h1>
          {{ detail.subject }}
          <span v-if="!isTopicBound" class="badge">기타</span>
        </h1>
        <div class="actions">
          <button v-if="isTopicBound" type="button" class="btn primary register-version" @click="showRegister = true">새 버전 등록</button>
          <button v-if="isAdmin" type="button" class="btn change-compat" @click="showCompat = true">호환성 변경</button>
          <button v-if="isAdmin" type="button" class="btn danger-outline delete-subject" @click="showDelete = true">삭제</button>
        </div>
      </div>
      <dl class="meta">
        <dt>토픽</dt>
        <dd><RouterLink v-if="detail.topic" :to="`/topics/${detail.topic}`">{{ detail.topic }}</RouterLink><span v-else>—</span></dd>
        <dt>종류</dt><dd>{{ detail.kind }}</dd>
        <dt>형식</dt><dd>{{ latestType || '—' }}</dd>
        <dt>호환성</dt><dd>{{ compatibilityLabel(detail) }}</dd>
      </dl>
      <p v-if="error" class="error">{{ error }}</p>

      <div class="version-row">
        <label>
          버전
          <select name="version" v-model="selected">
            <option v-for="v in detail.versions" :key="v.version" :value="String(v.version)">v{{ v.version }} (id {{ v.id }}, {{ v.schemaType }})</option>
          </select>
        </label>
        <label class="compare-toggle">
          <input type="checkbox" name="compare" v-model="compare" /> 다른 버전과 비교
        </label>
        <label v-if="compare">
          비교 대상
          <select name="compare-version" v-model="compareSelected">
            <option value="">— 선택 —</option>
            <option v-for="v in detail.versions" :key="v.version" :value="String(v.version)">v{{ v.version }}</option>
          </select>
        </label>
      </div>
      <div class="panes" :class="{ split: compare && compareVersion }">
        <pre v-if="current" class="schema">{{ formatSchema(current.schemaType, current.schema) }}</pre>
        <pre v-if="compare && compareVersion" class="schema schema-compare">{{ formatSchema(compareVersion.schemaType, compareVersion.schema) }}</pre>
      </div>
      <p v-if="current && current.references.length" class="hint">
        참조: <span v-for="r in current.references" :key="r.name">{{ r.name }} → {{ r.subject }} v{{ r.version }}; </span>
      </p>

      <SchemaRegisterModal v-if="showRegister && detail.topic && isTopicBound" :topic="detail.topic"
                           :kind="detail.kind === 'key' ? 'key' : 'value'" @close="showRegister = false" @registered="onRegistered" />
      <CompatibilityModal v-if="showCompat" :subject="detail.subject" :current="detail.compatibility"
                          :source="detail.compatibilitySource" @close="showCompat = false" @saved="onCompatSaved" />
      <SchemaDeleteModal v-if="showDelete" :subject="detail.subject" @close="showDelete = false" @deleted="onDeleted" />
    </template>
  </main>
</template>

<style scoped>
.head-row { display: flex; justify-content: space-between; align-items: center; }
.actions { display: flex; gap: 0.5rem; }
.meta { display: grid; grid-template-columns: max-content 1fr; gap: 0.3rem 1rem; margin: 0.5rem 0 1rem; }
.meta dt { color: var(--ink-soft); }
.meta dd { margin: 0; }
.badge { margin-left: 0.5rem; padding: 0.1rem 0.4rem; border-radius: 4px; font-size: 0.75rem; vertical-align: middle; background: var(--surface-2); color: var(--ink-soft); }
.version-row { display: flex; gap: 1rem; align-items: flex-end; flex-wrap: wrap; margin-bottom: 0.5rem; }
.version-row label { display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; }
.compare-toggle { flex-direction: row !important; align-items: center; gap: 0.35rem !important; }
.panes { display: grid; grid-template-columns: 1fr; gap: 0.75rem; }
.panes.split { grid-template-columns: 1fr 1fr; }
.schema { margin: 0; padding: 0.75rem; background: var(--surface-2); border-radius: 6px; overflow: auto; font-size: 0.8rem; max-height: 60vh; }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.error { color: var(--crit); }
</style>
```

- [ ] **Step 6: 라우트와 목록의 전역 호환성 버튼**

`web/src/router/index.ts` 의 `/schemas` 줄 뒤에:

```ts
    { path: '/schemas/:subject', component: () => import('@/views/SchemaSubjectView.vue') },
```

`web/src/views/SchemasView.vue`: `<slot name="global-actions" />` 를 아래로 교체하고, script 에 `import CompatibilityModal from '@/components/CompatibilityModal.vue'`, `const showGlobal = ref(false)`, `const { globalCompatibility, setStatus } = useSchemaRegistry()`(기존 구조분해 확장), `function onGlobalSaved(s: unknown) { setStatus(s as SchemaRegistryStatus); showGlobal.value = false }` (`SchemaRegistryStatus` 타입 import 추가) 를 추가한다.

```vue
        <button v-if="isAdmin" type="button" class="btn change-global" @click="showGlobal = true">전역 호환성 변경</button>
```

그리고 템플릿 끝(`SchemaRegisterModal` 줄 뒤)에:

```vue
    <CompatibilityModal v-if="showGlobal" :current="globalCompatibility ?? 'BACKWARD'" @close="showGlobal = false" @saved="onGlobalSaved" />
```

`SchemasView.spec.ts` 의 첫 테스트에 `expect(wrapper.find('button.change-global').exists()).toBe(true)` 를, DEVELOPER 테스트에 `expect(wrapper.find('button.change-global').exists()).toBe(false)` 를 추가한다.

- [ ] **Step 7: 통과 확인**

Run: `cd web && npx vitest run && npm run type-check`
Expected: 새 스펙 10건 포함 전부 통과, type-check 통과

- [ ] **Step 8: 커밋**

```bash
git add web/src/components/CompatibilityModal.vue web/src/components/SchemaDeleteModal.vue web/src/views/SchemaSubjectView.vue web/src/views/SchemasView.vue web/src/router/index.ts web/src/components/__tests__/CompatibilityModal.spec.ts web/src/components/__tests__/SchemaDeleteModal.spec.ts web/src/views/__tests__/SchemaSubjectView.spec.ts web/src/views/__tests__/SchemasView.spec.ts
git commit -m "feat(web): 스키마 상세 화면, 호환성·삭제 모달, 전역 호환성 변경"
```

---

### Task 9: 토픽 상세의 스키마 섹션

**Files:**
- Create: `web/src/components/TopicSchemaSection.vue`
- Modify: `web/src/views/TopicDetailView.vue` (섹션 삽입), `web/src/views/__tests__/TopicDetailView.spec.ts` (컴포넌트 mock 한 줄)
- Test: `web/src/components/__tests__/TopicSchemaSection.spec.ts`

**Interfaces:**
- Consumes: `useSchemaRegistry().configured`, `GET /schemas/topics/{topic}` → `TopicSchemas`, `SchemaRegisterModal`(고정 모드), `compatibilityLabel`
- Produces: `TopicSchemaSection` props `{ topic: string }`; 선택자 `section.topic-schemas`, `tr.key`, `tr.value`, `button.register-key`, `button.register-value`, `a[href="/schemas/<subject>"]`

- [ ] **Step 1: 실패하는 테스트 작성**

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const configured = ref(true)
vi.mock('@/composables/useSchemaRegistry', () => ({
  useSchemaRegistry: () => ({ configured, globalCompatibility: ref('BACKWARD'), status: ref(null), load: vi.fn(), setStatus: vi.fn() }),
}))

import { api } from '@/api/client'
import TopicSchemaSection from '../TopicSchemaSection.vue'

const stubs = { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } }
const value = { subject: 'orders-value', topic: 'orders', kind: 'value', latestVersion: 2, schemaType: 'AVRO', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL' }

describe('TopicSchemaSection', () => {
  beforeEach(() => { vi.mocked(api).mockReset(); configured.value = true })

  it('key/value 행을 보여주고 없는 쪽은 등록 버튼', async () => {
    vi.mocked(api).mockResolvedValueOnce({ topic: 'orders', key: null, value })
    const wrapper = mount(TopicSchemaSection, { props: { topic: 'orders' }, global: { stubs } })
    await flushPromises()
    expect(api).toHaveBeenCalledWith('/schemas/topics/orders')
    expect(wrapper.find('tr.value').text()).toContain('AVRO')
    expect(wrapper.find('tr.value').text()).toContain('v2')
    expect(wrapper.find('a[href="/schemas/orders-value"]').exists()).toBe(true)
    expect(wrapper.find('tr.key').text()).toContain('없음')
    expect(wrapper.find('button.register-key').exists()).toBe(true)
    expect(wrapper.find('button.register-value').exists()).toBe(true) // 새 버전 등록
  })

  it('Registry 미설정이면 섹션을 그리지 않고 호출도 하지 않는다', async () => {
    configured.value = false
    const wrapper = mount(TopicSchemaSection, { props: { topic: 'orders' }, global: { stubs } })
    await flushPromises()
    expect(wrapper.find('section.topic-schemas').exists()).toBe(false)
    expect(api).not.toHaveBeenCalled()
  })

  it('조회 실패는 섹션 안에만 표시한다', async () => {
    vi.mocked(api).mockRejectedValueOnce(new Error('Schema Registry 접속 불가'))
    const wrapper = mount(TopicSchemaSection, { props: { topic: 'orders' }, global: { stubs } })
    await flushPromises()
    expect(wrapper.find('section.topic-schemas').text()).toContain('Schema Registry 접속 불가')
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/components/__tests__/TopicSchemaSection.spec.ts`
Expected: FAIL

- [ ] **Step 3: `TopicSchemaSection.vue`**

```vue
<script setup lang="ts">
import { ref, onMounted, watch } from 'vue'
import { api } from '@/api/client'
import { useSchemaRegistry } from '@/composables/useSchemaRegistry'
import { compatibilityLabel, type SubjectSummary, type TopicSchemas } from '@/lib/schemas'
import SchemaRegisterModal from './SchemaRegisterModal.vue'

// 토픽 상세의 "스키마" 섹션. Registry 미설정이면 아무것도 그리지 않는다(호출도 안 함).
const props = defineProps<{ topic: string }>()
const { configured } = useSchemaRegistry()

const data = ref<TopicSchemas | null>(null)
const error = ref('')
const registerKind = ref<'key' | 'value' | null>(null)

async function load() {
  if (!configured.value) return
  try {
    data.value = await api<TopicSchemas>(`/schemas/topics/${props.topic}`)
    error.value = ''
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
}
onMounted(load)
watch(configured, (on) => { if (on && !data.value) load() })

function cell(s: SubjectSummary | null) {
  return s ? `${s.schemaType} · v${s.latestVersion} · ${compatibilityLabel(s)}` : '없음'
}
function onRegistered() { registerKind.value = null; load() }
</script>

<template>
  <section v-if="configured" class="topic-schemas">
    <h2>스키마</h2>
    <p v-if="error" class="error">{{ error }}</p>
    <table v-else-if="data">
      <thead><tr><th>종류</th><th>스키마</th><th></th></tr></thead>
      <tbody>
        <tr class="key">
          <td>key</td>
          <td>
            <RouterLink v-if="data.key" :to="`/schemas/${data.key.subject}`">{{ cell(data.key) }}</RouterLink>
            <span v-else>{{ cell(null) }}</span>
          </td>
          <td><button type="button" class="btn register-key" @click="registerKind = 'key'">{{ data.key ? '새 버전 등록' : '등록' }}</button></td>
        </tr>
        <tr class="value">
          <td>value</td>
          <td>
            <RouterLink v-if="data.value" :to="`/schemas/${data.value.subject}`">{{ cell(data.value) }}</RouterLink>
            <span v-else>{{ cell(null) }}</span>
          </td>
          <td><button type="button" class="btn register-value" @click="registerKind = 'value'">{{ data.value ? '새 버전 등록' : '등록' }}</button></td>
        </tr>
      </tbody>
    </table>
    <SchemaRegisterModal v-if="registerKind" :topic="topic" :kind="registerKind" @close="registerKind = null" @registered="onRegistered" />
  </section>
</template>

<style scoped>
.error { color: var(--crit); }
</style>
```

- [ ] **Step 4: `TopicDetailView.vue` 에 삽입**

script 의 import 에 `import TopicSchemaSection from '@/components/TopicSchemaSection.vue'` 추가. 템플릿에서 `<h2>설정</h2>` 블록(`</ul>` 까지) 바로 뒤에:

```vue
      <TopicSchemaSection :topic="String(route.params.name)" />
```

`web/src/views/__tests__/TopicDetailView.spec.ts` 의 다른 `vi.mock(...)` 줄들 옆에 한 줄 추가(기존 mount 호출은 손대지 않는다):

```ts
vi.mock('@/components/TopicSchemaSection.vue', () => ({ default: { name: 'TopicSchemaSection', template: '<div />' } }))
```

- [ ] **Step 5: 통과 확인**

Run: `cd web && npx vitest run && npm run type-check`
Expected: 전부 통과 (TopicDetailView.spec 포함), type-check 통과

- [ ] **Step 6: 커밋**

```bash
git add web/src/components/TopicSchemaSection.vue web/src/views/TopicDetailView.vue web/src/views/__tests__/TopicDetailView.spec.ts web/src/components/__tests__/TopicSchemaSection.spec.ts
git commit -m "feat(web): 토픽 상세에 스키마 섹션"
```

---

### Task 10: 로컬 검증 (운영 Schema Registry)

**Files:** 없음 (검증만). 실패 시 해당 Task 로 돌아가 고친다.

- [ ] **Step 1: 전체 테스트**

Run: `cd was && ./gradlew test` (Docker 필요: Kafka IT 2종 + Schema Registry IT), then `cd web && npm run build`
Expected: 둘 다 성공

- [ ] **Step 2: 로컬 설정과 기동**

`was/config/application-local.yml` 의 `app:` 아래에 추가(gitignore 대상):

```yaml
  schema-registry:
    urls: http://10.10.10.17:8081,http://10.10.10.18:8081
```

Run: `cd was && ./gradlew bootJar -x test && java -Xmx512m -Dspring.profiles.active=local -jar build/libs/kafka-admin-was-0.0.1-SNAPSHOT.jar` (터미널 1), `cd web && npm run dev` (터미널 2)

- [ ] **Step 3: API 왕복 (운영 Registry, `smoke-test` 토픽 사용)**

```bash
S=/tmp/sr-check; mkdir -p $S
curl -s -c $S/ck -H 'Content-Type: application/json' -d '{"username":"admin","password":"devpw"}' localhost:8080/api/auth/login
curl -s -b $S/ck localhost:8080/api/schemas/status; echo
curl -s -b $S/ck localhost:8080/api/schemas/subjects; echo
curl -s -b $S/ck -H 'Content-Type: application/json' -d '{"topic":"smoke-test","kind":"value","schemaType":"JSON","schema":"{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}}}"}' localhost:8080/api/schemas/compatibility; echo
curl -s -b $S/ck -H 'Content-Type: application/json' -d '{"topic":"smoke-test","kind":"value","schemaType":"JSON","schema":"{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}}}"}' -w ' [%{http_code}]\n' localhost:8080/api/schemas/register
curl -s -b $S/ck localhost:8080/api/schemas/subjects/smoke-test-value; echo
curl -s -b $S/ck localhost:8080/api/schemas/topics/smoke-test; echo
curl -s -b $S/ck -X PUT -H 'Content-Type: application/json' -d '{"compatibility":"NONE"}' localhost:8080/api/ops/schemas/subjects/smoke-test-value/config; echo
curl -s -b $S/ck -X PUT -H 'Content-Type: application/json' -d '{"compatibility":null}' localhost:8080/api/ops/schemas/subjects/smoke-test-value/config; echo
curl -s -b $S/ck -X DELETE localhost:8080/api/ops/schemas/subjects/smoke-test-value; echo
curl -s -b $S/ck 'localhost:8080/api/ops/audit-logs?size=6'
```

Expected: status `configured:true, globalCompatibility:"BACKWARD"`; 검사 `compatible:true`; 등록 201 `{subject:"smoke-test-value", version:1}`; 상세 versions 1건; 토픽별 value 채워짐; 호환성 NONE→SUBJECT, null→GLOBAL; 삭제 `deletedVersions:[1]`; 감사 로그에 `SCHEMA_REGISTER`(params 에 본문 없음), `SCHEMA_SET_COMPATIBILITY`×2, `SCHEMA_DELETE_SUBJECT`.
운영 Registry 는 soft delete 후 같은 서브젝트 재등록 시 버전이 이어질 수 있다(v2). 검증 목적이므로 무방하다.

- [ ] **Step 4: 화면 확인**

http://localhost:5173/schemas — 메뉴 "스키마"가 보이는지, 등록 모달에서 토픽 `smoke-test`·JSON 으로 검사→등록, 상세 화면 버전 본문·비교, 호환성 변경(NONE 경고), 삭제(이름 타이핑), 토픽 상세(`/topics/smoke-test`)의 스키마 섹션 링크. 확인 후 `smoke-test-value` 를 삭제해 Registry 를 원상 복구한다.

- [ ] **Step 5: 마무리**

`superpowers:finishing-a-development-branch` 로 브랜치 정리. 운영 `.env` 에 `SCHEMA_REGISTRY_URLS=http://10.10.10.17:8081,http://10.10.10.18:8081` 추가가 배포 선결 작업임을 보고에 포함한다.
