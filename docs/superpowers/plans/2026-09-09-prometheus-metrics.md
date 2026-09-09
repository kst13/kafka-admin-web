# Prometheus 지표 연동 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 운영 Prometheus(JMX exporter + kafka_exporter)의 지표를 관리자 사이트가 백엔드 프록시로 읽어 클러스터 상태 배지·브로커 상세 화면·토픽 유입 차트를 그리고, 기존 알림 체계를 7개 규칙으로 확장한다.

**Architecture:** 백엔드 `metrics` 패키지가 Spring `RestClient` 로 Prometheus HTTP API(`/api/v1/query`, `/api/v1/query_range`, `/-/healthy`)를 호출한다. PromQL 은 `MetricKey` enum 카탈로그에 고정하고 인자(브로커 instance, 토픽)는 라벨 이스케이프 후 치환한다. `ClusterHealthService` 가 클러스터 8개 + 브로커 12개 질의로 스냅샷을 조립해 15초 캐시하고, `MetricSeriesService` 가 범위별 step/rate 창으로 시계열 1개를 조회한다. `MetricsCollector` 는 Kafka 수집과 **별도 try/catch** 로 Prometheus 스냅샷을 `metric_sample` 에 저장하고 `AlertEvaluator` 가 새 규칙을 평가한다. 프론트는 `usePrometheus` 로 설정 여부를 받아 미설정이면 기존 화면을 그대로 두고, 설정 시 클러스터 배지·브로커 열·`/brokers/:id` 화면·토픽 차트 교체를 켠다.

**Tech Stack:** Spring Boot 4.1 / Spring Framework 7 (`RestClient`, `MockRestServiceServer`) / Jackson 3 / Java 21 / Testcontainers 2.0.5 (`prom/prometheus:v2.54.1` + `nginx:alpine`) / Awaitility / Vue 3.5 + TS / vue-router 5 / vitest 4

**Spec:** `docs/superpowers/specs/2026-09-09-prometheus-metrics-design.md`

## Global Constraints

- 설정 `app.prometheus.*`: `url`(`PROMETHEUS_URL`, 빈 값이면 비활성), `timeout-ms`(`PROMETHEUS_TIMEOUT_MS`, 5000, connect/read 모두), `jmx-port`(`PROMETHEUS_JMX_PORT`, 7071), `cache-seconds`(`PROMETHEUS_CACHE_SECONDS`, 15).
- 예외 → HTTP: `PrometheusNotConfiguredException` 503 "Prometheus 가 설정되지 않았습니다"; `PrometheusUnavailableException`(연결 실패·5xx) 503 "Prometheus 접속 불가"; `PrometheusQueryException`(응답 `status != "success"` 또는 4xx) 500 "Prometheus 질의 오류: <message>"; 잘못된 key/range·브로커/토픽 키 불일치 → `IllegalArgumentException` 400; 없는 브로커 id → `BrokerNotFoundException` 404 "존재하지 않는 브로커입니다: <id>".
- Prometheus 값이 `NaN`/`+Inf`/`-Inf`/파싱 불가이면 그 포인트를 **건너뛴다**(0 으로 바꾸지 않는다).
- PromQL 인자 치환 시 라벨 값의 `\` → `\\`, `"` → `\"`, 개행 → `\n` 으로 이스케이프. 브로커 인자 이름은 `broker`(값 `<host>:<jmxPort>`), 토픽 인자 이름은 `topic`.
- 범위: `1h`(창 60분, step 30s, rate 2m) / `6h`(6시간, 1m, 5m) / `24h`(24시간, 5m, 10m) / `7d`(7일, 30m, 1h). 현재값 질의의 rate 창은 `5m`.
- 브로커 집계 시리즈는 `topic=""` 필터. 핸들러·네트워크 유휴율은 `clamp_max(…, 1) * 100`. `activebrokercount`/`fencedbrokercount` 는 `max()`.
- 새 `metricType`: `OFFLINE_PARTITIONS`, `UNDER_MIN_ISR`, `UNCLEAN_ELECTIONS`(누적), `ACTIVE_BROKERS`(subjectKey `cluster`), `P99_PRODUCE_MS`, `P99_FETCH_MS`, `HANDLER_IDLE_PCT`, `HEAP_USED_PCT`(subjectKey 브로커 id 문자열). 새 `ruleType`: `OFFLINE_PARTITIONS`, `URP_HIGH`, `UNCLEAN_ELECTION`, `BROKER_DOWN`, `LATENCY_HIGH`, `HANDLER_SATURATED`, `HEAP_HIGH`, `PROMETHEUS_UNAVAILABLE`. 스키마 변경 없음.
- 임계치 `app.monitor.*`: `p99-produce-ms-threshold`(`MONITOR_P99_PRODUCE_MS`, 1000), `p99-fetch-ms-threshold`(`MONITOR_P99_FETCH_MS`, 2000), `handler-idle-min-pct`(`MONITOR_HANDLER_IDLE_MIN_PCT`, 20), `heap-used-pct-threshold`(`MONITOR_HEAP_USED_PCT`, 85).
- Prometheus 수집 실패는 Kafka 수집 결과에 영향이 없다. 별도 카운터 3회 연속 실패 시 `PROMETHEUS_UNAVAILABLE` 알림 1건(subjectKey `prometheus`), 성공 시 0 으로 초기화.
- 기존 `/api/metrics`, `/api/topics/{name}/throughput`, `PRODUCED_*`/`CONSUMED_*` 수집은 유지(폴백·소비 내역 화면).
- `SecurityConfig` 변경 없음(`/api/**` authenticated). 새 API 는 모두 조회 전용·인증만.
- Boot 4 테스트 슬라이스 import: `org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest`, `org.springframework.test.context.bean.override.mockito.MockitoBean`. Testcontainers 는 싱글턴 패턴(`static { start(); }`), `@Container` 금지. H2 테스트 DB 이름은 고유하게.
- 프론트: 외부 차트 라이브러리 금지(SVG 직접). 기존 `TrendChart` 는 남긴다(URP·디스크 추이가 사용). "예시" 데이터(`isDemo`, `demoThroughput`, `demoTrend`, `.demo-badge`)는 `TopicDetailView` 에서 완전히 제거.
- 실행 위치: git worktree(브랜치 `feature/prometheus-metrics`, base `production`). 백엔드 테스트 `cd was && ./gradlew test --tests '<FQCN>'`(IT 는 Docker 필요, 한 번에 하나의 gradle 실행, 전체 실행은 마지막 Task 에서 1회). 프론트 `cd web && npx vitest run <path>`, `npm run type-check`.
- 커밋 메시지 끝:
  ```
  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01VqsEEP7LjxWJAg8cwH1o35
  ```

## File Structure

백엔드 (`was/src/main/java/com/osstem/kafkaadmin/`):

| 파일 | 책임 |
|---|---|
| `metrics/PrometheusProperties.java`, `metrics/PrometheusConfig.java` | 설정 레코드 + 전용 `RestClient.Builder` 빈(타임아웃) |
| `metrics/PrometheusNotConfiguredException.java`, `PrometheusUnavailableException.java`, `PrometheusQueryException.java`, `BrokerNotFoundException.java` | 도메인 예외 |
| `metrics/PrometheusClient.java` | `/api/v1/query`, `/api/v1/query_range`, `/-/healthy` 호출·파싱·오류 매핑 |
| `metrics/SeriesRange.java` | 범위 enum(창·step·rate 창·파서) |
| `metrics/MetricKey.java` | PromQL 카탈로그 enum + 인자 치환·이스케이프 |
| `metrics/BrokerInstanceResolver.java` | 브로커 id ↔ `instance` 라벨 매핑 |
| `metrics/dto/MetricsDtos.java` | `ClusterHealth`, `BrokerSnapshot`, `SeriesResponse`, `Series`, `SeriesPoint`, `PrometheusStatus` |
| `metrics/ClusterHealthService.java` | 20개 질의로 스냅샷 조립 + TTL 캐시 |
| `metrics/MetricSeriesService.java` | 시계열 1개 조회 |
| `api/MetricsController.java` | 새 API 4개 |
| `api/ApiExceptionHandler.java` (수정) | 예외 매핑 추가 |
| `monitor/MonitorProperties.java`, `monitor/AlertEvaluator.java`, `monitor/MetricsCollector.java`, `monitor/MetricSampleRepository.java` (수정) | 임계치 4개, 규칙 7개, Prometheus 스냅샷 수집·실패 카운터, 직전 샘플 조회 |
| `api/MonitorController.java` (수정) | `MonitorStatus` 필드 2개 추가 |
| `src/main/resources/application.yml` (수정) | `app.prometheus.*`, `app.monitor.*` 추가 |

프론트 (`web/src/`):

| 파일 | 책임 |
|---|---|
| `lib/metrics.ts` | 타입, `SERIES_RANGES`, 포맷터, `healthBadge`, `ALERT_RULE_LABELS`, `alertLink` |
| `composables/usePrometheus.ts` | `/prometheus/status` 싱글턴 |
| `components/MetricChart.vue` | 다중 시리즈 SVG 차트 |
| `views/ClusterView.vue` (수정) | 배지·브로커 열·링크·Prometheus 수집 상태 |
| `views/BrokerDetailView.vue` (신설), `router/index.ts` (수정) | `/brokers/:id` |
| `views/TopicDetailView.vue` (수정) | Prometheus 차트/폴백, 예시 데이터 제거 |
| `views/AlertsView.vue` (수정) | 규칙 라벨·링크 |
| `App.vue`, `views/LoginView.vue` (수정) | `usePrometheus().load()` |

배포·문서: `deploy/.env.example`, `README.md`(문서 목록 1줄 + 로컬 설정 1문장).

---

### Task 1: Prometheus 설정·예외·클라이언트

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/PrometheusProperties.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/PrometheusConfig.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/PrometheusNotConfiguredException.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/PrometheusUnavailableException.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/PrometheusQueryException.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/BrokerNotFoundException.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/PrometheusClient.java`
- Modify: `was/src/main/resources/application.yml` (`app.schema-registry` 블록 아래)
- Test: `was/src/test/java/com/osstem/kafkaadmin/metrics/PrometheusClientTest.java`

**Interfaces:**
- Produces: `PrometheusProperties(String url, Integer timeoutMs, Integer jmxPort, Integer cacheSeconds)` + `configured()`, `baseUrl()`, `timeout()`, `port()`, `cacheTtl()`.
- Produces: `PrometheusClient` — `boolean configured()`, `String url()`, `boolean healthy()`, `List<InstantSample> instant(String promql)`, `List<RangeSeries> range(String promql, Instant start, Instant end, Duration step)`; 레코드 `PrometheusClient.InstantSample(Map<String,String> labels, double value)`, `PrometheusClient.RangeSeries(Map<String,String> labels, List<Point> points)`, `PrometheusClient.Point(Instant t, double v)`.
- Produces: 예외 4종(메시지는 Global Constraints 그대로). `BrokerNotFoundException(int id)`.

- [ ] **Step 1: 설정·예외 작성**

`PrometheusProperties.java`:

```java
package com.osstem.kafkaadmin.metrics;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

// app.prometheus.url 이 비어 있으면 기능 비활성. 타임아웃은 connect/read 공통.
@ConfigurationProperties(prefix = "app.prometheus")
public record PrometheusProperties(String url, Integer timeoutMs, Integer jmxPort, Integer cacheSeconds) {

    public boolean configured() { return url != null && !url.isBlank(); }

    public String baseUrl() {
        String u = url == null ? "" : url.trim();
        return u.endsWith("/") ? u.substring(0, u.length() - 1) : u;
    }

    public int timeout() { return timeoutMs == null ? 5000 : timeoutMs; }
    public int port() { return jmxPort == null ? 7071 : jmxPort; }
    public Duration cacheTtl() { return Duration.ofSeconds(cacheSeconds == null ? 15 : cacheSeconds); }
}
```

`PrometheusConfig.java`:

```java
package com.osstem.kafkaadmin.metrics;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import java.time.Duration;

@Configuration
@EnableConfigurationProperties(PrometheusProperties.class)
public class PrometheusConfig {

    // 전용 Builder 빈 — Boot 기본 RestClient.Builder 및 schemaRegistryRestClientBuilder 와 이름으로 구분
    @Bean("prometheusRestClientBuilder")
    public RestClient.Builder prometheusRestClientBuilder(PrometheusProperties props) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(props.timeout()));
        factory.setReadTimeout(Duration.ofMillis(props.timeout()));
        return RestClient.builder().requestFactory(factory);
    }
}
```

예외 4개 (각각 별 파일):

```java
package com.osstem.kafkaadmin.metrics;

public class PrometheusNotConfiguredException extends RuntimeException {
    public PrometheusNotConfiguredException() { super("Prometheus 가 설정되지 않았습니다"); }
}
```

```java
package com.osstem.kafkaadmin.metrics;

public class PrometheusUnavailableException extends RuntimeException {
    public PrometheusUnavailableException(Throwable cause) { super("Prometheus 접속 불가", cause); }
}
```

```java
package com.osstem.kafkaadmin.metrics;

// 카탈로그 질의가 거부됨 — 코드 결함이므로 500
public class PrometheusQueryException extends RuntimeException {
    public PrometheusQueryException(String message) { super("Prometheus 질의 오류: " + message); }
}
```

```java
package com.osstem.kafkaadmin.metrics;

public class BrokerNotFoundException extends RuntimeException {
    public BrokerNotFoundException(int id) { super("존재하지 않는 브로커입니다: " + id); }
}
```

`application.yml` 의 `schema-registry` 블록 바로 아래(같은 들여쓰기, `app:` 하위)에 추가:

```yaml
  prometheus:
    url: ${PROMETHEUS_URL:}
    timeout-ms: ${PROMETHEUS_TIMEOUT_MS:5000}
    jmx-port: ${PROMETHEUS_JMX_PORT:7071}
    cache-seconds: ${PROMETHEUS_CACHE_SECONDS:15}
```

- [ ] **Step 2: 실패하는 클라이언트 테스트 작성**

`PrometheusClientTest.java`:

```java
package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.metrics.PrometheusClient.InstantSample;
import com.osstem.kafkaadmin.metrics.PrometheusClient.RangeSeries;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.RequestMatcher;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;
import java.net.ConnectException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;
import static org.hamcrest.Matchers.startsWith;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

// RestClient 를 MockRestServiceServer 에 묶어 요청 형태·파싱·오류 매핑을 검증한다 (실서버는 PrometheusIT).
class PrometheusClientTest {

    private static final String BASE = "http://prom:9090";
    private MockRestServiceServer server;
    private PrometheusClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        client = new PrometheusClient(new PrometheusProperties(BASE + "/", 5000, 7071, 15), builder);
    }

    // query 파라미터는 엄격 인코딩되므로 디코딩해서 원문과 비교한다
    private static RequestMatcher queryParamDecoded(String name, String expected) {
        return request -> {
            String raw = UriComponentsBuilder.fromUri(request.getURI()).build(true).getQueryParams().getFirst(name);
            assertThat(raw).isNotNull();
            assertThat(URLDecoder.decode(raw, StandardCharsets.UTF_8)).isEqualTo(expected);
        };
    }

    @Test
    void 설정_여부와_URL을_노출한다() {
        assertThat(client.configured()).isTrue();
        assertThat(client.url()).isEqualTo(BASE);
        PrometheusClient off = new PrometheusClient(new PrometheusProperties("", null, null, null), RestClient.builder());
        assertThat(off.configured()).isFalse();
        assertThatThrownBy(() -> off.instant("up")).isInstanceOf(PrometheusNotConfiguredException.class);
        assertThat(off.healthy()).isFalse();
    }

    @Test
    void 현재값_질의를_파싱하고_NaN과_Inf는_건너뛴다() {
        String promql = "sum(kafka_server_brokertopicmetrics_bytesin_total{instance=\"10.0.0.1:7071\",topic=\"\"})";
        server.expect(requestTo(startsWith(BASE + "/api/v1/query?")))
                .andExpect(queryParamDecoded("query", promql))
                .andRespond(withSuccess("""
                        {"status":"success","data":{"resultType":"vector","result":[
                          {"metric":{"instance":"10.0.0.1:7071"},"value":[1757400000.1,"12.5"]},
                          {"metric":{"instance":"10.0.0.2:7071"},"value":[1757400000.1,"NaN"]},
                          {"metric":{"instance":"10.0.0.3:7071"},"value":[1757400000.1,"+Inf"]}
                        ]}}""", MediaType.APPLICATION_JSON));

        List<InstantSample> out = client.instant(promql);
        assertThat(out).containsExactly(new InstantSample(Map.of("instance", "10.0.0.1:7071"), 12.5));
        server.verify();
    }

    @Test
    void 범위_질의는_start_end_step을_초단위로_보내고_시리즈별_포인트를_돌려준다() {
        Instant start = Instant.ofEpochSecond(1_757_400_000L);
        Instant end = start.plusSeconds(3600);
        server.expect(requestTo(startsWith(BASE + "/api/v1/query_range?")))
                .andExpect(queryParamDecoded("query", "up"))
                .andExpect(queryParam("start", "1757400000"))
                .andExpect(queryParam("end", "1757403600"))
                .andExpect(queryParam("step", "30s"))
                .andRespond(withSuccess("""
                        {"status":"success","data":{"resultType":"matrix","result":[
                          {"metric":{"partition":"0"},"values":[[1757400000,"1"],[1757400030,"NaN"],[1757400060,"3"]]},
                          {"metric":{"partition":"1"},"values":[]}
                        ]}}""", MediaType.APPLICATION_JSON));

        List<RangeSeries> out = client.range("up", start, end, Duration.ofSeconds(30));
        assertThat(out).hasSize(2);
        assertThat(out.get(0).labels()).containsEntry("partition", "0");
        assertThat(out.get(0).points()).containsExactly(
                new PrometheusClient.Point(Instant.ofEpochSecond(1_757_400_000L), 1.0),
                new PrometheusClient.Point(Instant.ofEpochSecond(1_757_400_060L), 3.0));
        assertThat(out.get(1).points()).isEmpty();
        server.verify();
    }

    @Test
    void 응답_status가_error면_질의_예외() {
        server.expect(requestTo(startsWith(BASE + "/api/v1/query?")))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON).body(
                        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"parse error: unexpected }\"}"));
        assertThatThrownBy(() -> client.instant("sum(}"))
                .isInstanceOf(PrometheusQueryException.class)
                .hasMessageContaining("parse error");
    }

    @Test
    void 본문_status가_success가_아니면_200이어도_질의_예외() {
        server.expect(requestTo(startsWith(BASE + "/api/v1/query?")))
                .andRespond(withSuccess("{\"status\":\"error\",\"error\":\"something\"}", MediaType.APPLICATION_JSON));
        assertThatThrownBy(() -> client.instant("up")).isInstanceOf(PrometheusQueryException.class)
                .hasMessageContaining("something");
    }

    @Test
    void 연결_실패와_5xx는_접속불가_예외() {
        server.expect(requestTo(startsWith(BASE + "/api/v1/query?")))
                .andRespond(withException(new ConnectException("refused")));
        assertThatThrownBy(() -> client.instant("up")).isInstanceOf(PrometheusUnavailableException.class);

        server.expect(requestTo(startsWith(BASE + "/api/v1/query?")))
                .andRespond(withStatus(HttpStatus.BAD_GATEWAY));
        assertThatThrownBy(() -> client.instant("up")).isInstanceOf(PrometheusUnavailableException.class);
    }

    @Test
    void healthy는_200이면_true_그_외_false() {
        server.expect(requestTo(BASE + "/-/healthy")).andRespond(withSuccess("OK", MediaType.TEXT_PLAIN));
        assertThat(client.healthy()).isTrue();
        server.expect(requestTo(BASE + "/-/healthy")).andRespond(withException(new ConnectException("refused")));
        assertThat(client.healthy()).isFalse();
    }
}
```

- [ ] **Step 3: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.metrics.PrometheusClientTest'`
Expected: 컴파일 실패 (`PrometheusClient` 없음)

- [ ] **Step 4: 클라이언트 구현**

`PrometheusClient.java`:

```java
package com.osstem.kafkaadmin.metrics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import tools.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.function.Supplier;

// Prometheus HTTP API 클라이언트. PromQL 은 URI 템플릿 변수로 넘겨 엄격 인코딩한다
// (문자열에 직접 붙이면 {…} 가 URI 템플릿으로 해석되어 IllegalArgumentException 이 난다).
@Component
public class PrometheusClient {

    public record InstantSample(Map<String, String> labels, double value) {}
    public record Point(Instant t, double v) {}
    public record RangeSeries(Map<String, String> labels, List<Point> points) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record QueryResponse(String status, String errorType, String error, Data data) {}
    @JsonIgnoreProperties(ignoreUnknown = true)
    record Data(String resultType, List<Result> result) {}
    @JsonIgnoreProperties(ignoreUnknown = true)
    record Result(Map<String, String> metric, List<Object> value, List<List<Object>> values) {}

    private final boolean configured;
    private final String baseUrl;
    private final RestClient rest;
    private final ObjectMapper json = new ObjectMapper();

    public PrometheusClient(PrometheusProperties props,
                            @Qualifier("prometheusRestClientBuilder") RestClient.Builder builder) {
        this.configured = props.configured();
        this.baseUrl = props.baseUrl();
        this.rest = builder.clone().baseUrl(baseUrl).build();
    }

    public boolean configured() { return configured; }
    public String url() { return baseUrl; }

    public boolean healthy() {
        if (!configured) return false;
        try {
            rest.get().uri("/-/healthy").retrieve().toBodilessEntity();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    public List<InstantSample> instant(String promql) {
        QueryResponse r = call(() -> rest.get().uri("/api/v1/query?query={q}", promql)
                .retrieve().body(QueryResponse.class));
        List<InstantSample> out = new ArrayList<>();
        for (Result res : results(r)) {
            if (res.value() == null || res.value().size() < 2) continue;
            OptionalDouble v = parseFinite(res.value().get(1));
            if (v.isPresent()) out.add(new InstantSample(labels(res), v.getAsDouble()));
        }
        return out;
    }

    public List<RangeSeries> range(String promql, Instant start, Instant end, Duration step) {
        QueryResponse r = call(() -> rest.get()
                .uri("/api/v1/query_range?query={q}&start={s}&end={e}&step={st}",
                        promql, start.getEpochSecond(), end.getEpochSecond(), step.getSeconds() + "s")
                .retrieve().body(QueryResponse.class));
        List<RangeSeries> out = new ArrayList<>();
        for (Result res : results(r)) {
            List<Point> points = new ArrayList<>();
            for (List<Object> pair : res.values() == null ? List.<List<Object>>of() : res.values()) {
                if (pair.size() < 2) continue;
                OptionalDouble v = parseFinite(pair.get(1));
                if (v.isPresent()) points.add(new Point(toInstant(pair.get(0)), v.getAsDouble()));
            }
            out.add(new RangeSeries(labels(res), points));
        }
        return out;
    }

    private QueryResponse call(Supplier<QueryResponse> action) {
        if (!configured) throw new PrometheusNotConfiguredException();
        QueryResponse r;
        try {
            r = action.get();
        } catch (ResourceAccessException e) {
            throw new PrometheusUnavailableException(e);
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().is5xxServerError()) throw new PrometheusUnavailableException(e);
            throw new PrometheusQueryException(errorMessage(e));
        }
        if (r == null || !"success".equals(r.status())) {
            throw new PrometheusQueryException(r == null || r.error() == null ? "빈 응답" : r.error());
        }
        return r;
    }

    // 4xx 본문 {"status":"error","error":"..."} 에서 메시지를 뽑고, 실패하면 상태 코드 문자열
    private String errorMessage(RestClientResponseException e) {
        try {
            QueryResponse body = json.readValue(e.getResponseBodyAsString(), QueryResponse.class);
            if (body != null && body.error() != null) return body.error();
        } catch (RuntimeException ignored) { /* 본문이 JSON 이 아님 */ }
        return "HTTP " + e.getStatusCode().value();
    }

    private static List<Result> results(QueryResponse r) {
        return r.data() == null || r.data().result() == null ? List.of() : r.data().result();
    }

    private static Map<String, String> labels(Result res) {
        return res.metric() == null ? Map.of() : Map.copyOf(res.metric());
    }

    // "NaN", "+Inf", "-Inf", 파싱 불가 → empty (포인트 건너뜀)
    static OptionalDouble parseFinite(Object raw) {
        if (raw == null) return OptionalDouble.empty();
        try {
            double d = raw instanceof Number n ? n.doubleValue() : Double.parseDouble(raw.toString());
            return Double.isFinite(d) ? OptionalDouble.of(d) : OptionalDouble.empty();
        } catch (NumberFormatException e) {
            return OptionalDouble.empty();
        }
    }

    private static Instant toInstant(Object ts) {
        double seconds = ts instanceof Number n ? n.doubleValue() : Double.parseDouble(ts.toString());
        return Instant.ofEpochMilli(Math.round(seconds * 1000));
    }
}
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.metrics.PrometheusClientTest'`
Expected: PASS (7 tests)

주의: `queryParam("start", "1757400000")` 매처는 Spring 이 URI 를 그대로 비교하므로 숫자 템플릿 변수는 인코딩 변화가 없다. `query` 는 엄격 인코딩되므로 반드시 `queryParamDecoded` 로 비교한다.

- [ ] **Step 6: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/metrics was/src/main/resources/application.yml was/src/test/java/com/osstem/kafkaadmin/metrics/PrometheusClientTest.java
git commit -m "feat(metrics): Prometheus 설정·예외·HTTP 클라이언트"
```

---

### Task 2: 질의 카탈로그 `MetricKey`, `SeriesRange`, `BrokerInstanceResolver`

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/SeriesRange.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/MetricKey.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/BrokerInstanceResolver.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/metrics/MetricKeyTest.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/metrics/BrokerInstanceResolverTest.java`

**Interfaces:**
- Consumes: `PrometheusProperties.port()`, `ClusterQueryService.getClusterInfo()` → `ClusterInfo.brokers()` (`BrokerInfo(int id, String host, int port)`).
- Produces: `enum SeriesRange { H1, H6, H24, D7 }` — `label()`(`"1h"`…), `window()`, `step()`, `rateWindow()`(`"2m"`…), `static SeriesRange parse(String)`.
- Produces: `enum MetricKey` (키 27개) — `scope()`(`MetricKey.Scope.CLUSTER|BROKER|TOPIC`), `unit()`, `perPartition()`, `String promql(Map<String,String> args, String rateWindow)`, `String promqlCurrent(Map<String,String> args)`(rate 창 `5m`), `String promqlByInstance(String rateWindow)`(BROKER 전용, `instance` 로 그룹), `static MetricKey parse(String)`, `static String escapeLabel(String)`.
- Produces: `BrokerInstanceResolver` — `String instanceOf(int brokerId)`(없으면 `BrokerNotFoundException`), `Map<String,Integer> brokerIdByInstance()`, `List<BrokerInfo> brokers()`.

- [ ] **Step 1: 실패하는 테스트 작성**

`MetricKeyTest.java`:

```java
package com.osstem.kafkaadmin.metrics;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;

class MetricKeyTest {

    @Test
    void 클러스터_키는_인자_없이_생성된다() {
        assertThat(MetricKey.ACTIVE_CONTROLLERS.promqlCurrent(Map.of()))
                .isEqualTo("sum(kafka_controller_kafkacontroller_activecontrollercount)");
        assertThat(MetricKey.ACTIVE_BROKERS.promqlCurrent(Map.of()))
                .isEqualTo("max(kafka_controller_kafkacontroller_activebrokercount)");
        assertThat(MetricKey.UNCLEAN_ELECTIONS_1H.scope()).isEqualTo(MetricKey.Scope.CLUSTER);
    }

    @Test
    void 브로커_키는_instance_라벨과_rate_창을_치환한다() {
        String q = MetricKey.BROKER_BYTES_IN.promql(Map.of("broker", "10.0.0.1:7071"), SeriesRange.H6.rateWindow());
        assertThat(q).isEqualTo(
                "sum(rate(kafka_server_brokertopicmetrics_bytesin_total{instance=\"10.0.0.1:7071\",topic=\"\"}[5m]))");
        assertThat(MetricKey.BROKER_HANDLER_IDLE_PCT.promqlCurrent(Map.of("broker", "b:7071"))).isEqualTo(
                "clamp_max(max(kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent{instance=\"b:7071\"}), 1) * 100");
        assertThat(MetricKey.BROKER_CPU_PCT.promqlCurrent(Map.of("broker", "b:7071")))
                .isEqualTo("rate(process_cpu_seconds_total{instance=\"b:7071\"}[5m]) * 100");
        assertThat(MetricKey.BROKER_BYTES_IN.unit()).isEqualTo("bytes/s");
    }

    @Test
    void 브로커_키의_전체_질의는_instance로_그룹한다() {
        assertThat(MetricKey.BROKER_BYTES_IN.promqlByInstance("5m")).isEqualTo(
                "sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesin_total{topic=\"\"}[5m]))");
        assertThat(MetricKey.BROKER_HEAP_USED_PCT.promqlByInstance("5m")).isEqualTo(
                "max by (instance) (jvm_memory_used_bytes{area=\"heap\"}) / max by (instance) (jvm_memory_max_bytes{area=\"heap\"}) * 100");
        assertThatThrownBy(() -> MetricKey.TOPIC_BYTES_IN.promqlByInstance("5m"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 토픽_키는_topic_라벨을_치환하고_파티션별_여부를_안다() {
        assertThat(MetricKey.TOPIC_MESSAGES_IN.promql(Map.of("topic", "orders"), "2m"))
                .isEqualTo("sum(rate(kafka_server_brokertopicmetrics_messagesin_total{topic=\"orders\"}[2m]))");
        assertThat(MetricKey.TOPIC_LOG_SIZE_BY_PARTITION.promqlCurrent(Map.of("topic", "orders")))
                .isEqualTo("max by (partition) (kafka_log_log_size{topic=\"orders\"})");
        assertThat(MetricKey.TOPIC_LOG_SIZE_BY_PARTITION.perPartition()).isTrue();
        assertThat(MetricKey.TOPIC_RETAINED_BY_PARTITION.perPartition()).isTrue();
        assertThat(MetricKey.TOPIC_MESSAGES_IN.perPartition()).isFalse();
        assertThat(MetricKey.TOPIC_LOG_SIZE_TOTAL.unit()).isEqualTo("bytes");
    }

    @Test
    void 라벨_값의_따옴표_역슬래시_개행을_이스케이프한다() {
        assertThat(MetricKey.escapeLabel("a\"b\\c\nd")).isEqualTo("a\\\"b\\\\c\\nd");
        assertThat(MetricKey.TOPIC_BYTES_IN.promqlCurrent(Map.of("topic", "x\"y")))
                .contains("{topic=\"x\\\"y\"}");
    }

    @Test
    void 필요한_인자가_없거나_키_이름이_틀리면_400용_예외() {
        assertThatThrownBy(() -> MetricKey.BROKER_BYTES_IN.promqlCurrent(Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("broker");
        assertThatThrownBy(() -> MetricKey.TOPIC_BYTES_IN.promqlCurrent(Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("topic");
        assertThatThrownBy(() -> MetricKey.parse("NOPE"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("지원하지 않는 key 입니다: NOPE");
        assertThat(MetricKey.parse("BROKER_URP")).isEqualTo(MetricKey.BROKER_URP);
    }

    @Test
    void 범위별_창_step_rate창() {
        assertThat(SeriesRange.parse("1h")).isEqualTo(SeriesRange.H1);
        assertThat(SeriesRange.H1.window().toMinutes()).isEqualTo(60);
        assertThat(SeriesRange.H1.step().getSeconds()).isEqualTo(30);
        assertThat(SeriesRange.H1.rateWindow()).isEqualTo("2m");
        assertThat(SeriesRange.H6.step().toMinutes()).isEqualTo(1);
        assertThat(SeriesRange.H6.rateWindow()).isEqualTo("5m");
        assertThat(SeriesRange.H24.window().toHours()).isEqualTo(24);
        assertThat(SeriesRange.H24.step().toMinutes()).isEqualTo(5);
        assertThat(SeriesRange.H24.rateWindow()).isEqualTo("10m");
        assertThat(SeriesRange.D7.window().toDays()).isEqualTo(7);
        assertThat(SeriesRange.D7.step().toMinutes()).isEqualTo(30);
        assertThat(SeriesRange.D7.rateWindow()).isEqualTo("1h");
        assertThat(SeriesRange.D7.label()).isEqualTo("7d");
        assertThatThrownBy(() -> SeriesRange.parse("2d"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("지원하지 않는 range 입니다: 2d");
    }
}
```

`BrokerInstanceResolverTest.java`:

```java
package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BrokerInstanceResolverTest {

    private final ClusterQueryService cluster = mock(ClusterQueryService.class);
    private final BrokerInstanceResolver resolver =
            new BrokerInstanceResolver(cluster, new PrometheusProperties("http://p", null, 7071, null));

    @Test
    void 브로커_id를_host_jmxPort_instance로_바꾸고_역방향_맵을_만든다() {
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("c", 1, List.of(
                new BrokerInfo(1, "10.0.0.1", 9094), new BrokerInfo(2, "10.0.0.2", 9094))));
        assertThat(resolver.instanceOf(2)).isEqualTo("10.0.0.2:7071");
        assertThat(resolver.brokerIdByInstance())
                .containsEntry("10.0.0.1:7071", 1).containsEntry("10.0.0.2:7071", 2).hasSize(2);
        assertThat(resolver.brokers()).extracting(BrokerInfo::id).containsExactly(1, 2);
    }

    @Test
    void 없는_id는_404용_예외() {
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("c", 1, List.of(new BrokerInfo(1, "h", 9094))));
        assertThatThrownBy(() -> resolver.instanceOf(9))
                .isInstanceOf(BrokerNotFoundException.class).hasMessage("존재하지 않는 브로커입니다: 9");
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.metrics.MetricKeyTest' --tests 'com.osstem.kafkaadmin.metrics.BrokerInstanceResolverTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 구현**

`SeriesRange.java`:

```java
package com.osstem.kafkaadmin.metrics;

import java.time.Duration;

// 시계열 범위: 창·step·rate 창은 스펙 표 그대로
public enum SeriesRange {
    H1("1h", Duration.ofHours(1), Duration.ofSeconds(30), "2m"),
    H6("6h", Duration.ofHours(6), Duration.ofMinutes(1), "5m"),
    H24("24h", Duration.ofHours(24), Duration.ofMinutes(5), "10m"),
    D7("7d", Duration.ofDays(7), Duration.ofMinutes(30), "1h");

    private final String label;
    private final Duration window;
    private final Duration step;
    private final String rateWindow;

    SeriesRange(String label, Duration window, Duration step, String rateWindow) {
        this.label = label; this.window = window; this.step = step; this.rateWindow = rateWindow;
    }

    public String label() { return label; }
    public Duration window() { return window; }
    public Duration step() { return step; }
    public String rateWindow() { return rateWindow; }

    public static SeriesRange parse(String s) {
        for (SeriesRange r : values()) if (r.label.equals(s)) return r;
        throw new IllegalArgumentException("지원하지 않는 range 입니다: " + s);
    }
}
```

`MetricKey.java`:

```java
package com.osstem.kafkaadmin.metrics;

import java.util.Map;

// PromQL 카탈로그. $b = 브로커 instance 라벨 값, $t = 토픽, $r = rate 창.
// byInstance 는 브로커 키의 "전체 브로커를 instance 로 그룹" 형태 (ClusterHealthService 가 한 번에 받아 나눔).
public enum MetricKey {
    // --- 클러스터 (인자 없음) ---
    ACTIVE_CONTROLLERS(Scope.CLUSTER, "count", "sum(kafka_controller_kafkacontroller_activecontrollercount)"),
    OFFLINE_PARTITIONS(Scope.CLUSTER, "count", "sum(kafka_controller_kafkacontroller_offlinepartitionscount)"),
    UNDER_REPLICATED(Scope.CLUSTER, "count", "sum(kafka_server_replicamanager_underreplicatedpartitions)"),
    UNDER_MIN_ISR(Scope.CLUSTER, "count", "sum(kafka_server_replicamanager_underminisrpartitioncount)"),
    UNCLEAN_ELECTIONS_1H(Scope.CLUSTER, "count", "sum(increase(kafka_controller_controllerstats_uncleanleaderelections_total[1h]))"),
    UNCLEAN_ELECTIONS_TOTAL(Scope.CLUSTER, "count", "sum(kafka_controller_controllerstats_uncleanleaderelections_total)"),
    ACTIVE_BROKERS(Scope.CLUSTER, "count", "max(kafka_controller_kafkacontroller_activebrokercount)"),
    FENCED_BROKERS(Scope.CLUSTER, "count", "max(kafka_controller_kafkacontroller_fencedbrokercount)"),

    // --- 브로커 (인자 broker) ---
    BROKER_BYTES_IN(Scope.BROKER, "bytes/s",
            "sum(rate(kafka_server_brokertopicmetrics_bytesin_total{instance=\"$b\",topic=\"\"}[$r]))",
            "sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesin_total{topic=\"\"}[$r]))"),
    BROKER_BYTES_OUT(Scope.BROKER, "bytes/s",
            "sum(rate(kafka_server_brokertopicmetrics_bytesout_total{instance=\"$b\",topic=\"\"}[$r]))",
            "sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesout_total{topic=\"\"}[$r]))"),
    BROKER_MESSAGES_IN(Scope.BROKER, "msg/s",
            "sum(rate(kafka_server_brokertopicmetrics_messagesin_total{instance=\"$b\",topic=\"\"}[$r]))",
            "sum by (instance) (rate(kafka_server_brokertopicmetrics_messagesin_total{topic=\"\"}[$r]))"),
    BROKER_P99_PRODUCE_MS(Scope.BROKER, "ms",
            "max(kafka_network_requestmetrics_totaltimems{instance=\"$b\",request=\"Produce\",quantile=\"0.99\"})",
            "max by (instance) (kafka_network_requestmetrics_totaltimems{request=\"Produce\",quantile=\"0.99\"})"),
    BROKER_P99_FETCH_MS(Scope.BROKER, "ms",
            "max(kafka_network_requestmetrics_totaltimems{instance=\"$b\",request=\"FetchConsumer\",quantile=\"0.99\"})",
            "max by (instance) (kafka_network_requestmetrics_totaltimems{request=\"FetchConsumer\",quantile=\"0.99\"})"),
    BROKER_HANDLER_IDLE_PCT(Scope.BROKER, "%",
            "clamp_max(max(kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent{instance=\"$b\"}), 1) * 100",
            "clamp_max(max by (instance) (kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent), 1) * 100"),
    BROKER_NETWORK_IDLE_PCT(Scope.BROKER, "%",
            "clamp_max(max(kafka_network_socketserver_networkprocessoravgidlepercent{instance=\"$b\"}), 1) * 100",
            "clamp_max(max by (instance) (kafka_network_socketserver_networkprocessoravgidlepercent), 1) * 100"),
    BROKER_REQUEST_QUEUE(Scope.BROKER, "count",
            "max(kafka_network_requestchannel_requestqueuesize{instance=\"$b\"})",
            "max by (instance) (kafka_network_requestchannel_requestqueuesize)"),
    BROKER_HEAP_USED_PCT(Scope.BROKER, "%",
            "max(jvm_memory_used_bytes{instance=\"$b\",area=\"heap\"}) / max(jvm_memory_max_bytes{instance=\"$b\",area=\"heap\"}) * 100",
            "max by (instance) (jvm_memory_used_bytes{area=\"heap\"}) / max by (instance) (jvm_memory_max_bytes{area=\"heap\"}) * 100"),
    BROKER_GC_TIME_PCT(Scope.BROKER, "%",
            "sum(rate(jvm_gc_collection_seconds_sum{instance=\"$b\"}[$r])) * 100",
            "sum by (instance) (rate(jvm_gc_collection_seconds_sum[$r])) * 100"),
    BROKER_CPU_PCT(Scope.BROKER, "%",
            "rate(process_cpu_seconds_total{instance=\"$b\"}[$r]) * 100",
            "max by (instance) (rate(process_cpu_seconds_total[$r])) * 100"),
    BROKER_LEADER_COUNT(Scope.BROKER, "count",
            "max(kafka_server_replicamanager_leadercount{instance=\"$b\"})",
            "max by (instance) (kafka_server_replicamanager_leadercount)"),
    BROKER_PARTITION_COUNT(Scope.BROKER, "count",
            "max(kafka_server_replicamanager_partitioncount{instance=\"$b\"})",
            "max by (instance) (kafka_server_replicamanager_partitioncount)"),
    BROKER_URP(Scope.BROKER, "count",
            "max(kafka_server_replicamanager_underreplicatedpartitions{instance=\"$b\"})",
            "max by (instance) (kafka_server_replicamanager_underreplicatedpartitions)"),

    // --- 토픽 (인자 topic) ---
    TOPIC_MESSAGES_IN(Scope.TOPIC, "msg/s", "sum(rate(kafka_server_brokertopicmetrics_messagesin_total{topic=\"$t\"}[$r]))"),
    TOPIC_BYTES_IN(Scope.TOPIC, "bytes/s", "sum(rate(kafka_server_brokertopicmetrics_bytesin_total{topic=\"$t\"}[$r]))"),
    TOPIC_LOG_SIZE_BY_PARTITION(Scope.TOPIC, "bytes", "max by (partition) (kafka_log_log_size{topic=\"$t\"})", true),
    TOPIC_LOG_SIZE_TOTAL(Scope.TOPIC, "bytes", "sum(kafka_log_log_size{topic=\"$t\"})"),
    TOPIC_RETAINED_BY_PARTITION(Scope.TOPIC, "count",
            "max by (partition) (kafka_topic_partition_current_offset{topic=\"$t\"} - kafka_topic_partition_oldest_offset{topic=\"$t\"})", true);

    public enum Scope { CLUSTER, BROKER, TOPIC }

    public static final String CURRENT_RATE_WINDOW = "5m";

    private final Scope scope;
    private final String unit;
    private final String template;
    private final String byInstanceTemplate; // BROKER 전용, 그 외 null
    private final boolean perPartition;

    MetricKey(Scope scope, String unit, String template) { this(scope, unit, template, null, false); }
    MetricKey(Scope scope, String unit, String template, boolean perPartition) { this(scope, unit, template, null, perPartition); }
    MetricKey(Scope scope, String unit, String template, String byInstanceTemplate) { this(scope, unit, template, byInstanceTemplate, false); }
    MetricKey(Scope scope, String unit, String template, String byInstanceTemplate, boolean perPartition) {
        this.scope = scope; this.unit = unit; this.template = template;
        this.byInstanceTemplate = byInstanceTemplate; this.perPartition = perPartition;
    }

    public Scope scope() { return scope; }
    public String unit() { return unit; }
    public boolean perPartition() { return perPartition; }

    public String promqlCurrent(Map<String, String> args) { return promql(args, CURRENT_RATE_WINDOW); }

    public String promql(Map<String, String> args, String rateWindow) {
        String q = template;
        if (scope == Scope.BROKER) q = q.replace("$b", escapeLabel(require(args, "broker")));
        if (scope == Scope.TOPIC) q = q.replace("$t", escapeLabel(require(args, "topic")));
        return q.replace("$r", rateWindow);
    }

    public String promqlByInstance(String rateWindow) {
        if (byInstanceTemplate == null) throw new IllegalArgumentException("브로커 지표 키가 아닙니다: " + name());
        return byInstanceTemplate.replace("$r", rateWindow);
    }

    public static MetricKey parse(String s) {
        for (MetricKey k : values()) if (k.name().equals(s)) return k;
        throw new IllegalArgumentException("지원하지 않는 key 입니다: " + s);
    }

    // PromQL 라벨 매처 문자열 리터럴 이스케이프
    public static String escapeLabel(String v) {
        return v.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    private static String require(Map<String, String> args, String name) {
        String v = args.get(name);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("인자 " + name + " 가 필요합니다");
        return v;
    }
}
```

`BrokerInstanceResolver.java`:

```java
package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import org.springframework.stereotype.Component;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// 브로커 id ↔ JMX exporter instance 라벨("<host>:<jmxPort>"). host 는 AdminClient describeCluster 값.
@Component
public class BrokerInstanceResolver {

    private final ClusterQueryService cluster;
    private final int jmxPort;

    public BrokerInstanceResolver(ClusterQueryService cluster, PrometheusProperties props) {
        this.cluster = cluster;
        this.jmxPort = props.port();
    }

    public List<BrokerInfo> brokers() { return cluster.getClusterInfo().brokers(); }

    public String instanceOf(int brokerId) {
        return brokers().stream().filter(b -> b.id() == brokerId).findFirst()
                .map(this::instance).orElseThrow(() -> new BrokerNotFoundException(brokerId));
    }

    public Map<String, Integer> brokerIdByInstance() {
        Map<String, Integer> out = new LinkedHashMap<>();
        for (BrokerInfo b : brokers()) out.put(instance(b), b.id());
        return out;
    }

    public String instance(BrokerInfo b) { return b.host() + ":" + jmxPort; }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.metrics.MetricKeyTest' --tests 'com.osstem.kafkaadmin.metrics.BrokerInstanceResolverTest'`
Expected: PASS (9 tests)

- [ ] **Step 5: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/metrics was/src/test/java/com/osstem/kafkaadmin/metrics
git commit -m "feat(metrics): PromQL 카탈로그 MetricKey, SeriesRange, 브로커 instance 매핑"
```

---

### Task 3: `ClusterHealthService` (스냅샷 조립 + 캐시)

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/dto/MetricsDtos.java`
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/ClusterHealthService.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/metrics/ClusterHealthServiceTest.java`

**Interfaces:**
- Consumes: `PrometheusClient.instant(String)`, `MetricKey.promqlCurrent(Map.of())`, `MetricKey.promqlByInstance("5m")`, `BrokerInstanceResolver.brokers()/instance(BrokerInfo)`, `PrometheusProperties.cacheTtl()`.
- Produces (`metrics/dto/MetricsDtos.java`):
  ```java
  ClusterHealth(boolean configured, Instant asOf, int activeControllers, int offlinePartitions,
      int underReplicated, int underMinIsr, double uncleanElectionsLastHour, double uncleanElectionsTotal,
      int activeBrokers, int fencedBrokers, List<BrokerSnapshot> brokers)
  BrokerSnapshot(int id, String host, boolean scraped, double bytesInPerSec, double bytesOutPerSec,
      double messagesInPerSec, double p99ProduceMs, double p99FetchMs, double handlerIdlePct,
      double networkIdlePct, double heapUsedPct, double cpuPct, int leaderCount, int partitionCount,
      int underReplicated)
  SeriesPoint(Instant t, double v)
  Series(String name, List<SeriesPoint> points)
  SeriesResponse(String key, String unit, String range, long stepSeconds, List<Series> series)
  PrometheusStatus(boolean configured, String url, boolean healthy)
  ```
  스펙 대비 `ClusterHealth.uncleanElectionsTotal` 1개 추가(수집기가 `UNCLEAN_ELECTIONS` 누적 샘플을 별도 질의 없이 얻기 위함). `SeriesResponse.range` 는 라벨 문자열(`"1h"`).
- Produces: `ClusterHealthService` — `boolean configured()`, `ClusterHealth health()`(TTL 캐시), `ClusterHealth refresh()`(캐시 무시·갱신). 생성자 `(PrometheusClient, BrokerInstanceResolver, PrometheusProperties)` 와 테스트용 `(…, Clock)`.

- [ ] **Step 1: DTO 작성**

`MetricsDtos.java`:

```java
package com.osstem.kafkaadmin.metrics.dto;

import java.time.Instant;
import java.util.List;

public final class MetricsDtos {
    private MetricsDtos() {}

    public record BrokerSnapshot(int id, String host, boolean scraped,
                                 double bytesInPerSec, double bytesOutPerSec, double messagesInPerSec,
                                 double p99ProduceMs, double p99FetchMs, double handlerIdlePct, double networkIdlePct,
                                 double heapUsedPct, double cpuPct, int leaderCount, int partitionCount,
                                 int underReplicated) {}

    // configured=false 면 나머지는 0/빈 목록
    public record ClusterHealth(boolean configured, Instant asOf,
                                int activeControllers, int offlinePartitions, int underReplicated, int underMinIsr,
                                double uncleanElectionsLastHour, double uncleanElectionsTotal,
                                int activeBrokers, int fencedBrokers, List<BrokerSnapshot> brokers) {
        public static ClusterHealth notConfigured(Instant asOf) {
            return new ClusterHealth(false, asOf, 0, 0, 0, 0, 0, 0, 0, 0, List.of());
        }
    }

    public record SeriesPoint(Instant t, double v) {}
    public record Series(String name, List<SeriesPoint> points) {}
    public record SeriesResponse(String key, String unit, String range, long stepSeconds, List<Series> series) {}
    public record PrometheusStatus(boolean configured, String url, boolean healthy) {}
}
```

- [ ] **Step 2: 실패하는 테스트 작성**

`ClusterHealthServiceTest.java`:

```java
package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import com.osstem.kafkaadmin.metrics.PrometheusClient.InstantSample;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.BrokerSnapshot;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.ClusterHealth;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class ClusterHealthServiceTest {

    private final PrometheusClient client = mock(PrometheusClient.class);
    private final ClusterQueryService cluster = mock(ClusterQueryService.class);
    private final PrometheusProperties props = new PrometheusProperties("http://p", null, 7071, 15);
    private final BrokerInstanceResolver resolver = new BrokerInstanceResolver(cluster, props);
    private final MutableClock clock = new MutableClock(Instant.parse("2026-09-09T00:00:00Z"));
    private ClusterHealthService service;

    static class MutableClock extends Clock {
        Instant now;
        MutableClock(Instant now) { this.now = now; }
        @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    private static InstantSample single(double v) { return new InstantSample(Map.of(), v); }
    private static InstantSample at(String instance, double v) { return new InstantSample(Map.of("instance", instance), v); }

    @BeforeEach
    void setUp() {
        when(client.configured()).thenReturn(true);
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("c", 1, List.of(
                new BrokerInfo(1, "10.0.0.1", 9094), new BrokerInfo(2, "10.0.0.2", 9094))));
        // 기본: 모든 질의는 빈 결과 → 0
        when(client.instant(anyString())).thenReturn(List.of());
        when(client.instant(MetricKey.ACTIVE_CONTROLLERS.promqlCurrent(Map.of()))).thenReturn(List.of(single(1)));
        when(client.instant(MetricKey.UNDER_REPLICATED.promqlCurrent(Map.of()))).thenReturn(List.of(single(2)));
        when(client.instant(MetricKey.UNCLEAN_ELECTIONS_TOTAL.promqlCurrent(Map.of()))).thenReturn(List.of(single(7)));
        when(client.instant(MetricKey.ACTIVE_BROKERS.promqlCurrent(Map.of()))).thenReturn(List.of(single(2)));
        when(client.instant(MetricKey.BROKER_BYTES_IN.promqlByInstance("5m")))
                .thenReturn(List.of(at("10.0.0.1:7071", 1024.0), at("10.0.0.2:7071", 2048.0), at("unknown:7071", 9)));
        when(client.instant(MetricKey.BROKER_P99_PRODUCE_MS.promqlByInstance("5m")))
                .thenReturn(List.of(at("10.0.0.1:7071", 12.5)));
        when(client.instant(MetricKey.BROKER_HANDLER_IDLE_PCT.promqlByInstance("5m")))
                .thenReturn(List.of(at("10.0.0.1:7071", 100.0)));
        when(client.instant(MetricKey.BROKER_LEADER_COUNT.promqlByInstance("5m")))
                .thenReturn(List.of(at("10.0.0.1:7071", 10), at("10.0.0.2:7071", 11)));
        service = new ClusterHealthService(client, resolver, props, clock);
    }

    @Test
    void 클러스터_값과_브로커별_값을_한_번의_질의_묶음으로_조립한다() {
        ClusterHealth h = service.health();

        assertThat(h.configured()).isTrue();
        assertThat(h.asOf()).isEqualTo(clock.now);
        assertThat(h.activeControllers()).isEqualTo(1);
        assertThat(h.underReplicated()).isEqualTo(2);
        assertThat(h.offlinePartitions()).isZero();
        assertThat(h.uncleanElectionsTotal()).isEqualTo(7.0);
        assertThat(h.activeBrokers()).isEqualTo(2);
        assertThat(h.brokers()).extracting(BrokerSnapshot::id).containsExactly(1, 2);
        BrokerSnapshot b1 = h.brokers().get(0);
        assertThat(b1.host()).isEqualTo("10.0.0.1");
        assertThat(b1.scraped()).isTrue();
        assertThat(b1.bytesInPerSec()).isEqualTo(1024.0);
        assertThat(b1.p99ProduceMs()).isEqualTo(12.5);
        assertThat(b1.handlerIdlePct()).isEqualTo(100.0);
        assertThat(b1.leaderCount()).isEqualTo(10);
        BrokerSnapshot b2 = h.brokers().get(1);
        assertThat(b2.bytesInPerSec()).isEqualTo(2048.0);
        assertThat(b2.p99ProduceMs()).isZero(); // 시리즈 없음 → 0
        // 클러스터 8개 + 브로커 12개 = 20개 질의, 브로커 수와 무관
        verify(client, times(20)).instant(anyString());
    }

    @Test
    void Prometheus에_시리즈가_전혀_없는_브로커는_scraped_false() {
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("c", 1, List.of(
                new BrokerInfo(1, "10.0.0.1", 9094), new BrokerInfo(3, "10.0.0.3", 9094))));
        ClusterHealth h = service.health();
        assertThat(h.brokers()).extracting(BrokerSnapshot::scraped).containsExactly(true, false);
    }

    @Test
    void TTL_안에서는_캐시를_돌려주고_지나면_다시_조회하며_refresh는_항상_조회한다() {
        service.health();
        service.health();
        verify(client, times(20)).instant(anyString());

        clock.now = clock.now.plusSeconds(16);
        service.health();
        verify(client, times(40)).instant(anyString());

        service.refresh();
        verify(client, times(60)).instant(anyString());
        service.health(); // refresh 결과가 캐시에 남는다
        verify(client, times(60)).instant(anyString());
    }

    @Test
    void 미설정이면_질의_없이_configured_false() {
        when(client.configured()).thenReturn(false);
        ClusterHealth h = service.health();
        assertThat(h.configured()).isFalse();
        assertThat(h.brokers()).isEmpty();
        assertThat(service.configured()).isFalse();
        verify(client, never()).instant(anyString());
    }
}
```

- [ ] **Step 3: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.metrics.ClusterHealthServiceTest'`
Expected: 컴파일 실패

- [ ] **Step 4: 구현**

`ClusterHealthService.java`:

```java
package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.metrics.PrometheusClient.InstantSample;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.BrokerSnapshot;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.ClusterHealth;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 클러스터 8개 + 브로커 12개(instance 그룹) 질의로 스냅샷을 조립한다. 화면 요청마다 20개 질의가 나가므로
// cache-seconds 동안 단일 항목 캐시 (동시 요청은 synchronized 로 하나만 조회).
@Service
public class ClusterHealthService {

    private record Cached(ClusterHealth value, Instant expiresAt) {}

    private static final List<MetricKey> BROKER_KEYS = List.of(
            MetricKey.BROKER_BYTES_IN, MetricKey.BROKER_BYTES_OUT, MetricKey.BROKER_MESSAGES_IN,
            MetricKey.BROKER_P99_PRODUCE_MS, MetricKey.BROKER_P99_FETCH_MS,
            MetricKey.BROKER_HANDLER_IDLE_PCT, MetricKey.BROKER_NETWORK_IDLE_PCT,
            MetricKey.BROKER_HEAP_USED_PCT, MetricKey.BROKER_CPU_PCT,
            MetricKey.BROKER_LEADER_COUNT, MetricKey.BROKER_PARTITION_COUNT, MetricKey.BROKER_URP);

    private final PrometheusClient client;
    private final BrokerInstanceResolver resolver;
    private final PrometheusProperties props;
    private final Clock clock;
    private volatile Cached cache;

    @Autowired
    public ClusterHealthService(PrometheusClient client, BrokerInstanceResolver resolver, PrometheusProperties props) {
        this(client, resolver, props, Clock.systemUTC());
    }

    ClusterHealthService(PrometheusClient client, BrokerInstanceResolver resolver, PrometheusProperties props, Clock clock) {
        this.client = client;
        this.resolver = resolver;
        this.props = props;
        this.clock = clock;
    }

    public boolean configured() { return client.configured(); }

    public ClusterHealth health() {
        Cached c = cache;
        if (c != null && c.expiresAt().isAfter(clock.instant())) return c.value();
        synchronized (this) {
            c = cache;
            if (c != null && c.expiresAt().isAfter(clock.instant())) return c.value();
            return refresh();
        }
    }

    // 캐시를 무시하고 조회한 뒤 캐시를 갱신한다 (수집기용)
    public ClusterHealth refresh() {
        Instant now = clock.instant();
        ClusterHealth h = client.configured() ? load(now) : ClusterHealth.notConfigured(now);
        cache = new Cached(h, now.plus(props.cacheTtl()));
        return h;
    }

    private ClusterHealth load(Instant now) {
        List<BrokerInfo> brokers = resolver.brokers();
        Map<MetricKey, Map<String, Double>> byKey = new HashMap<>();
        for (MetricKey k : BROKER_KEYS) {
            Map<String, Double> m = new HashMap<>();
            for (InstantSample s : client.instant(k.promqlByInstance(MetricKey.CURRENT_RATE_WINDOW))) {
                String inst = s.labels().get("instance");
                if (inst != null) m.put(inst, s.value());
            }
            byKey.put(k, m);
        }
        List<BrokerSnapshot> snapshots = new ArrayList<>();
        for (BrokerInfo b : brokers) {
            String inst = resolver.instance(b);
            boolean scraped = byKey.values().stream().anyMatch(m -> m.containsKey(inst));
            snapshots.add(new BrokerSnapshot(b.id(), b.host(), scraped,
                    v(byKey, MetricKey.BROKER_BYTES_IN, inst), v(byKey, MetricKey.BROKER_BYTES_OUT, inst),
                    v(byKey, MetricKey.BROKER_MESSAGES_IN, inst),
                    v(byKey, MetricKey.BROKER_P99_PRODUCE_MS, inst), v(byKey, MetricKey.BROKER_P99_FETCH_MS, inst),
                    v(byKey, MetricKey.BROKER_HANDLER_IDLE_PCT, inst), v(byKey, MetricKey.BROKER_NETWORK_IDLE_PCT, inst),
                    v(byKey, MetricKey.BROKER_HEAP_USED_PCT, inst), v(byKey, MetricKey.BROKER_CPU_PCT, inst),
                    i(byKey, MetricKey.BROKER_LEADER_COUNT, inst), i(byKey, MetricKey.BROKER_PARTITION_COUNT, inst),
                    i(byKey, MetricKey.BROKER_URP, inst)));
        }
        return new ClusterHealth(true, now,
                (int) Math.round(scalar(MetricKey.ACTIVE_CONTROLLERS)),
                (int) Math.round(scalar(MetricKey.OFFLINE_PARTITIONS)),
                (int) Math.round(scalar(MetricKey.UNDER_REPLICATED)),
                (int) Math.round(scalar(MetricKey.UNDER_MIN_ISR)),
                scalar(MetricKey.UNCLEAN_ELECTIONS_1H),
                scalar(MetricKey.UNCLEAN_ELECTIONS_TOTAL),
                (int) Math.round(scalar(MetricKey.ACTIVE_BROKERS)),
                (int) Math.round(scalar(MetricKey.FENCED_BROKERS)),
                snapshots);
    }

    // 클러스터 집계 질의: 결과가 비면 0
    private double scalar(MetricKey key) {
        List<InstantSample> r = client.instant(key.promqlCurrent(Map.of()));
        return r.isEmpty() ? 0.0 : r.get(0).value();
    }

    private static double v(Map<MetricKey, Map<String, Double>> byKey, MetricKey k, String inst) {
        return byKey.getOrDefault(k, Map.of()).getOrDefault(inst, 0.0);
    }

    private static int i(Map<MetricKey, Map<String, Double>> byKey, MetricKey k, String inst) {
        return (int) Math.round(v(byKey, k, inst));
    }
}
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.metrics.ClusterHealthServiceTest'`
Expected: PASS (4 tests)

- [ ] **Step 6: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/metrics was/src/test/java/com/osstem/kafkaadmin/metrics/ClusterHealthServiceTest.java
git commit -m "feat(metrics): ClusterHealthService — 20개 질의 스냅샷 조립과 TTL 캐시"
```

---

### Task 4: `MetricSeriesService`

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/metrics/MetricSeriesService.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/metrics/MetricSeriesServiceTest.java`

**Interfaces:**
- Consumes: `PrometheusClient.range(promql, start, end, step)`, `MetricKey.promql(args, range.rateWindow())`, `SeriesRange`.
- Produces: `SeriesResponse series(MetricKey key, Map<String,String> args, SeriesRange range)`. 파티션별 키는 시리즈 이름 = `partition` 라벨(숫자 오름차순), 그 외 이름 = `key.name()`. 캐시 없음.

- [ ] **Step 1: 실패하는 테스트 작성**

```java
package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.metrics.PrometheusClient.Point;
import com.osstem.kafkaadmin.metrics.PrometheusClient.RangeSeries;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.Series;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.SeriesResponse;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MetricSeriesServiceTest {

    private final PrometheusClient client = mock(PrometheusClient.class);
    private final Instant now = Instant.parse("2026-09-09T12:00:00Z");
    private final MetricSeriesService service =
            new MetricSeriesService(client, Clock.fixed(now, ZoneOffset.UTC));

    @Test
    void 범위에_맞는_창_step_rate창으로_질의하고_단일_시리즈는_키_이름을_쓴다() {
        when(client.range(anyString(), any(), any(), any())).thenReturn(List.of(
                new RangeSeries(Map.of(), List.of(new Point(now.minusSeconds(60), 5.0), new Point(now, 6.0)))));

        SeriesResponse r = service.series(MetricKey.BROKER_BYTES_IN, Map.of("broker", "h:7071"), SeriesRange.H24);

        ArgumentCaptor<String> q = ArgumentCaptor.forClass(String.class);
        verify(client).range(q.capture(), eq(now.minus(Duration.ofHours(24))), eq(now), eq(Duration.ofMinutes(5)));
        assertThat(q.getValue()).contains("[10m]").contains("instance=\"h:7071\"");
        assertThat(r.key()).isEqualTo("BROKER_BYTES_IN");
        assertThat(r.unit()).isEqualTo("bytes/s");
        assertThat(r.range()).isEqualTo("24h");
        assertThat(r.stepSeconds()).isEqualTo(300);
        assertThat(r.series()).hasSize(1);
        assertThat(r.series().get(0).name()).isEqualTo("BROKER_BYTES_IN");
        assertThat(r.series().get(0).points()).extracting(p -> p.v()).containsExactly(5.0, 6.0);
    }

    @Test
    void 파티션별_키는_partition_라벨을_이름으로_쓰고_숫자순으로_정렬한다() {
        when(client.range(anyString(), any(), any(), any())).thenReturn(List.of(
                new RangeSeries(Map.of("partition", "10"), List.of(new Point(now, 1.0))),
                new RangeSeries(Map.of("partition", "2"), List.of(new Point(now, 2.0))),
                new RangeSeries(Map.of("partition", "0"), List.of())));

        SeriesResponse r = service.series(MetricKey.TOPIC_LOG_SIZE_BY_PARTITION, Map.of("topic", "orders"), SeriesRange.H1);

        assertThat(r.series()).extracting(Series::name).containsExactly("0", "2", "10");
        assertThat(r.stepSeconds()).isEqualTo(30);
        verify(client).range(anyString(), eq(now.minus(Duration.ofHours(1))), eq(now), eq(Duration.ofSeconds(30)));
    }

    @Test
    void 결과가_없으면_빈_시리즈_목록() {
        when(client.range(anyString(), any(), any(), any())).thenReturn(List.of());
        SeriesResponse r = service.series(MetricKey.TOPIC_BYTES_IN, Map.of("topic", "t"), SeriesRange.D7);
        assertThat(r.series()).isEmpty();
        assertThat(r.stepSeconds()).isEqualTo(1800);
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.metrics.MetricSeriesServiceTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 구현**

```java
package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.metrics.PrometheusClient.RangeSeries;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.Series;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.SeriesPoint;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.SeriesResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

// 시계열 1개 조회 (한 요청에 질의 1개, 캐시 없음)
@Service
public class MetricSeriesService {

    private final PrometheusClient client;
    private final Clock clock;

    @Autowired
    public MetricSeriesService(PrometheusClient client) { this(client, Clock.systemUTC()); }

    MetricSeriesService(PrometheusClient client, Clock clock) {
        this.client = client;
        this.clock = clock;
    }

    public SeriesResponse series(MetricKey key, Map<String, String> args, SeriesRange range) {
        Instant end = clock.instant();
        Instant start = end.minus(range.window());
        List<RangeSeries> raw = client.range(key.promql(args, range.rateWindow()), start, end, range.step());
        List<Series> series = raw.stream()
                .map(s -> new Series(
                        key.perPartition() ? s.labels().getOrDefault("partition", "?") : key.name(),
                        s.points().stream().map(p -> new SeriesPoint(p.t(), p.v())).toList()))
                .sorted(Comparator.comparing(Series::name, MetricSeriesService::compareNames))
                .toList();
        return new SeriesResponse(key.name(), key.unit(), range.label(), range.step().getSeconds(), series);
    }

    // 파티션 번호는 숫자순, 숫자가 아니면 문자열순
    private static int compareNames(String a, String b) {
        try {
            return Integer.compare(Integer.parseInt(a), Integer.parseInt(b));
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.metrics.MetricSeriesServiceTest'`
Expected: PASS (3 tests)

- [ ] **Step 5: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/metrics/MetricSeriesService.java was/src/test/java/com/osstem/kafkaadmin/metrics/MetricSeriesServiceTest.java
git commit -m "feat(metrics): MetricSeriesService — 범위별 시계열 조회"
```

---

### Task 5: API 4개 + 예외 매핑 + 배포 문서

**Files:**
- Create: `was/src/main/java/com/osstem/kafkaadmin/api/MetricsController.java`
- Modify: `was/src/main/java/com/osstem/kafkaadmin/api/ApiExceptionHandler.java` (파일 끝, 마지막 `}` 앞)
- Modify: `deploy/.env.example` (Schema Registry 블록 아래)
- Modify: `README.md` (문서 목록의 Schema Registry 줄 아래 1줄, 로컬 개발 문단 끝 1문장)
- Test: `was/src/test/java/com/osstem/kafkaadmin/api/MetricsControllerTest.java`

**Interfaces:**
- Consumes: `PrometheusClient.configured()/url()/healthy()`, `ClusterHealthService.health()`, `MetricSeriesService.series(...)`, `BrokerInstanceResolver.instanceOf(int)`, `MetricKey.parse/scope`, `SeriesRange.parse`, DTO 들.
- Produces: `GET /api/prometheus/status` → `PrometheusStatus`; `GET /api/cluster/health` → `ClusterHealth`; `GET /api/brokers/{id}/series?key=&range=` → `SeriesResponse`(`BROKER_*` 만); `GET /api/topics/{name}/series?key=&range=` → `SeriesResponse`(`TOPIC_*` 만).

- [ ] **Step 1: 실패하는 슬라이스 테스트 작성**

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.config.SecurityConfig;
import com.osstem.kafkaadmin.metrics.*;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(MetricsController.class)
@Import(SecurityConfig.class) // 비로그인 401 검증용 (SchemaControllerTest 와 같은 방식)
class MetricsControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean PrometheusClient client;
    @MockitoBean ClusterHealthService healthService;
    @MockitoBean MetricSeriesService seriesService;
    @MockitoBean BrokerInstanceResolver resolver;

    private static final Instant T = Instant.parse("2026-09-09T00:00:00Z");

    @Test
    @WithMockUser
    void 상태와_클러스터_건강을_조회한다() throws Exception {
        given(client.configured()).willReturn(true);
        given(client.url()).willReturn("http://prom:9090");
        given(client.healthy()).willReturn(true);
        mvc.perform(get("/api/prometheus/status")).andExpect(status().isOk())
                .andExpect(jsonPath("$.configured").value(true))
                .andExpect(jsonPath("$.url").value("http://prom:9090"))
                .andExpect(jsonPath("$.healthy").value(true));

        given(healthService.health()).willReturn(new ClusterHealth(true, T, 1, 0, 2, 0, 0, 5, 3, 0, List.of(
                new BrokerSnapshot(1, "10.0.0.1", true, 1024, 2048, 10, 12.5, 30, 95, 99, 40, 7, 10, 30, 0))));
        mvc.perform(get("/api/cluster/health")).andExpect(status().isOk())
                .andExpect(jsonPath("$.activeControllers").value(1))
                .andExpect(jsonPath("$.brokers[0].id").value(1))
                .andExpect(jsonPath("$.brokers[0].bytesInPerSec").value(1024.0));
    }

    @Test
    @WithMockUser
    void 미설정이면_상태는_false이고_건강은_200_configured_false() throws Exception {
        given(client.configured()).willReturn(false);
        given(client.url()).willReturn("");
        given(client.healthy()).willReturn(false);
        given(healthService.health()).willReturn(ClusterHealth.notConfigured(T));
        mvc.perform(get("/api/prometheus/status")).andExpect(status().isOk())
                .andExpect(jsonPath("$.configured").value(false)).andExpect(jsonPath("$.healthy").value(false));
        mvc.perform(get("/api/cluster/health")).andExpect(status().isOk())
                .andExpect(jsonPath("$.configured").value(false)).andExpect(jsonPath("$.brokers").isEmpty());
    }

    @Test
    @WithMockUser
    void 브로커_시계열은_instance를_풀어_서비스에_넘긴다() throws Exception {
        given(resolver.instanceOf(2)).willReturn("10.0.0.2:7071");
        given(seriesService.series(eq(MetricKey.BROKER_BYTES_IN), eq(Map.of("broker", "10.0.0.2:7071")), eq(SeriesRange.H6)))
                .willReturn(new SeriesResponse("BROKER_BYTES_IN", "bytes/s", "6h", 60,
                        List.of(new Series("BROKER_BYTES_IN", List.of(new SeriesPoint(T, 1.5))))));
        mvc.perform(get("/api/brokers/2/series?key=BROKER_BYTES_IN&range=6h")).andExpect(status().isOk())
                .andExpect(jsonPath("$.unit").value("bytes/s"))
                .andExpect(jsonPath("$.stepSeconds").value(60))
                .andExpect(jsonPath("$.series[0].points[0].v").value(1.5));
    }

    @Test
    @WithMockUser
    void 토픽_시계열은_topic_인자로_넘긴다() throws Exception {
        given(seriesService.series(eq(MetricKey.TOPIC_MESSAGES_IN), eq(Map.of("topic", "orders")), eq(SeriesRange.H1)))
                .willReturn(new SeriesResponse("TOPIC_MESSAGES_IN", "msg/s", "1h", 30, List.of()));
        mvc.perform(get("/api/topics/orders/series?key=TOPIC_MESSAGES_IN&range=1h")).andExpect(status().isOk())
                .andExpect(jsonPath("$.key").value("TOPIC_MESSAGES_IN"));
    }

    @Test
    @WithMockUser
    void 잘못된_key_range와_종류_불일치는_400_없는_브로커는_404() throws Exception {
        given(resolver.instanceOf(anyInt())).willReturn("h:7071");
        mvc.perform(get("/api/brokers/1/series?key=NOPE&range=1h")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("지원하지 않는 key 입니다: NOPE"));
        mvc.perform(get("/api/brokers/1/series?key=BROKER_BYTES_IN&range=2d")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("지원하지 않는 range 입니다: 2d"));
        mvc.perform(get("/api/topics/t/series?key=BROKER_BYTES_IN&range=1h")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("토픽 지표 키가 아닙니다: BROKER_BYTES_IN"));
        mvc.perform(get("/api/brokers/1/series?key=TOPIC_BYTES_IN&range=1h")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("브로커 지표 키가 아닙니다: TOPIC_BYTES_IN"));
        willThrow(new BrokerNotFoundException(9)).given(resolver).instanceOf(9);
        mvc.perform(get("/api/brokers/9/series?key=BROKER_BYTES_IN&range=1h")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("존재하지 않는 브로커입니다: 9"));
    }

    @Test
    @WithMockUser
    void Prometheus_예외는_503_500으로_매핑된다() throws Exception {
        // 재스텁 시 given(mock.call()) 은 이전 스텁의 예외를 실제로 던지므로 willThrow(...).given(mock) 형태를 쓴다
        willThrow(new PrometheusNotConfiguredException()).given(seriesService).series(any(), any(), any());
        mvc.perform(get("/api/topics/t/series?key=TOPIC_BYTES_IN&range=1h")).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("Prometheus 가 설정되지 않았습니다"));
        willThrow(new PrometheusUnavailableException(new RuntimeException("x"))).given(healthService).health();
        mvc.perform(get("/api/cluster/health")).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("Prometheus 접속 불가"));
        willThrow(new PrometheusQueryException("parse error")).given(seriesService).series(any(), any(), any());
        mvc.perform(get("/api/topics/t/series?key=TOPIC_BYTES_IN&range=1h")).andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Prometheus 질의 오류: parse error"));
    }

    @Test
    void 비로그인은_401() throws Exception {
        mvc.perform(get("/api/prometheus/status")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/cluster/health")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/brokers/1/series?key=BROKER_BYTES_IN&range=1h")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/topics/t/series?key=TOPIC_BYTES_IN&range=1h")).andExpect(status().isUnauthorized());
    }
}
```

- [ ] **Step 2: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.api.MetricsControllerTest'`
Expected: 컴파일 실패

- [ ] **Step 3: 컨트롤러·예외 매핑 구현**

`MetricsController.java`:

```java
package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.metrics.BrokerInstanceResolver;
import com.osstem.kafkaadmin.metrics.ClusterHealthService;
import com.osstem.kafkaadmin.metrics.MetricKey;
import com.osstem.kafkaadmin.metrics.MetricSeriesService;
import com.osstem.kafkaadmin.metrics.PrometheusClient;
import com.osstem.kafkaadmin.metrics.SeriesRange;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.ClusterHealth;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.PrometheusStatus;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.SeriesResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

// Prometheus 기반 조회 API. 모두 인증만 요구(조회 전용).
@RestController
@RequestMapping("/api")
public class MetricsController {

    private final PrometheusClient client;
    private final ClusterHealthService health;
    private final MetricSeriesService series;
    private final BrokerInstanceResolver resolver;

    public MetricsController(PrometheusClient client, ClusterHealthService health,
                             MetricSeriesService series, BrokerInstanceResolver resolver) {
        this.client = client;
        this.health = health;
        this.series = series;
        this.resolver = resolver;
    }

    @GetMapping("/prometheus/status")
    public PrometheusStatus status() {
        return new PrometheusStatus(client.configured(), client.url(), client.healthy());
    }

    @GetMapping("/cluster/health")
    public ClusterHealth clusterHealth() { return health.health(); }

    @GetMapping("/brokers/{id}/series")
    public SeriesResponse brokerSeries(@PathVariable int id, @RequestParam String key, @RequestParam String range) {
        MetricKey k = MetricKey.parse(key);
        if (k.scope() != MetricKey.Scope.BROKER) throw new IllegalArgumentException("브로커 지표 키가 아닙니다: " + key);
        SeriesRange r = SeriesRange.parse(range);
        return series.series(k, Map.of("broker", resolver.instanceOf(id)), r);
    }

    @GetMapping("/topics/{name}/series")
    public SeriesResponse topicSeries(@PathVariable String name, @RequestParam String key, @RequestParam String range) {
        MetricKey k = MetricKey.parse(key);
        if (k.scope() != MetricKey.Scope.TOPIC) throw new IllegalArgumentException("토픽 지표 키가 아닙니다: " + key);
        SeriesRange r = SeriesRange.parse(range);
        return series.series(k, Map.of("topic", name), r);
    }
}
```

`ApiExceptionHandler.java` — import 4개 추가 후 클래스 끝(마지막 `}` 앞)에 추가:

```java
import com.osstem.kafkaadmin.metrics.BrokerNotFoundException;
import com.osstem.kafkaadmin.metrics.PrometheusNotConfiguredException;
import com.osstem.kafkaadmin.metrics.PrometheusQueryException;
import com.osstem.kafkaadmin.metrics.PrometheusUnavailableException;
```

```java
    // --- Prometheus ---
    @ExceptionHandler({PrometheusNotConfiguredException.class, PrometheusUnavailableException.class})
    public ResponseEntity<Map<String, String>> prometheusUnavailable(RuntimeException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", e.getMessage()));
    }

    // 카탈로그 질의가 거부된 것은 코드 결함 → 500
    @ExceptionHandler(PrometheusQueryException.class)
    public ResponseEntity<Map<String, String>> prometheusQuery(PrometheusQueryException e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(BrokerNotFoundException.class)
    public ResponseEntity<Map<String, String>> brokerNotFound(BrokerNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
    }
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.api.MetricsControllerTest'`
Expected: PASS (7 tests)

- [ ] **Step 5: 배포·문서 갱신**

`deploy/.env.example` — Schema Registry 블록 아래에 추가:

```
# --- Prometheus (선택): 비워두면 클러스터 상태 배지·브로커 상세·토픽 유입 차트(Prometheus)를 숨긴다 ---
# PROMETHEUS_URL=http://10.0.0.13:9090
# PROMETHEUS_TIMEOUT_MS=5000
# PROMETHEUS_JMX_PORT=7071
# PROMETHEUS_CACHE_SECONDS=15
```

같은 파일의 감시 블록 끝(`# MONITOR_RETENTION_DAYS=7` 아래)에 추가:

```
# MONITOR_P99_PRODUCE_MS=1000
# MONITOR_P99_FETCH_MS=2000
# MONITOR_HANDLER_IDLE_MIN_PCT=20
# MONITOR_HEAP_USED_PCT=85
```

`README.md` 문서 목록의 Schema Registry 줄 바로 아래에 추가:

```
- [Prometheus 지표 연동 설계](docs/superpowers/specs/2026-09-09-prometheus-metrics-design.md) — 클러스터 상태 배지·브로커 상세(`/brokers/:id`)·토픽 유입 차트·알림 규칙 7개. 설정: `PROMETHEUS_URL`
```

`README.md` 로컬 개발 문단(Schema Registry 문장) 끝에 문장 추가:

```
Prometheus 지표를 보려면 같은 파일에 `app.prometheus.url: http://10.10.10.19:9090` 을 적는다(비우면 관련 배지·화면이 숨겨진다).
```

- [ ] **Step 6: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/api/MetricsController.java was/src/main/java/com/osstem/kafkaadmin/api/ApiExceptionHandler.java was/src/test/java/com/osstem/kafkaadmin/api/MetricsControllerTest.java deploy/.env.example README.md
git commit -m "feat(api): Prometheus 상태·클러스터 건강·브로커/토픽 시계열 API"
```

---

### Task 6: 알림 규칙 7개 (`MonitorProperties`, `AlertEvaluator`, 직전 샘플 조회)

**Files:**
- Modify: `was/src/main/java/com/osstem/kafkaadmin/monitor/MonitorProperties.java`
- Modify: `was/src/main/java/com/osstem/kafkaadmin/monitor/MetricSampleRepository.java`
- Modify: `was/src/main/java/com/osstem/kafkaadmin/monitor/AlertEvaluator.java`
- Modify: `was/src/main/java/com/osstem/kafkaadmin/monitor/MetricSample.java:6` (주석), `monitor/AlertEvent.java:6` (주석)
- Modify: `was/src/main/resources/application.yml` (`app.monitor` 블록)
- Modify: `was/src/test/java/com/osstem/kafkaadmin/monitor/MonitorSchedulerStartupCheckTest.java:16` (생성자 인자)
- Test: `was/src/test/java/com/osstem/kafkaadmin/monitor/AlertEvaluatorTest.java` (확장)

**Interfaces:**
- Produces: `MonitorProperties(boolean enabled, long lagThreshold, int diskUsedPctThreshold, int certWarnDays, int cooldownMinutes, int retentionDays, int p99ProduceMsThreshold, int p99FetchMsThreshold, int handlerIdleMinPct, int heapUsedPctThreshold)`.
- Produces: `MetricSampleRepository.findTopByMetricTypeAndSubjectKeyAndSampledAtBeforeOrderBySampledAtDesc(String, String, Instant)` → `Optional<MetricSample>`.
- Produces: `AlertEvaluator` 생성자 `(AlertEventRepository, AlertNotifier, MonitorProperties, MetricSampleRepository)`; `evaluate(List<MetricSample>)` 가 새 규칙 7개 처리. 규칙별 메시지(아래 구현 그대로).

- [ ] **Step 1: 설정 확장**

`MonitorProperties.java`:

```java
package com.osstem.kafkaadmin.monitor;

import org.springframework.boot.context.properties.ConfigurationProperties;

// 임계치는 DB가 아닌 설정으로 관리한다(2단계 YAGNI). env로 재정의 가능.
@ConfigurationProperties(prefix = "app.monitor")
public record MonitorProperties(
        boolean enabled,
        long lagThreshold,
        int diskUsedPctThreshold,
        int certWarnDays,
        int cooldownMinutes,
        int retentionDays,
        int p99ProduceMsThreshold,
        int p99FetchMsThreshold,
        int handlerIdleMinPct,
        int heapUsedPctThreshold) {
}
```

`application.yml` 의 `app.monitor` 블록 끝(`retention-days` 아래)에 추가:

```yaml
    p99-produce-ms-threshold: ${MONITOR_P99_PRODUCE_MS:1000}
    p99-fetch-ms-threshold: ${MONITOR_P99_FETCH_MS:2000}
    handler-idle-min-pct: ${MONITOR_HANDLER_IDLE_MIN_PCT:20}
    heap-used-pct-threshold: ${MONITOR_HEAP_USED_PCT:85}
```

`MonitorSchedulerStartupCheckTest.java:16` 을 `new MonitorProperties(true, 1000, 80, 30, 30, 7, 1000, 2000, 20, 85)` 로 바꾼다. 다른 `new MonitorProperties(` 사용처는 없다(`grep -rn "new MonitorProperties(" was/src` 로 확인).

`MetricSampleRepository.java` 에 메서드 추가(import `java.util.Optional`):

```java
    // 같은 type/subject 의 직전 샘플(이번 배치 이전) — 증가/감소 판정용
    Optional<MetricSample> findTopByMetricTypeAndSubjectKeyAndSampledAtBeforeOrderBySampledAtDesc(
            String metricType, String subjectKey, Instant before);
```

`MetricSample.java:6` 주석을 다음으로 교체:

```java
// 지표 이력 1건. metricType: LAG | CONSUMED_TOTAL | CONSUMED_TOPIC | PRODUCED_PARTITION | PRODUCED_TOPIC | URP | DISK_USED_PCT | BROKER_COUNT
//   Prometheus 스냅샷: OFFLINE_PARTITIONS | UNDER_MIN_ISR | UNCLEAN_ELECTIONS(누적) | ACTIVE_BROKERS (subjectKey cluster)
//                     P99_PRODUCE_MS | P99_FETCH_MS | HANDLER_IDLE_PCT | HEAP_USED_PCT (subjectKey 브로커 id)
```

`AlertEvent.java:6` 주석을 다음으로 교체:

```java
// 알림 이력 1건. ruleType: LAG_HIGH | DISK_HIGH | CERT_EXPIRY | COLLECTOR_FAILURE
//   | OFFLINE_PARTITIONS | URP_HIGH | UNCLEAN_ELECTION | BROKER_DOWN | LATENCY_HIGH | HANDLER_SATURATED | HEAP_HIGH | PROMETHEUS_UNAVAILABLE
```

- [ ] **Step 2: 실패하는 테스트 추가**

`AlertEvaluatorTest.java` 에 `@Autowired MetricSampleRepository samples;` 필드와 아래 테스트를 추가한다(기존 테스트 2개 유지). import 에 `java.time.temporal.ChronoUnit` 추가.

```java
    private List<String> ruleTypes() {
        return alerts.findTop50ByOrderByOccurredAtDesc().stream().map(AlertEvent::getRuleType).toList();
    }

    @Test
    void 오프라인_파티션과_URP는_0보다_크면_알림() {
        Instant now = Instant.now();
        evaluator.evaluate(List.of(
                new MetricSample("OFFLINE_PARTITIONS", "cluster", 2, now),
                new MetricSample("URP", "cluster", 1, now),
                new MetricSample("UNDER_MIN_ISR", "cluster", 3, now))); // 이력만, 규칙 없음
        assertThat(ruleTypes()).containsExactlyInAnyOrder("OFFLINE_PARTITIONS", "URP_HIGH");
        assertThat(alerts.findTop50ByOrderByOccurredAtDesc())
                .filteredOn(a -> a.getRuleType().equals("OFFLINE_PARTITIONS"))
                .first().satisfies(a -> {
                    assertThat(a.getMessage()).isEqualTo("오프라인 파티션 2개");
                    assertThat(a.getSubjectKey()).isEqualTo("cluster");
                });

        evaluator.evaluate(List.of(
                new MetricSample("OFFLINE_PARTITIONS", "cluster", 0, now),
                new MetricSample("URP", "cluster", 0, now)));
        assertThat(ruleTypes()).hasSize(2); // 0 은 알림 없음
    }

    @Test
    void 언클린_선출은_직전_샘플보다_커질_때_브로커_감소는_직전보다_작아질_때만_알림() {
        Instant t0 = Instant.now().minus(2, ChronoUnit.MINUTES);
        Instant t1 = Instant.now().minus(1, ChronoUnit.MINUTES);
        Instant t2 = Instant.now();

        // 첫 샘플: 직전 없음 → 판정 안 함
        samples.save(new MetricSample("UNCLEAN_ELECTIONS", "cluster", 5, t0));
        samples.save(new MetricSample("ACTIVE_BROKERS", "cluster", 3, t0));
        evaluator.evaluate(List.of(
                new MetricSample("UNCLEAN_ELECTIONS", "cluster", 5, t0),
                new MetricSample("ACTIVE_BROKERS", "cluster", 3, t0)));
        assertThat(ruleTypes()).isEmpty();

        // 변화 없음 → 알림 없음
        samples.save(new MetricSample("UNCLEAN_ELECTIONS", "cluster", 5, t1));
        samples.save(new MetricSample("ACTIVE_BROKERS", "cluster", 3, t1));
        evaluator.evaluate(List.of(
                new MetricSample("UNCLEAN_ELECTIONS", "cluster", 5, t1),
                new MetricSample("ACTIVE_BROKERS", "cluster", 3, t1)));
        assertThat(ruleTypes()).isEmpty();

        // 증가 / 감소
        samples.save(new MetricSample("UNCLEAN_ELECTIONS", "cluster", 7, t2));
        samples.save(new MetricSample("ACTIVE_BROKERS", "cluster", 2, t2));
        evaluator.evaluate(List.of(
                new MetricSample("UNCLEAN_ELECTIONS", "cluster", 7, t2),
                new MetricSample("ACTIVE_BROKERS", "cluster", 2, t2)));
        assertThat(ruleTypes()).containsExactlyInAnyOrder("UNCLEAN_ELECTION", "BROKER_DOWN");
        assertThat(alerts.findTop50ByOrderByOccurredAtDesc())
                .filteredOn(a -> a.getRuleType().equals("UNCLEAN_ELECTION")).first()
                .satisfies(a -> {
                    assertThat(a.getMessage()).isEqualTo("언클린 리더 선출 2회 발생 (누적 7)");
                    assertThat(a.getValue()).isEqualTo(2.0);
                });
        assertThat(alerts.findTop50ByOrderByOccurredAtDesc())
                .filteredOn(a -> a.getRuleType().equals("BROKER_DOWN")).first()
                .satisfies(a -> assertThat(a.getMessage()).isEqualTo("활성 브로커 수 감소 3 → 2"));
    }

    @Test
    void 브로커_지연_핸들러_힙_규칙은_경계값을_넘을_때만_알림이고_subjectKey는_브로커_id() {
        Instant now = Instant.now();
        // 기본 임계치: produce 1000, fetch 2000, 핸들러 최소 20, 힙 85
        evaluator.evaluate(List.of(
                new MetricSample("P99_PRODUCE_MS", "1", 1000, now),   // 경계 — 알림 없음
                new MetricSample("P99_PRODUCE_MS", "2", 1000.5, now), // 초과
                new MetricSample("P99_FETCH_MS", "1", 2500, now),     // 초과
                new MetricSample("HANDLER_IDLE_PCT", "1", 20, now),   // 경계 — 알림 없음
                new MetricSample("HANDLER_IDLE_PCT", "3", 19.9, now), // 미달
                new MetricSample("HEAP_USED_PCT", "1", 85, now),      // 경계 — 알림 없음
                new MetricSample("HEAP_USED_PCT", "2", 90, now)));    // 초과
        List<AlertEvent> saved = alerts.findTop50ByOrderByOccurredAtDesc();
        assertThat(saved).extracting(AlertEvent::getRuleType, AlertEvent::getSubjectKey)
                .containsExactlyInAnyOrder(
                        tuple("LATENCY_HIGH", "2"), tuple("LATENCY_HIGH", "1"),
                        tuple("HANDLER_SATURATED", "3"), tuple("HEAP_HIGH", "2"));
        assertThat(saved).filteredOn(a -> a.getRuleType().equals("LATENCY_HIGH") && a.getSubjectKey().equals("1"))
                .first().satisfies(a -> assertThat(a.getMessage()).isEqualTo("브로커 1 Fetch p99 2500ms (임계치 2000ms)"));
        assertThat(saved).filteredOn(a -> a.getRuleType().equals("HANDLER_SATURATED"))
                .first().satisfies(a -> assertThat(a.getMessage()).isEqualTo("브로커 3 요청 핸들러 유휴율 19.9% (최소 20%)"));
        assertThat(saved).filteredOn(a -> a.getRuleType().equals("HEAP_HIGH"))
                .first().satisfies(a -> assertThat(a.getMessage()).isEqualTo("브로커 2 힙 사용률 90.0% (임계치 85%)"));
    }
```

`import static org.assertj.core.api.Assertions.tuple;` 도 추가한다.

- [ ] **Step 3: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.monitor.AlertEvaluatorTest'`
Expected: 새 테스트 3개 FAIL (알림 미발생)

- [ ] **Step 4: 규칙 구현**

`AlertEvaluator.java` 전체:

```java
package com.osstem.kafkaadmin.monitor;

import org.springframework.stereotype.Service;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

@Service
public class AlertEvaluator {

    private final AlertEventRepository alerts;
    private final AlertNotifier notifier;
    private final MonitorProperties props;
    private final MetricSampleRepository samples;

    public AlertEvaluator(AlertEventRepository alerts, AlertNotifier notifier,
                          MonitorProperties props, MetricSampleRepository samples) {
        this.alerts = alerts;
        this.notifier = notifier;
        this.props = props;
        this.samples = samples;
    }

    public void evaluate(List<MetricSample> batch) {
        for (MetricSample s : batch) {
            double v = s.getValue();
            String subject = s.getSubjectKey();
            switch (s.getMetricType()) {
                case "LAG" -> {
                    if (v > props.lagThreshold()) {
                        raise("LAG_HIGH", subject,
                                "컨슈머 그룹 %s 랙 %.0f (임계치 %d)".formatted(subject, v, props.lagThreshold()),
                                v, props.lagThreshold());
                    }
                }
                case "DISK_USED_PCT" -> {
                    if (v > props.diskUsedPctThreshold()) {
                        raise("DISK_HIGH", subject,
                                "브로커 %s 디스크 사용률 %.1f%% (임계치 %d%%)".formatted(subject, v, props.diskUsedPctThreshold()),
                                v, props.diskUsedPctThreshold());
                    }
                }
                // --- 클러스터 (Prometheus 스냅샷 + 기존 URP) ---
                case "OFFLINE_PARTITIONS" -> {
                    if (v > 0) raise("OFFLINE_PARTITIONS", subject, "오프라인 파티션 %.0f개".formatted(v), v, 0);
                }
                case "URP" -> {
                    if (v > 0) raise("URP_HIGH", subject, "미복제 파티션(URP) %.0f개".formatted(v), v, 0);
                }
                case "UNCLEAN_ELECTIONS" -> previous(s).ifPresent(prev -> {
                    if (v > prev) {
                        raise("UNCLEAN_ELECTION", subject,
                                "언클린 리더 선출 %.0f회 발생 (누적 %.0f)".formatted(v - prev, v), v - prev, prev);
                    }
                });
                case "ACTIVE_BROKERS" -> previous(s).ifPresent(prev -> {
                    if (v < prev) {
                        raise("BROKER_DOWN", subject,
                                "활성 브로커 수 감소 %.0f → %.0f".formatted(prev, v), v, prev);
                    }
                });
                // --- 브로커 (subjectKey = 브로커 id) ---
                case "P99_PRODUCE_MS" -> {
                    if (v > props.p99ProduceMsThreshold()) {
                        raise("LATENCY_HIGH", subject,
                                "브로커 %s Produce p99 %.0fms (임계치 %dms)".formatted(subject, v, props.p99ProduceMsThreshold()),
                                v, props.p99ProduceMsThreshold());
                    }
                }
                case "P99_FETCH_MS" -> {
                    if (v > props.p99FetchMsThreshold()) {
                        raise("LATENCY_HIGH", subject,
                                "브로커 %s Fetch p99 %.0fms (임계치 %dms)".formatted(subject, v, props.p99FetchMsThreshold()),
                                v, props.p99FetchMsThreshold());
                    }
                }
                case "HANDLER_IDLE_PCT" -> {
                    if (v < props.handlerIdleMinPct()) {
                        raise("HANDLER_SATURATED", subject,
                                "브로커 %s 요청 핸들러 유휴율 %.1f%% (최소 %d%%)".formatted(subject, v, props.handlerIdleMinPct()),
                                v, props.handlerIdleMinPct());
                    }
                }
                case "HEAP_USED_PCT" -> {
                    if (v > props.heapUsedPctThreshold()) {
                        raise("HEAP_HIGH", subject,
                                "브로커 %s 힙 사용률 %.1f%% (임계치 %d%%)".formatted(subject, v, props.heapUsedPctThreshold()),
                                v, props.heapUsedPctThreshold());
                    }
                }
                default -> { /* BROKER_COUNT, UNDER_MIN_ISR, CONSUMED_*, PRODUCED_* 는 이력만 */ }
            }
        }
    }

    // 직전 저장 샘플(이번 샘플 시각보다 앞선 것) — 첫 샘플이면 empty 라 증감 판정을 하지 않는다
    private Optional<Double> previous(MetricSample s) {
        return samples.findTopByMetricTypeAndSubjectKeyAndSampledAtBeforeOrderBySampledAtDesc(
                        s.getMetricType(), s.getSubjectKey(), s.getSampledAt())
                .map(MetricSample::getValue);
    }

    // 쿨다운: 동일 (ruleType, subjectKey) 알림이 cooldownMinutes 내에 있으면 중복 발생 억제
    public void raise(String ruleType, String subjectKey, String message,
                      double value, double threshold) {
        Instant cutoff = Instant.now().minus(props.cooldownMinutes(), ChronoUnit.MINUTES);
        if (alerts.existsByRuleTypeAndSubjectKeyAndOccurredAtAfter(ruleType, subjectKey, cutoff)) {
            return;
        }
        AlertEvent event = alerts.save(
                new AlertEvent(ruleType, subjectKey, message, value, threshold, Instant.now()));
        notifier.send(event);
    }
}
```

주의: `MetricsCollector` 는 `samples.saveAll(batch)` 후 `evaluate(batch)` 를 호출하므로 이번 샘플도 DB 에 있다. `SampledAtBefore`(strictly before) 로 이번 샘플을 제외한다 — 그래서 테스트도 평가 전에 같은 시각으로 저장한다.

- [ ] **Step 5: 테스트 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.monitor.AlertEvaluatorTest' --tests 'com.osstem.kafkaadmin.monitor.MonitorSchedulerStartupCheckTest'`
Expected: PASS

- [ ] **Step 6: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/monitor was/src/main/resources/application.yml was/src/test/java/com/osstem/kafkaadmin/monitor/AlertEvaluatorTest.java was/src/test/java/com/osstem/kafkaadmin/monitor/MonitorSchedulerStartupCheckTest.java
git commit -m "feat(monitor): 알림 규칙 7개 추가 (오프라인·URP·언클린·브로커 감소·지연·핸들러·힙)"
```

---

### Task 7: 수집기 Prometheus 스냅샷 + `MonitorStatus` 확장

**Files:**
- Modify: `was/src/main/java/com/osstem/kafkaadmin/monitor/MetricsCollector.java`
- Modify: `was/src/main/java/com/osstem/kafkaadmin/api/MonitorController.java` (`MonitorStatus` 레코드, `status()`)
- Modify: `was/src/test/java/com/osstem/kafkaadmin/monitor/MetricsCollectorFailureTest.java:22`, `MetricsCollectorConsumedTotalTest.java:30`, `MetricsCollectorProducedTest.java:28` (생성자)
- Modify: `was/src/test/java/com/osstem/kafkaadmin/api/MonitorControllerTest.java` (status 검증 2줄)
- Test: `was/src/test/java/com/osstem/kafkaadmin/monitor/MetricsCollectorPrometheusTest.java`

**Interfaces:**
- Consumes: `ClusterHealthService.configured()/refresh()`, `ClusterHealth`, `BrokerSnapshot`.
- Produces: `MetricsCollector` 생성자 `(GroupQueryService, MonitorQueryService, ClusterQueryService, MetricSampleRepository, AlertEvaluator, ClusterHealthService)`; `Instant prometheusLastSuccessAt()`, `int prometheusConsecutiveFailures()`.
- Produces: `MonitorController.MonitorStatus(Instant lastCollectedAt, int consecutiveFailures, List<CertStatus> certs, Instant prometheusLastCollectedAt, int prometheusConsecutiveFailures)`.

- [ ] **Step 1: 기존 테스트 3개의 생성자 갱신**

세 파일 모두 필드 추가 + 생성자 인자 추가:

```java
    private final ClusterHealthService health = mock(ClusterHealthService.class);
    private final MetricsCollector collector =
            new MetricsCollector(groups, monitorQuery, cluster, samples, evaluator, health);
```

import `com.osstem.kafkaadmin.metrics.ClusterHealthService;`. mock 의 `configured()` 는 기본 false 이므로 Prometheus 단계는 건너뛴다.

- [ ] **Step 2: 실패하는 테스트 작성**

`MetricsCollectorPrometheusTest.java`:

```java
package com.osstem.kafkaadmin.monitor;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.MonitorQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import com.osstem.kafkaadmin.metrics.ClusterHealthService;
import com.osstem.kafkaadmin.metrics.PrometheusUnavailableException;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.BrokerSnapshot;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.ClusterHealth;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// Prometheus 스냅샷 수집은 Kafka 수집과 독립: 실패해도 Kafka 배치는 저장되고, 별도 카운터로 알림한다.
class MetricsCollectorPrometheusTest {

    private final GroupQueryService groups = mock(GroupQueryService.class);
    private final MonitorQueryService monitorQuery = mock(MonitorQueryService.class);
    private final ClusterQueryService cluster = mock(ClusterQueryService.class);
    private final MetricSampleRepository samples = mock(MetricSampleRepository.class);
    private final AlertEvaluator evaluator = mock(AlertEvaluator.class);
    private final ClusterHealthService health = mock(ClusterHealthService.class);
    private final MetricsCollector collector =
            new MetricsCollector(groups, monitorQuery, cluster, samples, evaluator, health);

    private static ClusterHealth snapshot() {
        return new ClusterHealth(true, Instant.now(), 1, 2, 0, 3, 0, 7, 3, 0, List.of(
                new BrokerSnapshot(1, "h1", true, 0, 0, 0, 12.5, 30, 95, 99, 40, 5, 10, 30, 0),
                new BrokerSnapshot(2, "h2", false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));
    }

    @BeforeEach
    void kafkaOk() {
        when(groups.listGroups()).thenReturn(List.of());
        when(monitorQuery.latestOffsetsByTopicPartition()).thenReturn(Map.of());
        when(monitorQuery.countUnderReplicatedPartitions()).thenReturn(0);
        when(monitorQuery.diskUsedPercentByBroker()).thenReturn(Map.of());
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("c", 1, List.of()));
        when(health.configured()).thenReturn(true);
    }

    @Test
    @SuppressWarnings("unchecked")
    void 스냅샷을_같은_시각의_샘플_8종으로_저장하고_평가한다() {
        when(health.refresh()).thenReturn(snapshot());

        collector.collectOnce();

        ArgumentCaptor<List<MetricSample>> captor = ArgumentCaptor.forClass(List.class);
        verify(samples, times(2)).saveAll(captor.capture()); // Kafka 배치 + Prometheus 배치
        List<MetricSample> prom = captor.getAllValues().get(1);
        assertThat(prom).extracting(MetricSample::getMetricType, MetricSample::getSubjectKey, MetricSample::getValue)
                .containsExactlyInAnyOrder(
                        tuple("OFFLINE_PARTITIONS", "cluster", 2.0),
                        tuple("UNDER_MIN_ISR", "cluster", 3.0),
                        tuple("UNCLEAN_ELECTIONS", "cluster", 7.0),
                        tuple("ACTIVE_BROKERS", "cluster", 3.0),
                        tuple("P99_PRODUCE_MS", "1", 12.5),
                        tuple("P99_FETCH_MS", "1", 30.0),
                        tuple("HANDLER_IDLE_PCT", "1", 95.0),
                        tuple("HEAP_USED_PCT", "1", 40.0)); // scraped=false 브로커 2 는 제외
        assertThat(prom).extracting(MetricSample::getSampledAt).containsOnly(prom.get(0).getSampledAt());
        verify(evaluator).evaluate(prom);
        assertThat(collector.prometheusLastSuccessAt()).isNotNull();
        assertThat(collector.prometheusConsecutiveFailures()).isZero();
        assertThat(collector.consecutiveFailures()).isZero();
    }

    @Test
    void Prometheus_실패는_Kafka_배치를_깨지_않고_3회_연속이면_한_번만_알림_성공하면_초기화() {
        when(health.refresh()).thenThrow(new PrometheusUnavailableException(new RuntimeException("down")));

        for (int i = 0; i < 5; i++) collector.collectOnce();

        verify(samples, times(5)).saveAll(any()); // Kafka 배치는 매번 저장
        assertThat(collector.lastSuccessAt()).isNotNull();
        assertThat(collector.consecutiveFailures()).isZero();
        assertThat(collector.prometheusConsecutiveFailures()).isEqualTo(5);
        assertThat(collector.prometheusLastSuccessAt()).isNull();
        verify(evaluator, times(1)).raise(eq("PROMETHEUS_UNAVAILABLE"), eq("prometheus"), anyString(), anyDouble(), anyDouble());
        verify(evaluator, never()).raise(eq("COLLECTOR_FAILURE"), anyString(), anyString(), anyDouble(), anyDouble());

        reset(health);
        when(health.configured()).thenReturn(true);
        when(health.refresh()).thenReturn(snapshot());
        collector.collectOnce();
        assertThat(collector.prometheusConsecutiveFailures()).isZero();
        assertThat(collector.prometheusLastSuccessAt()).isNotNull();
    }

    @Test
    void 미설정이면_Prometheus_단계를_건너뛴다() {
        when(health.configured()).thenReturn(false);
        collector.collectOnce();
        verify(health, never()).refresh();
        verify(samples, times(1)).saveAll(any());
        assertThat(collector.prometheusLastSuccessAt()).isNull();
    }
}
```

- [ ] **Step 3: 실패 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.monitor.MetricsCollectorPrometheusTest'`
Expected: 컴파일 실패

- [ ] **Step 4: 수집기 구현**

`MetricsCollector.java` — import 추가:

```java
import com.osstem.kafkaadmin.metrics.ClusterHealthService;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.BrokerSnapshot;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.ClusterHealth;
```

필드·생성자를 다음으로 교체:

```java
    private static final int FAILURE_ALERT_AT = 3;

    private final GroupQueryService groups;
    private final MonitorQueryService monitorQuery;
    private final ClusterQueryService cluster;
    private final MetricSampleRepository samples;
    private final AlertEvaluator evaluator;
    private final ClusterHealthService health;
    private final AtomicInteger failures = new AtomicInteger();
    private final AtomicReference<Instant> lastSuccess = new AtomicReference<>();
    private final AtomicInteger prometheusFailures = new AtomicInteger();
    private final AtomicReference<Instant> prometheusLastSuccess = new AtomicReference<>();

    public MetricsCollector(GroupQueryService groups, MonitorQueryService monitorQuery,
                            ClusterQueryService cluster, MetricSampleRepository samples,
                            AlertEvaluator evaluator, ClusterHealthService health) {
        this.groups = groups;
        this.monitorQuery = monitorQuery;
        this.cluster = cluster;
        this.samples = samples;
        this.evaluator = evaluator;
        this.health = health;
    }
```

`collectOnce()` 는 기존 본문을 `collectKafka()` 로 옮기고 다음처럼 바꾼다:

```java
    public void collectOnce() {
        collectKafka();
        collectPrometheus();
    }

    // 기존 본문 그대로 (try { … } catch (RuntimeException e) { … COLLECTOR_FAILURE … })
    private void collectKafka() {
        try {
            … 기존 collectOnce 의 try 블록 …
        } catch (RuntimeException e) {
            … 기존 catch 블록 …
        }
    }

    // Prometheus 스냅샷 — Kafka 수집과 독립된 try/catch, 별도 실패 카운터 (스펙 "수집" 절)
    private void collectPrometheus() {
        if (!health.configured()) return;
        try {
            Instant now = Instant.now();
            ClusterHealth h = health.refresh();
            List<MetricSample> batch = new ArrayList<>();
            batch.add(new MetricSample("OFFLINE_PARTITIONS", "cluster", h.offlinePartitions(), now));
            batch.add(new MetricSample("UNDER_MIN_ISR", "cluster", h.underMinIsr(), now));
            batch.add(new MetricSample("UNCLEAN_ELECTIONS", "cluster", h.uncleanElectionsTotal(), now));
            batch.add(new MetricSample("ACTIVE_BROKERS", "cluster", h.activeBrokers(), now));
            for (BrokerSnapshot b : h.brokers()) {
                if (!b.scraped()) continue; // 시리즈 없는 브로커의 0 은 거짓 알림(핸들러 0%)을 만든다
                String id = String.valueOf(b.id());
                batch.add(new MetricSample("P99_PRODUCE_MS", id, b.p99ProduceMs(), now));
                batch.add(new MetricSample("P99_FETCH_MS", id, b.p99FetchMs(), now));
                batch.add(new MetricSample("HANDLER_IDLE_PCT", id, b.handlerIdlePct(), now));
                batch.add(new MetricSample("HEAP_USED_PCT", id, b.heapUsedPct(), now));
            }
            samples.saveAll(batch);
            evaluator.evaluate(batch);
            prometheusFailures.set(0);
            prometheusLastSuccess.set(now);
        } catch (RuntimeException e) {
            int count = prometheusFailures.incrementAndGet();
            log.warn("Prometheus 지표 수집 실패 ({}회 연속): {}", count, e.getMessage());
            if (count == FAILURE_ALERT_AT) {
                evaluator.raise("PROMETHEUS_UNAVAILABLE", "prometheus",
                        "Prometheus 지표 수집이 %d회 연속 실패: %s".formatted(count, e.getMessage()),
                        count, FAILURE_ALERT_AT);
            }
        }
    }

    public Instant lastSuccessAt() { return lastSuccess.get(); }
    public int consecutiveFailures() { return failures.get(); }
    public Instant prometheusLastSuccessAt() { return prometheusLastSuccess.get(); }
    public int prometheusConsecutiveFailures() { return prometheusFailures.get(); }
```

`MonitorController.java`:

```java
    public record MonitorStatus(Instant lastCollectedAt, int consecutiveFailures,
                                List<CertStatus> certs,
                                Instant prometheusLastCollectedAt, int prometheusConsecutiveFailures) {}
```

```java
    @GetMapping("/monitor/status")
    public MonitorStatus status() {
        return new MonitorStatus(collector.lastSuccessAt(), collector.consecutiveFailures(),
                certChecker.lastStatuses(),
                collector.prometheusLastSuccessAt(), collector.prometheusConsecutiveFailures());
    }
```

`MonitorControllerTest.알림_이력과_모니터_상태를_조회한다` 에 stub 과 검증 추가:

```java
        given(collector.prometheusLastSuccessAt()).willReturn(Instant.parse("2026-08-07T01:00:30Z"));
        given(collector.prometheusConsecutiveFailures()).willReturn(2);
```

```java
                .andExpect(jsonPath("$.prometheusLastCollectedAt").value("2026-08-07T01:00:30Z"))
                .andExpect(jsonPath("$.prometheusConsecutiveFailures").value(2));
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.monitor.MetricsCollector*Test' --tests 'com.osstem.kafkaadmin.api.MonitorControllerTest'`
Expected: PASS (Prometheus 3 + Failure 1 + Consumed 1+ + Produced 1+ + Controller 4)

- [ ] **Step 6: 커밋**

```bash
git add was/src/main/java/com/osstem/kafkaadmin/monitor/MetricsCollector.java was/src/main/java/com/osstem/kafkaadmin/api/MonitorController.java was/src/test/java/com/osstem/kafkaadmin/monitor was/src/test/java/com/osstem/kafkaadmin/api/MonitorControllerTest.java
git commit -m "feat(monitor): 수집기가 Prometheus 스냅샷을 저장·평가하고 실패 카운터를 상태에 노출"
```

---

### Task 8: Prometheus 통합 테스트 (Testcontainers)

**Files:**
- Test: `was/src/test/java/com/osstem/kafkaadmin/metrics/PrometheusIntegrationTestBase.java`
- Test: `was/src/test/java/com/osstem/kafkaadmin/metrics/PrometheusIT.java`

**Interfaces:**
- Consumes: `PrometheusClient`, `ClusterHealthService.refresh()`, `MetricSeriesService.series(...)`, `ClusterQueryService`(MockitoBean 으로 브로커 3대 고정).

구성: `nginx:alpine` 이 `/b1.txt`, `/b2.txt`, `/b3.txt` 세 개의 exposition 텍스트를 서빙하고, `prom/prometheus:v2.54.1` 이 같은 네트워크에서 `__metrics_path__` 와 `instance` 라벨을 타깃별로 지정해 5초 간격으로 스크랩한다. Kafka 컨테이너는 필요 없다(`ClusterQueryService` 를 mock).

- [ ] **Step 1: 베이스 클래스 작성**

```java
package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.List;
import static org.mockito.Mockito.when;

// 정적 metrics 서버(nginx) + Prometheus 2.54.1 을 같은 네트워크에 띄운다. 싱글턴 패턴(@Container 금지).
// 브로커 3대는 타깃별 instance 라벨로 흉내 낸다 (10.0.0.1~3:7071). Kafka 는 필요 없어 ClusterQueryService 를 mock.
@SpringBootTest
public abstract class PrometheusIntegrationTestBase {

    static final Network NET = Network.newNetwork();

    // 브로커별로 다른 값: p99 produce = 10/20/30, heap used = 40/50/60 %, leader = 5/6/7, controller 는 b1 만
    static String exposition(int n) {
        return """
                kafka_controller_kafkacontroller_activecontrollercount %d
                kafka_controller_kafkacontroller_offlinepartitionscount 0
                kafka_controller_kafkacontroller_activebrokercount 3
                kafka_controller_kafkacontroller_fencedbrokercount 0
                kafka_controller_controllerstats_uncleanleaderelections_total 4
                kafka_server_replicamanager_underreplicatedpartitions 0
                kafka_server_replicamanager_underminisrpartitioncount 0
                kafka_server_replicamanager_leadercount %d
                kafka_server_replicamanager_partitioncount 21
                kafka_server_brokertopicmetrics_bytesin_total{topic=""} 1000
                kafka_server_brokertopicmetrics_bytesout_total{topic=""} 2000
                kafka_server_brokertopicmetrics_messagesin_total{topic=""} 300
                kafka_server_brokertopicmetrics_bytesin_total{topic="orders"} 500
                kafka_server_brokertopicmetrics_messagesin_total{topic="orders"} 100
                kafka_network_requestmetrics_totaltimems{request="Produce",quantile="0.99"} %d
                kafka_network_requestmetrics_totaltimems{request="FetchConsumer",quantile="0.99"} 55
                kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent 2.0
                kafka_network_socketserver_networkprocessoravgidlepercent 0.9
                kafka_network_requestchannel_requestqueuesize 1
                jvm_memory_used_bytes{area="heap"} %d
                jvm_memory_max_bytes{area="heap"} 1000
                jvm_gc_collection_seconds_sum 12.5
                process_cpu_seconds_total 100
                kafka_log_log_size{topic="orders",partition="0"} 1024
                kafka_log_log_size{topic="orders",partition="1"} 2048
                kafka_topic_partition_current_offset{topic="orders",partition="0"} 150
                kafka_topic_partition_oldest_offset{topic="orders",partition="0"} 50
                kafka_topic_partition_current_offset{topic="orders",partition="1"} 80
                kafka_topic_partition_oldest_offset{topic="orders",partition="1"} 0
                """.formatted(n == 1 ? 1 : 0, 4 + n, 10 * n, 300 + 100 * n);
    }

    static final String PROM_CONFIG = """
            global:
              scrape_interval: 5s
            scrape_configs:
              - job_name: kafka-broker
                static_configs:
                  - targets: ['metrics-server:80']
                    labels: { instance: '10.0.0.1:7071', __metrics_path__: '/b1.txt' }
                  - targets: ['metrics-server:80']
                    labels: { instance: '10.0.0.2:7071', __metrics_path__: '/b2.txt' }
                  - targets: ['metrics-server:80']
                    labels: { instance: '10.0.0.3:7071', __metrics_path__: '/b3.txt' }
            """;

    protected static final GenericContainer<?> METRICS =
            new GenericContainer<>(DockerImageName.parse("nginx:alpine"))
                    .withNetwork(NET).withNetworkAliases("metrics-server")
                    .withCopyToContainer(Transferable.of(exposition(1)), "/usr/share/nginx/html/b1.txt")
                    .withCopyToContainer(Transferable.of(exposition(2)), "/usr/share/nginx/html/b2.txt")
                    .withCopyToContainer(Transferable.of(exposition(3)), "/usr/share/nginx/html/b3.txt")
                    .withExposedPorts(80);

    protected static final GenericContainer<?> PROMETHEUS =
            new GenericContainer<>(DockerImageName.parse("prom/prometheus:v2.54.1"))
                    .withNetwork(NET)
                    .withCopyToContainer(Transferable.of(PROM_CONFIG), "/etc/prometheus/prometheus.yml")
                    .withExposedPorts(9090)
                    .waitingFor(Wait.forHttp("/-/ready").forPort(9090).forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(2)));

    static {
        METRICS.start();
        PROMETHEUS.start();
    }

    protected static String prometheusUrl() {
        return "http://" + PROMETHEUS.getHost() + ":" + PROMETHEUS.getMappedPort(9090);
    }

    @MockitoBean ClusterQueryService cluster;

    @BeforeEach
    void threeBrokers() {
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("it", 1, List.of(
                new BrokerInfo(1, "10.0.0.1", 9094), new BrokerInfo(2, "10.0.0.2", 9094), new BrokerInfo(3, "10.0.0.3", 9094))));
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("app.prometheus.url", PrometheusIntegrationTestBase::prometheusUrl);
        registry.add("app.prometheus.timeout-ms", () -> "5000");
        registry.add("app.prometheus.jmx-port", () -> "7071");
        registry.add("app.prometheus.cache-seconds", () -> "1");
        registry.add("app.monitor.enabled", () -> "false");
        registry.add("spring.datasource.url", () -> "jdbc:h2:mem:prometheus-it;DB_CLOSE_DELAY=-1");
    }
}
```

- [ ] **Step 2: IT 작성**

```java
package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.BrokerSnapshot;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.ClusterHealth;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.Series;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.SeriesResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Duration;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.awaitility.Awaitility.await;

class PrometheusIT extends PrometheusIntegrationTestBase {

    @Autowired PrometheusClient client;
    @Autowired ClusterHealthService healthService;
    @Autowired MetricSeriesService seriesService;

    // 첫 스크랩(5초 간격)과 rate 창(5m 안에 샘플 2개)을 기다린다
    @BeforeEach
    void waitForScrapes() {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(client.instant("count(up{job=\"kafka-broker\"} == 1)")).isNotEmpty()
                        .first().satisfies(s -> assertThat(s.value()).isEqualTo(3.0)));
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(client.instant("min(count_over_time(up{job=\"kafka-broker\"}[5m]))")).isNotEmpty()
                        .first().satisfies(s -> assertThat(s.value()).isGreaterThanOrEqualTo(2.0)));
    }

    @Test
    void healthy_와_클러스터_건강_스냅샷을_실제_Prometheus에서_조립한다() {
        assertThat(client.healthy()).isTrue();

        ClusterHealth h = healthService.refresh();
        assertThat(h.configured()).isTrue();
        assertThat(h.activeControllers()).isEqualTo(1);
        assertThat(h.offlinePartitions()).isZero();
        assertThat(h.activeBrokers()).isEqualTo(3);
        assertThat(h.uncleanElectionsTotal()).isEqualTo(12.0); // 3대 합
        assertThat(h.brokers()).extracting(BrokerSnapshot::id).containsExactly(1, 2, 3);
        assertThat(h.brokers()).allMatch(BrokerSnapshot::scraped);
        assertThat(h.brokers()).extracting(BrokerSnapshot::p99ProduceMs).containsExactly(10.0, 20.0, 30.0);
        assertThat(h.brokers()).extracting(BrokerSnapshot::heapUsedPct).containsExactly(40.0, 50.0, 60.0);
        assertThat(h.brokers()).extracting(BrokerSnapshot::leaderCount).containsExactly(5, 6, 7);
        assertThat(h.brokers().get(0).handlerIdlePct()).isCloseTo(100.0, within(0.01)); // 2.0 → clamp → 100%
        assertThat(h.brokers().get(0).networkIdlePct()).isCloseTo(90.0, within(0.01));
        assertThat(h.brokers().get(0).bytesInPerSec()).isZero(); // 고정 카운터의 rate 는 0
    }

    @Test
    void 시계열_범위_4종과_파티션별_시리즈를_왕복한다() {
        Map<String, String> broker = Map.of("broker", "10.0.0.2:7071");
        for (SeriesRange r : SeriesRange.values()) {
            SeriesResponse s = seriesService.series(MetricKey.BROKER_P99_PRODUCE_MS, broker, r);
            assertThat(s.range()).isEqualTo(r.label());
            assertThat(s.stepSeconds()).isEqualTo(r.step().getSeconds());
            assertThat(s.series()).hasSize(1);
            assertThat(s.series().get(0).name()).isEqualTo("BROKER_P99_PRODUCE_MS");
            assertThat(s.series().get(0).points()).isNotEmpty().allMatch(p -> p.v() == 20.0);
        }
        SeriesResponse parts = seriesService.series(MetricKey.TOPIC_LOG_SIZE_BY_PARTITION, Map.of("topic", "orders"), SeriesRange.H1);
        assertThat(parts.unit()).isEqualTo("bytes");
        assertThat(parts.series()).extracting(Series::name).containsExactly("0", "1");
        assertThat(parts.series().get(1).points()).isNotEmpty().allMatch(p -> p.v() == 2048.0);

        SeriesResponse retained = seriesService.series(MetricKey.TOPIC_RETAINED_BY_PARTITION, Map.of("topic", "orders"), SeriesRange.H1);
        assertThat(retained.series()).extracting(Series::name).containsExactly("0", "1");
        assertThat(retained.series().get(0).points()).allMatch(p -> p.v() == 100.0);

        SeriesResponse none = seriesService.series(MetricKey.TOPIC_BYTES_IN, Map.of("topic", "ghost"), SeriesRange.H1);
        assertThat(none.series()).isEmpty();
    }
}
```

- [ ] **Step 3: 실행 (Docker 필요)**

Run: `cd was && ./gradlew test --tests 'com.osstem.kafkaadmin.metrics.PrometheusIT'`
Expected: PASS (2 tests). 첫 실행은 이미지 pull 로 1~2분 걸릴 수 있다. `nginx` 의 `.txt` 는 `text/plain` 으로 서빙되어 Prometheus 텍스트 파서가 받는다. 실패 시 `docker logs <prometheus 컨테이너>` 에서 타깃 오류를 확인한다.

- [ ] **Step 4: 커밋**

```bash
git add was/src/test/java/com/osstem/kafkaadmin/metrics/PrometheusIntegrationTestBase.java was/src/test/java/com/osstem/kafkaadmin/metrics/PrometheusIT.java
git commit -m "test(metrics): Prometheus + 정적 metrics 서버 통합 테스트"
```

---

### Task 9: 프론트 `lib/metrics.ts` + `usePrometheus` + 진입 시 로드

**Files:**
- Create: `web/src/lib/metrics.ts`
- Create: `web/src/composables/usePrometheus.ts`
- Modify: `web/src/App.vue:1-11` (script), `web/src/views/LoginView.vue:1-27` (script)
- Modify: `web/src/views/__tests__/LoginView.spec.ts` (mock 1줄)
- Test: `web/src/lib/__tests__/metrics.spec.ts`, `web/src/composables/__tests__/usePrometheus.spec.ts`

**Interfaces:**
- Produces (`lib/metrics.ts`):
  ```ts
  export interface BrokerSnapshot { id; host; scraped; bytesInPerSec; bytesOutPerSec; messagesInPerSec; p99ProduceMs; p99FetchMs; handlerIdlePct; networkIdlePct; heapUsedPct; cpuPct; leaderCount; partitionCount; underReplicated }
  export interface ClusterHealth { configured; asOf; activeControllers; offlinePartitions; underReplicated; underMinIsr; uncleanElectionsLastHour; uncleanElectionsTotal; activeBrokers; fencedBrokers; brokers: BrokerSnapshot[] }
  export interface Point { t: string; v: number }
  export interface Series { name: string; points: Point[] }
  export interface SeriesResponse { key; unit; range; stepSeconds; series: Series[] }
  export interface PrometheusStatus { configured: boolean; url: string; healthy: boolean }
  export type SeriesRange = '1h' | '6h' | '24h' | '7d'
  export const SERIES_RANGES: { value: SeriesRange; label: string }[]
  export type BadgeLevel = 'ok' | 'warn' | 'crit'
  export interface Badge { label: string; level: BadgeLevel }
  export function healthBadge(h: ClusterHealth, brokerCount: number): Badge[]   // 6개
  export function formatBytesPerSec(v: number): string   // "1.5 MB/s" / "512 KB/s" / "12 B/s"
  export function formatBytes(v: number): string         // "1.0 GB" / "2.0 KB"
  export function formatMs(v: number): string            // "12 ms" / "1.2 s"
  export function formatPct(v: number): string           // "95.0%"
  export function formatCount(v: number): string         // "1,234"
  export function formatValue(v: number, unit: string): string   // unit 별 분기
  export const ALERT_RULE_LABELS: Record<string, { label: string; description: string }>
  export function alertLink(ruleType: string, subjectKey: string): string | null
  ```
- Produces (`usePrometheus`): `{ status, configured, healthy, load, setStatus }` — `useSchemaRegistry` 와 같은 모듈 스코프 싱글턴, `GET /prometheus/status`.

- [ ] **Step 1: 실패하는 테스트 작성**

`web/src/lib/__tests__/metrics.spec.ts`:

```ts
import { describe, it, expect } from 'vitest'
import {
  healthBadge, formatBytesPerSec, formatBytes, formatMs, formatPct, formatCount, formatValue,
  ALERT_RULE_LABELS, alertLink, SERIES_RANGES, type ClusterHealth,
} from '../metrics'

const healthy: ClusterHealth = {
  configured: true, asOf: '2026-09-09T00:00:00Z',
  activeControllers: 1, offlinePartitions: 0, underReplicated: 0, underMinIsr: 0,
  uncleanElectionsLastHour: 0, uncleanElectionsTotal: 0, activeBrokers: 3, fencedBrokers: 0, brokers: [],
}

describe('healthBadge', () => {
  it('정상 클러스터는 6개 배지가 모두 ok', () => {
    const badges = healthBadge(healthy, 3)
    expect(badges).toHaveLength(6)
    expect(badges.every((b) => b.level === 'ok')).toBe(true)
    expect(badges.map((b) => b.label)).toEqual([
      '활성 컨트롤러 1', '오프라인 파티션 0', '복제 부족 0', 'min.isr 미달 0', '언클린 선출(1h) 0', '활성 브로커 3/3',
    ])
  })

  it('규칙별 등급: 컨트롤러≠1 crit, 오프라인>0 crit, URP>0 warn, min.isr>0 crit, 언클린>0 warn, 브로커<표 crit', () => {
    const bad = healthBadge({
      ...healthy, activeControllers: 0, offlinePartitions: 2, underReplicated: 1, underMinIsr: 1,
      uncleanElectionsLastHour: 1, activeBrokers: 2,
    }, 3)
    expect(bad.map((b) => b.level)).toEqual(['crit', 'crit', 'warn', 'crit', 'warn', 'crit'])
    expect(healthBadge({ ...healthy, activeControllers: 2 }, 3)[0]?.level).toBe('crit')
  })
})

describe('포맷터', () => {
  it('bytes/s 는 B/KB/MB 단위로', () => {
    expect(formatBytesPerSec(12)).toBe('12 B/s')
    expect(formatBytesPerSec(512 * 1024)).toBe('512.0 KB/s')
    expect(formatBytesPerSec(1.5 * 1024 * 1024)).toBe('1.5 MB/s')
  })
  it('bytes 는 KB/MB/GB', () => {
    expect(formatBytes(2048)).toBe('2.0 KB')
    expect(formatBytes(1024 ** 3)).toBe('1.0 GB')
  })
  it('ms 는 1000 이상이면 초', () => {
    expect(formatMs(12.4)).toBe('12 ms')
    expect(formatMs(1234)).toBe('1.2 s')
  })
  it('퍼센트·건수', () => {
    expect(formatPct(95)).toBe('95.0%')
    expect(formatCount(1234)).toBe('1,234')
    expect(formatCount(2.6)).toBe('3')
  })
  it('formatValue 는 unit 으로 분기한다', () => {
    expect(formatValue(1048576, 'bytes/s')).toBe('1.0 MB/s')
    expect(formatValue(2048, 'bytes')).toBe('2.0 KB')
    expect(formatValue(50, 'ms')).toBe('50 ms')
    expect(formatValue(12.34, '%')).toBe('12.3%')
    expect(formatValue(7, 'count')).toBe('7')
    expect(formatValue(3.5, 'msg/s')).toBe('3.5 msg/s')
  })
})

describe('알림 라벨·링크', () => {
  it('규칙 12종에 한글 라벨이 있다', () => {
    for (const t of ['LAG_HIGH', 'DISK_HIGH', 'CERT_EXPIRY', 'COLLECTOR_FAILURE', 'OFFLINE_PARTITIONS', 'URP_HIGH',
      'UNCLEAN_ELECTION', 'BROKER_DOWN', 'LATENCY_HIGH', 'HANDLER_SATURATED', 'HEAP_HIGH', 'PROMETHEUS_UNAVAILABLE']) {
      expect(ALERT_RULE_LABELS[t]?.label, t).toBeTruthy()
    }
  })
  it('브로커 규칙은 /brokers/{id}, 파티션 규칙은 /, 랙은 그룹, 그 외 null', () => {
    expect(alertLink('LATENCY_HIGH', '2')).toBe('/brokers/2')
    expect(alertLink('HANDLER_SATURATED', '1')).toBe('/brokers/1')
    expect(alertLink('HEAP_HIGH', '3')).toBe('/brokers/3')
    expect(alertLink('DISK_HIGH', '3')).toBe('/brokers/3')
    expect(alertLink('OFFLINE_PARTITIONS', 'cluster')).toBe('/')
    expect(alertLink('URP_HIGH', 'cluster')).toBe('/')
    expect(alertLink('UNCLEAN_ELECTION', 'cluster')).toBe('/')
    expect(alertLink('BROKER_DOWN', 'cluster')).toBe('/')
    expect(alertLink('LAG_HIGH', 'g1')).toBe('/groups/g1')
    expect(alertLink('COLLECTOR_FAILURE', 'collector')).toBeNull()
    expect(alertLink('UNKNOWN', 'x')).toBeNull()
  })
  it('범위 목록은 1h/6h/24h/7d 순', () => {
    expect(SERIES_RANGES.map((r) => r.value)).toEqual(['1h', '6h', '24h', '7d'])
  })
})
```

`web/src/composables/__tests__/usePrometheus.spec.ts`:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import { usePrometheus } from '../usePrometheus'

describe('usePrometheus', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    usePrometheus().setStatus(null)
  })

  it('설정되어 있으면 configured 와 healthy 를 노출한다', async () => {
    vi.mocked(api).mockResolvedValue({ configured: true, url: 'http://prom:9090', healthy: true })
    const { load, configured, healthy } = usePrometheus()
    await load()
    expect(configured.value).toBe(true)
    expect(healthy.value).toBe(true)
    expect(api).toHaveBeenCalledWith('/prometheus/status')
  })

  it('미설정이거나 조회 실패면 configured 가 false', async () => {
    vi.mocked(api).mockResolvedValue({ configured: false, url: '', healthy: false })
    const { load, configured } = usePrometheus()
    await load()
    expect(configured.value).toBe(false)
    vi.mocked(api).mockRejectedValue(new Error('401'))
    await load()
    expect(configured.value).toBe(false)
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/lib/__tests__/metrics.spec.ts src/composables/__tests__/usePrometheus.spec.ts`
Expected: FAIL (모듈 없음)

- [ ] **Step 3: 구현**

`web/src/lib/metrics.ts`:

```ts
// Prometheus 지표 화면 공용 타입·상수·순수 함수 (백엔드 metrics/dto/MetricsDtos 와 1:1)

export interface BrokerSnapshot {
  id: number
  host: string
  scraped: boolean
  bytesInPerSec: number
  bytesOutPerSec: number
  messagesInPerSec: number
  p99ProduceMs: number
  p99FetchMs: number
  handlerIdlePct: number
  networkIdlePct: number
  heapUsedPct: number
  cpuPct: number
  leaderCount: number
  partitionCount: number
  underReplicated: number
}

export interface ClusterHealth {
  configured: boolean
  asOf: string
  activeControllers: number
  offlinePartitions: number
  underReplicated: number
  underMinIsr: number
  uncleanElectionsLastHour: number
  uncleanElectionsTotal: number
  activeBrokers: number
  fencedBrokers: number
  brokers: BrokerSnapshot[]
}

export interface Point { t: string; v: number }
export interface Series { name: string; points: Point[] }
export interface SeriesResponse { key: string; unit: string; range: string; stepSeconds: number; series: Series[] }
export interface PrometheusStatus { configured: boolean; url: string; healthy: boolean }

export type SeriesRange = '1h' | '6h' | '24h' | '7d'
export const SERIES_RANGES: { value: SeriesRange; label: string }[] = [
  { value: '1h', label: '1시간' },
  { value: '6h', label: '6시간' },
  { value: '24h', label: '24시간' },
  { value: '7d', label: '7일' },
]

export type BadgeLevel = 'ok' | 'warn' | 'crit'
export interface Badge { label: string; level: BadgeLevel }

// 배지 규칙 (스펙): 활성 컨트롤러 ≠ 1 crit, 오프라인 > 0 crit, 복제 부족 > 0 warn,
// min.isr 미달 > 0 crit, 언클린 1h > 0 warn, 활성 브로커 < 브로커 표 수 crit
export function healthBadge(h: ClusterHealth, brokerCount: number): Badge[] {
  return [
    { label: `활성 컨트롤러 ${h.activeControllers}`, level: h.activeControllers === 1 ? 'ok' : 'crit' },
    { label: `오프라인 파티션 ${h.offlinePartitions}`, level: h.offlinePartitions > 0 ? 'crit' : 'ok' },
    { label: `복제 부족 ${h.underReplicated}`, level: h.underReplicated > 0 ? 'warn' : 'ok' },
    { label: `min.isr 미달 ${h.underMinIsr}`, level: h.underMinIsr > 0 ? 'crit' : 'ok' },
    { label: `언클린 선출(1h) ${formatCount(h.uncleanElectionsLastHour)}`, level: h.uncleanElectionsLastHour > 0 ? 'warn' : 'ok' },
    { label: `활성 브로커 ${h.activeBrokers}/${brokerCount}`, level: h.activeBrokers < brokerCount ? 'crit' : 'ok' },
  ]
}

const KB = 1024
const MB = KB * 1024
const GB = MB * 1024

export function formatBytesPerSec(v: number): string {
  if (v >= MB) return `${(v / MB).toFixed(1)} MB/s`
  if (v >= KB) return `${(v / KB).toFixed(1)} KB/s`
  return `${Math.round(v)} B/s`
}

export function formatBytes(v: number): string {
  if (v >= GB) return `${(v / GB).toFixed(1)} GB`
  if (v >= MB) return `${(v / MB).toFixed(1)} MB`
  if (v >= KB) return `${(v / KB).toFixed(1)} KB`
  return `${Math.round(v)} B`
}

export function formatMs(v: number): string {
  return v >= 1000 ? `${(v / 1000).toFixed(1)} s` : `${Math.round(v)} ms`
}

export function formatPct(v: number): string { return `${v.toFixed(1)}%` }
export function formatCount(v: number): string { return Math.round(v).toLocaleString('en-US') }

export function formatValue(v: number, unit: string): string {
  switch (unit) {
    case 'bytes/s': return formatBytesPerSec(v)
    case 'bytes': return formatBytes(v)
    case 'ms': return formatMs(v)
    case '%': return formatPct(v)
    case 'count': return formatCount(v)
    default: return `${Number(v.toFixed(1)).toLocaleString('en-US')} ${unit}`
  }
}

export const ALERT_RULE_LABELS: Record<string, { label: string; description: string }> = {
  LAG_HIGH: { label: '컨슈머 랙 초과', description: '컨슈머 그룹의 총 랙이 임계치를 넘었습니다' },
  DISK_HIGH: { label: '디스크 사용률 초과', description: '브로커 로그 디렉터리 사용률이 임계치를 넘었습니다' },
  CERT_EXPIRY: { label: '인증서 만료 임박', description: '브로커 TLS 인증서 만료가 가까워졌습니다' },
  COLLECTOR_FAILURE: { label: '지표 수집 실패', description: 'Kafka 지표 수집이 연속 실패했습니다' },
  OFFLINE_PARTITIONS: { label: '오프라인 파티션', description: '리더가 없는 파티션이 있어 읽기/쓰기가 막힙니다' },
  URP_HIGH: { label: '미복제 파티션', description: 'ISR 이 복제본 수보다 적은 파티션이 있습니다' },
  UNCLEAN_ELECTION: { label: '언클린 리더 선출', description: 'ISR 밖 복제본이 리더가 되어 데이터 유실 가능성이 있습니다' },
  BROKER_DOWN: { label: '브로커 감소', description: '컨트롤러가 보는 활성 브로커 수가 줄었습니다' },
  LATENCY_HIGH: { label: '요청 지연 초과', description: 'Produce/Fetch p99 지연이 임계치를 넘었습니다' },
  HANDLER_SATURATED: { label: '요청 핸들러 포화', description: '요청 핸들러 유휴율이 최소치 아래로 떨어졌습니다' },
  HEAP_HIGH: { label: '힙 사용률 초과', description: '브로커 JVM 힙 사용률이 임계치를 넘었습니다' },
  PROMETHEUS_UNAVAILABLE: { label: 'Prometheus 수집 실패', description: 'Prometheus 지표 수집이 연속 실패했습니다' },
}

const BROKER_RULES = new Set(['LATENCY_HIGH', 'HANDLER_SATURATED', 'HEAP_HIGH', 'DISK_HIGH'])
const CLUSTER_RULES = new Set(['OFFLINE_PARTITIONS', 'URP_HIGH', 'UNCLEAN_ELECTION', 'BROKER_DOWN'])

export function alertLink(ruleType: string, subjectKey: string): string | null {
  if (BROKER_RULES.has(ruleType)) return `/brokers/${encodeURIComponent(subjectKey)}`
  if (CLUSTER_RULES.has(ruleType)) return '/'
  if (ruleType === 'LAG_HIGH') return `/groups/${encodeURIComponent(subjectKey)}`
  return null
}
```

`web/src/composables/usePrometheus.ts`:

```ts
import { ref, computed } from 'vue'
import { api } from '@/api/client'
import type { PrometheusStatus } from '@/lib/metrics'

// 모듈 스코프 싱글턴: App 진입·로그인 성공 시 로드. configured=false 면 Prometheus 섹션·화면을 숨긴다.
const status = ref<PrometheusStatus | null>(null)

export function usePrometheus() {
  async function load() {
    try {
      status.value = await api<PrometheusStatus>('/prometheus/status')
    } catch {
      status.value = { configured: false, url: '', healthy: false }
    }
  }
  function setStatus(s: PrometheusStatus | null) { status.value = s }
  const configured = computed(() => status.value?.configured === true)
  const healthy = computed(() => status.value?.healthy === true)
  return { status, configured, healthy, load, setStatus }
}
```

`App.vue` script:

```ts
import { onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { useSession } from '@/composables/useSession'
import { useSchemaRegistry } from '@/composables/useSchemaRegistry'
import { usePrometheus } from '@/composables/usePrometheus'

const route = useRoute()
const { load } = useSession()
const { configured: schemaRegistryConfigured, load: loadSchemaRegistry } = useSchemaRegistry()
const { load: loadPrometheus } = usePrometheus()
onMounted(() => { load(); loadSchemaRegistry(); loadPrometheus() })
```

`LoginView.vue`: import `usePrometheus` 추가, `await useSchemaRegistry().load()` 다음 줄에 `await usePrometheus().load()`.

`LoginView.spec.ts` 의 `useSchemaRegistry` mock 줄 아래에 추가:

```ts
vi.mock('@/composables/usePrometheus', () => ({ usePrometheus: () => ({ load: vi.fn() }) }))
```

- [ ] **Step 4: 테스트 통과·타입 확인**

Run: `cd web && npx vitest run src/lib/__tests__/metrics.spec.ts src/composables/__tests__/usePrometheus.spec.ts src/views/__tests__/LoginView.spec.ts && npm run type-check`
Expected: PASS, 타입 오류 없음

- [ ] **Step 5: 커밋**

```bash
git add web/src/lib/metrics.ts web/src/lib/__tests__/metrics.spec.ts web/src/composables/usePrometheus.ts web/src/composables/__tests__/usePrometheus.spec.ts web/src/App.vue web/src/views/LoginView.vue web/src/views/__tests__/LoginView.spec.ts
git commit -m "feat(web): metrics 타입·포맷터·배지 규칙, usePrometheus 상태 로드"
```

---

### Task 10: `MetricChart` 컴포넌트 (다중 시리즈 SVG)

**Files:**
- Create: `web/src/components/MetricChart.vue`
- Test: `web/src/components/__tests__/MetricChart.spec.ts`

**Interfaces:**
- Consumes: `Series`, `formatValue` (`@/lib/metrics`).
- Produces: `<MetricChart :series="Series[]" :unit="string" :title="string" />`. 시리즈별 색상(최대 4색 순환), 호버 시 시각 + 시리즈별 값, 데이터 없으면 "데이터 없음". 범례에 시리즈 이름. `svg[aria-label]=title`.

- [ ] **Step 1: 실패하는 테스트 작성**

```ts
import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import MetricChart from '../MetricChart.vue'

const series = [
  { name: 'BROKER_BYTES_IN', points: [
    { t: '2026-09-09T00:00:00Z', v: 1048576 }, { t: '2026-09-09T00:01:00Z', v: 2097152 }, { t: '2026-09-09T00:02:00Z', v: 3145728 },
  ] },
  { name: 'BROKER_BYTES_OUT', points: [
    { t: '2026-09-09T00:00:00Z', v: 524288 }, { t: '2026-09-09T00:01:00Z', v: 524288 }, { t: '2026-09-09T00:02:00Z', v: 524288 },
  ] },
]

describe('MetricChart', () => {
  it('시리즈마다 polyline 과 범례를 그리고 title 을 aria-label 로 쓴다', () => {
    const w = mount(MetricChart, { props: { series, unit: 'bytes/s', title: '처리량' } })
    expect(w.find('svg').attributes('aria-label')).toBe('처리량')
    expect(w.findAll('polyline')).toHaveLength(2)
    const legend = w.findAll('.legend-item')
    expect(legend).toHaveLength(2)
    expect(legend[0]?.text()).toContain('BROKER_BYTES_IN')
    // 최근 값이 단위 포맷으로 보인다
    expect(legend[0]?.text()).toContain('3.0 MB/s')
    expect(legend[1]?.text()).toContain('512.0 KB/s')
  })

  it('호버하면 시각과 시리즈별 값을 보여준다', async () => {
    const w = mount(MetricChart, { props: { series, unit: 'bytes/s', title: 't' } })
    const svg = w.find('svg')
    ;(svg.element as SVGElement).getBoundingClientRect = () =>
      ({ left: 0, top: 0, width: 640, height: 200, right: 640, bottom: 200, x: 0, y: 0, toJSON: () => ({}) }) as DOMRect
    await svg.trigger('mousemove', { clientX: 0 }) // 첫 포인트
    expect(w.find('.reading').text()).toContain('1.0 MB/s')
    expect(w.find('.reading').text()).toContain('512.0 KB/s')
    expect(w.find('.reading').text()).toContain(new Date('2026-09-09T00:00:00Z').toLocaleTimeString())
    await svg.trigger('mouseleave')
    expect(w.find('.reading').exists()).toBe(false)
  })

  it('포인트가 없으면 데이터 없음 문구', () => {
    const w = mount(MetricChart, { props: { series: [{ name: 'x', points: [] }], unit: 'ms', title: 't' } })
    expect(w.text()).toContain('데이터 없음')
    expect(w.findAll('polyline')).toHaveLength(0)
    const empty = mount(MetricChart, { props: { series: [], unit: 'ms', title: 't' } })
    expect(empty.text()).toContain('데이터 없음')
  })

  it('y축 눈금은 단위 포맷을 쓴다', () => {
    const w = mount(MetricChart, { props: { series, unit: 'bytes/s', title: 't' } })
    expect(w.find('.y-max').text()).toBe('3.0 MB/s')
    expect(w.find('.y-min').text()).toBe('0 B/s')
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/components/__tests__/MetricChart.spec.ts`
Expected: FAIL (컴포넌트 없음)

- [ ] **Step 3: 구현**

```vue
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
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `cd web && npx vitest run src/components/__tests__/MetricChart.spec.ts && npm run type-check`
Expected: PASS (4 tests)

- [ ] **Step 5: 커밋**

```bash
git add web/src/components/MetricChart.vue web/src/components/__tests__/MetricChart.spec.ts
git commit -m "feat(web): MetricChart 다중 시리즈 SVG 차트"
```

---

### Task 11: 클러스터 화면 확장 (배지·브로커 열·링크·Prometheus 수집 상태)

**Files:**
- Modify: `web/src/views/ClusterView.vue`
- Test: `web/src/views/__tests__/ClusterView.spec.ts` (신규)

**Interfaces:**
- Consumes: `usePrometheus().configured`, `GET /cluster/health` → `ClusterHealth`, `healthBadge`, `formatBytesPerSec`, `formatMs`, `formatPct` (`@/lib/metrics`), `MonitorStatus.prometheusLastCollectedAt/prometheusConsecutiveFailures`.
- Produces: `.health-badges > .badge.ok|warn|crit`, 브로커 표 열 6개 추가, 브로커 id 셀 `<RouterLink :to="/brokers/{id}">`, Prometheus 불가 시 `.prom-error` 문구.

- [ ] **Step 1: 실패하는 테스트 작성**

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
const configured = ref(true)
vi.mock('@/composables/usePrometheus', () => ({ usePrometheus: () => ({ configured }) }))

import { api } from '@/api/client'
import ClusterView from '../ClusterView.vue'

const cluster = { clusterId: 'c1', controllerId: 1, brokers: [
  { id: 1, host: '10.0.0.1', port: 9094 }, { id: 2, host: '10.0.0.2', port: 9094 },
] }
const monitor = { lastCollectedAt: null, consecutiveFailures: 0, certs: [],
  prometheusLastCollectedAt: '2026-09-09T00:00:00Z', prometheusConsecutiveFailures: 0 }
const health = {
  configured: true, asOf: '2026-09-09T00:00:00Z', activeControllers: 1, offlinePartitions: 0, underReplicated: 1,
  underMinIsr: 0, uncleanElectionsLastHour: 0, uncleanElectionsTotal: 0, activeBrokers: 2, fencedBrokers: 0,
  brokers: [
    { id: 1, host: '10.0.0.1', scraped: true, bytesInPerSec: 1048576, bytesOutPerSec: 524288, messagesInPerSec: 10,
      p99ProduceMs: 12, p99FetchMs: 1500, handlerIdlePct: 95.5, networkIdlePct: 99, heapUsedPct: 40.2, cpuPct: 5,
      leaderCount: 10, partitionCount: 30, underReplicated: 0 },
    { id: 2, host: '10.0.0.2', scraped: false, bytesInPerSec: 0, bytesOutPerSec: 0, messagesInPerSec: 0,
      p99ProduceMs: 0, p99FetchMs: 0, handlerIdlePct: 0, networkIdlePct: 0, heapUsedPct: 0, cpuPct: 0,
      leaderCount: 0, partitionCount: 0, underReplicated: 0 },
  ],
}

function mockApi(healthResult: unknown = health) {
  vi.mocked(api).mockImplementation((url: string) => {
    if (url === '/cluster') return Promise.resolve(cluster)
    if (url === '/monitor/status') return Promise.resolve(monitor)
    if (url === '/cluster/health') return healthResult instanceof Error ? Promise.reject(healthResult) : Promise.resolve(healthResult)
    if (url.startsWith('/metrics')) return Promise.resolve([])
    if (url === '/monitor/disk') return Promise.resolve({ thresholdPct: 80, brokers: [] })
    return Promise.reject(new Error(`unexpected url: ${url}`))
  })
}

const mountOpts = { global: { stubs: { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' }, TrendChart: true } } }

describe('ClusterView (Prometheus)', () => {
  beforeEach(() => {
    vi.mocked(api).mockReset()
    configured.value = true
  })

  it('배지 6개와 브로커 지표 열을 보여주고 브로커 id 는 상세 링크다', async () => {
    mockApi()
    const w = mount(ClusterView, mountOpts)
    await flushPromises()

    const badges = w.findAll('.health-badges .badge')
    expect(badges).toHaveLength(6)
    expect(badges[2]?.classes()).toContain('warn') // 복제 부족 1
    expect(badges[0]?.classes()).toContain('ok')

    const headers = w.findAll('thead th').map((th) => th.text())
    expect(headers).toEqual(expect.arrayContaining(['유입', '유출', 'Produce p99', 'Fetch p99', '핸들러 유휴', '힙']))
    const rows = w.findAll('tbody tr')
    expect(rows[0]?.text()).toContain('1.0 MB/s')
    expect(rows[0]?.text()).toContain('512.0 KB/s')
    expect(rows[0]?.text()).toContain('12 ms')
    expect(rows[0]?.text()).toContain('1.5 s')
    expect(rows[0]?.text()).toContain('95.5%')
    expect(rows[0]?.text()).toContain('40.2%')
    expect(rows[0]?.find('a').attributes('href')).toBe('/brokers/1')
    // scraped=false 는 —
    expect(rows[1]?.text()).toContain('—')
    expect(rows[1]?.find('a').attributes('href')).toBe('/brokers/2')
    expect(w.text()).toContain('Prometheus 마지막 수집')
  })

  it('Prometheus 접속 불가면 배지 대신 한 줄 문구를 보여주고 표는 유지된다', async () => {
    mockApi(new Error('Prometheus 접속 불가'))
    const w = mount(ClusterView, mountOpts)
    await flushPromises()
    expect(w.find('.health-badges').exists()).toBe(false)
    expect(w.find('.prom-error').text()).toContain('Prometheus 접속 불가')
    expect(w.findAll('tbody tr')).toHaveLength(2)
    expect(w.findAll('tbody tr')[0]?.find('a').attributes('href')).toBe('/brokers/1')
  })

  it('미설정이면 배지·지표 열·링크 없이 기존 화면 그대로다', async () => {
    configured.value = false
    mockApi()
    const w = mount(ClusterView, mountOpts)
    await flushPromises()
    expect(w.find('.health-badges').exists()).toBe(false)
    expect(w.findAll('thead th').map((th) => th.text())).not.toContain('유입')
    expect(w.findAll('tbody tr')[0]?.find('a').exists()).toBe(false)
    expect(vi.mocked(api).mock.calls.map((c) => c[0])).not.toContain('/cluster/health')
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/views/__tests__/ClusterView.spec.ts`
Expected: FAIL

- [ ] **Step 3: 구현**

`ClusterView.vue` script — import·상태·로드 추가:

```ts
import { ref, computed, onMounted } from 'vue'
import { api } from '@/api/client'
import TrendChart from '@/components/TrendChart.vue'
import { usePrometheus } from '@/composables/usePrometheus'
import { healthBadge, formatBytesPerSec, formatMs, formatPct, type ClusterHealth } from '@/lib/metrics'
```

`MonitorStatus` 인터페이스에 필드 추가:

```ts
  prometheusLastCollectedAt: string | null
  prometheusConsecutiveFailures: number
```

상태·계산값:

```ts
const { configured: prometheusConfigured } = usePrometheus()
const health = ref<ClusterHealth | null>(null)
const healthError = ref('')

const badges = computed(() =>
  health.value && cluster.value ? healthBadge(health.value, cluster.value.brokers.length) : [],
)
const snapshotById = computed(() => new Map((health.value?.brokers ?? []).map((b) => [b.id, b])))
const EMPTY_CELLS = ['—', '—', '—', '—', '—', '—']
// 열 순서: 유입, 유출, Produce p99, Fetch p99, 핸들러 유휴, 힙. scraped=false 또는 스냅샷 없음이면 —
function brokerCells(id: number): string[] {
  const s = snapshotById.value.get(id)
  if (!s || !s.scraped) return EMPTY_CELLS
  return [
    formatBytesPerSec(s.bytesInPerSec), formatBytesPerSec(s.bytesOutPerSec),
    formatMs(s.p99ProduceMs), formatMs(s.p99FetchMs),
    formatPct(s.handlerIdlePct), formatPct(s.heapUsedPct),
  ]
}

async function loadHealth() {
  if (!prometheusConfigured.value) return
  try {
    health.value = await api<ClusterHealth>('/cluster/health')
  } catch (e) {
    healthError.value = e instanceof Error ? e.message : 'Prometheus 접속 불가'
  }
}
```

`onMounted` 에서 `/cluster` 로드 직후(감시 상태 로드 전) `await loadHealth()` 호출.

템플릿 — `<p>Cluster ID: …</p>` 바로 아래에:

```vue
      <template v-if="prometheusConfigured">
        <div v-if="badges.length > 0" class="health-badges">
          <span v-for="b in badges" :key="b.label" class="badge" :class="b.level">{{ b.label }}</span>
        </div>
        <p v-else-if="healthError" class="prom-error">{{ healthError }}</p>
      </template>
```

브로커 표를 다음으로 교체:

```vue
      <table>
        <thead>
          <tr>
            <th>브로커 ID</th><th>주소</th><th>역할</th>
            <template v-if="prometheusConfigured">
              <th>유입</th><th>유출</th><th>Produce p99</th><th>Fetch p99</th><th>핸들러 유휴</th><th>힙</th>
            </template>
          </tr>
        </thead>
        <tbody>
          <tr v-for="b in cluster.brokers" :key="b.id">
            <td><RouterLink v-if="prometheusConfigured" :to="`/brokers/${b.id}`">{{ b.id }}</RouterLink><template v-else>{{ b.id }}</template></td>
            <td>{{ b.host }}:{{ b.port }}</td>
            <td>{{ b.id === cluster.controllerId ? '컨트롤러' : '' }}</td>
            <template v-if="prometheusConfigured">
              <td v-for="(c, i) in brokerCells(b.id)" :key="i" class="num">{{ c }}</td>
            </template>
          </tr>
        </tbody>
      </table>
```

감시 상태의 "마지막 수집" 문단 아래에:

```vue
        <p v-if="prometheusConfigured">
          Prometheus 마지막 수집:
          {{ monitor.prometheusLastCollectedAt ? new Date(monitor.prometheusLastCollectedAt).toLocaleString() : '없음' }}
          <span v-if="monitor.prometheusConsecutiveFailures > 0" class="warn">
            (연속 실패 {{ monitor.prometheusConsecutiveFailures }}회)
          </span>
        </p>
```

스타일 추가:

```css
.health-badges { display: flex; flex-wrap: wrap; gap: 0.5rem; margin: 0.5rem 0 1rem; }
.badge { padding: 0.2rem 0.6rem; border-radius: 999px; font-size: 0.85rem; font-weight: 600; }
.badge.ok { background: var(--ok-soft); color: var(--ok); }
.badge.warn { background: var(--warn-soft); color: var(--warn); }
.badge.crit { background: var(--crit-soft); color: var(--crit); }
.prom-error { color: var(--crit); font-size: 0.9rem; }
.num { text-align: right; font-variant-numeric: tabular-nums; }
```

- [ ] **Step 4: 테스트 통과·타입 확인**

Run: `cd web && npx vitest run src/views/__tests__/ClusterView.spec.ts && npm run type-check`
Expected: PASS (3 tests)

- [ ] **Step 5: 커밋**

```bash
git add web/src/views/ClusterView.vue web/src/views/__tests__/ClusterView.spec.ts
git commit -m "feat(web): 클러스터 화면에 상태 배지·브로커 지표 열·브로커 상세 링크"
```

---

### Task 12: 브로커 상세 화면 `/brokers/:id`

**Files:**
- Create: `web/src/views/BrokerDetailView.vue`
- Modify: `web/src/router/index.ts` (라우트 1줄, `/alerts` 위)
- Test: `web/src/views/__tests__/BrokerDetailView.spec.ts`

**Interfaces:**
- Consumes: `GET /cluster/health`(카드 값·host), `GET /brokers/{id}/series?key=&range=`(차트 9개 키), `GET /alerts`(이 브로커 subjectKey 필터), `MetricChart`, `SERIES_RANGES`, 포맷터, `ALERT_RULE_LABELS`.
- Produces: 라우트 `{ path: '/brokers/:id', component: () => import('@/views/BrokerDetailView.vue') }`. 화면: `.cards .card`, `.range-tabs button.on`, `MetricChart` 4개(title 처리량/지연/부하/JVM), 알림 표.

차트 → 키 매핑:

| 차트 | 키 | unit |
|---|---|---|
| 처리량 | `BROKER_BYTES_IN`, `BROKER_BYTES_OUT` | bytes/s |
| 지연 | `BROKER_P99_PRODUCE_MS`, `BROKER_P99_FETCH_MS` | ms |
| 부하 | `BROKER_HANDLER_IDLE_PCT`, `BROKER_NETWORK_IDLE_PCT`, `BROKER_REQUEST_QUEUE` | % (요청 큐는 count 지만 같은 차트에 그린다 — 범례 값은 각자 포맷하지 않고 차트 unit `%` 로 표시하므로, 요청 큐는 **별도 4번째 시리즈가 아니라 카드로만** 보여준다: 부하 차트는 `BROKER_HANDLER_IDLE_PCT`, `BROKER_NETWORK_IDLE_PCT` 2개 + 요청 큐는 카드) |
| JVM | `BROKER_HEAP_USED_PCT`, `BROKER_GC_TIME_PCT` | % |

스펙의 부하 차트 3개 시리즈 중 `BROKER_REQUEST_QUEUE` 는 단위가 달라(`count`) 같은 % 축에 그릴 수 없다. **결정:** 부하 차트는 유휴율 2개, 요청 큐는 카드(현재값은 `series` 의 마지막 포인트)로 표시한다. 시리즈 API 호출 수는 9개(범위 전환마다 병렬).

- [ ] **Step 1: 실패하는 테스트 작성**

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
vi.mock('vue-router', () => ({ useRoute: () => ({ params: { id: '2' } }) }))

import { api } from '@/api/client'
import BrokerDetailView from '../BrokerDetailView.vue'
import MetricChart from '@/components/MetricChart.vue'

const health = {
  configured: true, asOf: '2026-09-09T00:00:00Z', activeControllers: 1, offlinePartitions: 0, underReplicated: 0,
  underMinIsr: 0, uncleanElectionsLastHour: 0, uncleanElectionsTotal: 0, activeBrokers: 2, fencedBrokers: 0,
  brokers: [
    { id: 1, host: '10.0.0.1', scraped: true, bytesInPerSec: 1, bytesOutPerSec: 1, messagesInPerSec: 1, p99ProduceMs: 1,
      p99FetchMs: 1, handlerIdlePct: 1, networkIdlePct: 1, heapUsedPct: 1, cpuPct: 1, leaderCount: 1, partitionCount: 1, underReplicated: 0 },
    { id: 2, host: '10.0.0.2', scraped: true, bytesInPerSec: 2097152, bytesOutPerSec: 1048576, messagesInPerSec: 250.4,
      p99ProduceMs: 12, p99FetchMs: 34, handlerIdlePct: 88.8, networkIdlePct: 97.1, heapUsedPct: 41.5, cpuPct: 12.3,
      leaderCount: 15, partitionCount: 45, underReplicated: 1 },
  ],
}
const alerts = [
  { ruleType: 'LATENCY_HIGH', subjectKey: '2', message: '브로커 2 Produce p99 1200ms (임계치 1000ms)', value: 1200, threshold: 1000, occurredAt: '2026-09-09T00:00:00Z' },
  { ruleType: 'HEAP_HIGH', subjectKey: '1', message: '다른 브로커', value: 90, threshold: 85, occurredAt: '2026-09-09T00:00:00Z' },
  { ruleType: 'LAG_HIGH', subjectKey: '2', message: '그룹 이름이 2 인 랙 알림', value: 1, threshold: 1, occurredAt: '2026-09-09T00:00:00Z' },
]

function seriesFor(url: string) {
  const key = new URL(url, 'http://x').searchParams.get('key') ?? ''
  const range = new URL(url, 'http://x').searchParams.get('range') ?? ''
  const unit = key.endsWith('_MS') ? 'ms' : key.endsWith('_PCT') ? '%' : key === 'BROKER_REQUEST_QUEUE' ? 'count' : 'bytes/s'
  return { key, unit, range, stepSeconds: 30, series: [{ name: key, points: [{ t: '2026-09-09T00:00:00Z', v: 7 }] }] }
}

function mockApi() {
  vi.mocked(api).mockImplementation((url: string) => {
    if (url === '/cluster/health') return Promise.resolve(health)
    if (url === '/alerts') return Promise.resolve(alerts)
    if (url.startsWith('/brokers/2/series')) return Promise.resolve(seriesFor(url))
    return Promise.reject(new Error(`unexpected url: ${url}`))
  })
}

describe('BrokerDetailView', () => {
  beforeEach(() => { vi.mocked(api).mockReset() })

  it('카드에 이 브로커의 현재값을 포맷해 보여준다', async () => {
    mockApi()
    const w = mount(BrokerDetailView)
    await flushPromises()
    expect(w.find('h1').text()).toContain('브로커 2')
    expect(w.text()).toContain('10.0.0.2')
    const cards = w.find('.cards').text()
    expect(cards).toContain('2.0 MB/s')
    expect(cards).toContain('1.0 MB/s')
    expect(cards).toContain('250.4 msg/s')
    expect(cards).toContain('12 ms')
    expect(cards).toContain('34 ms')
    expect(cards).toContain('88.8%')
    expect(cards).toContain('97.1%')
    expect(cards).toContain('41.5%')
    expect(cards).toContain('12.3%')
    expect(cards).toContain('15 / 45')
    expect(cards).toContain('요청 큐')
  })

  it('차트 4개를 그리고 범위를 바꾸면 시리즈 9개를 새 범위로 다시 조회한다', async () => {
    mockApi()
    const w = mount(BrokerDetailView)
    await flushPromises()
    const charts = w.findAllComponents(MetricChart)
    expect(charts).toHaveLength(4)
    expect(charts.map((c) => c.props('title'))).toEqual(['처리량', '지연', '부하', 'JVM'])
    expect((charts[0]?.props('series') as { name: string }[]).map((s) => s.name)).toEqual(['BROKER_BYTES_IN', 'BROKER_BYTES_OUT'])
    expect(charts[1]?.props('unit')).toBe('ms')
    const firstCalls = vi.mocked(api).mock.calls.map((c) => c[0] as string).filter((u) => u.startsWith('/brokers/2/series'))
    expect(firstCalls).toHaveLength(9)
    expect(firstCalls.every((u) => u.includes('range=1h'))).toBe(true)

    const btn = w.findAll('.range-tabs button').find((b) => b.text() === '24시간')
    await btn?.trigger('click')
    await flushPromises()
    const after = vi.mocked(api).mock.calls.map((c) => c[0] as string).filter((u) => u.includes('range=24h'))
    expect(after).toHaveLength(9)
    expect(btn?.classes()).toContain('on')
  })

  it('알림 이력은 이 브로커(subjectKey=id)의 브로커 규칙만 라벨과 함께 보여준다', async () => {
    mockApi()
    const w = mount(BrokerDetailView)
    await flushPromises()
    const rows = w.findAll('.broker-alerts tbody tr')
    expect(rows).toHaveLength(1)
    expect(rows[0]?.text()).toContain('요청 지연 초과')
    expect(rows[0]?.text()).toContain('1200ms')
  })

  it('없는 브로커면 오류 문구', async () => {
    vi.mocked(api).mockImplementation((url: string) => {
      if (url === '/cluster/health') return Promise.resolve({ ...health, brokers: [] })
      if (url === '/alerts') return Promise.resolve([])
      return Promise.reject(new Error('존재하지 않는 브로커입니다: 2'))
    })
    const w = mount(BrokerDetailView)
    await flushPromises()
    expect(w.find('.error').text()).toContain('존재하지 않는 브로커')
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/views/__tests__/BrokerDetailView.spec.ts`
Expected: FAIL

- [ ] **Step 3: 구현**

`router/index.ts` 의 `/alerts` 줄 위에 추가:

```ts
    { path: '/brokers/:id', component: () => import('@/views/BrokerDetailView.vue') },
```

`BrokerDetailView.vue`:

```vue
<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute } from 'vue-router'
import { api } from '@/api/client'
import MetricChart from '@/components/MetricChart.vue'
import {
  SERIES_RANGES, ALERT_RULE_LABELS, formatBytesPerSec, formatMs, formatPct, formatCount, formatValue,
  type ClusterHealth, type BrokerSnapshot, type SeriesResponse, type Series, type SeriesRange,
} from '@/lib/metrics'

interface AlertEvent { ruleType: string; subjectKey: string; message: string; value: number; threshold: number; occurredAt: string }

const route = useRoute()
const id = computed(() => Number(route.params.id))
const snapshot = ref<BrokerSnapshot | null>(null)
const error = ref('')
const range = ref<SeriesRange>('1h')
const alerts = ref<AlertEvent[]>([])
const seriesByKey = ref<Map<string, Series[]>>(new Map())
const requestQueue = ref<number | null>(null)

const CHARTS = [
  { title: '처리량', unit: 'bytes/s', keys: ['BROKER_BYTES_IN', 'BROKER_BYTES_OUT'] },
  { title: '지연', unit: 'ms', keys: ['BROKER_P99_PRODUCE_MS', 'BROKER_P99_FETCH_MS'] },
  { title: '부하', unit: '%', keys: ['BROKER_HANDLER_IDLE_PCT', 'BROKER_NETWORK_IDLE_PCT'] },
  { title: 'JVM', unit: '%', keys: ['BROKER_HEAP_USED_PCT', 'BROKER_GC_TIME_PCT'] },
]
const ALL_KEYS = [...CHARTS.flatMap((c) => c.keys), 'BROKER_REQUEST_QUEUE']
const BROKER_RULES = new Set(['LATENCY_HIGH', 'HANDLER_SATURATED', 'HEAP_HIGH', 'DISK_HIGH'])

function chartSeries(keys: string[]): Series[] {
  return keys.flatMap((k) => seriesByKey.value.get(k) ?? [])
}

const brokerAlerts = computed(() =>
  alerts.value.filter((a) => a.subjectKey === String(id.value) && BROKER_RULES.has(a.ruleType)),
)

async function loadSeries() {
  const results = await Promise.all(ALL_KEYS.map(async (key) => {
    try {
      const r = await api<SeriesResponse>(`/brokers/${id.value}/series?key=${key}&range=${range.value}`)
      return [key, r.series] as const
    } catch (e) {
      if (!error.value) error.value = e instanceof Error ? e.message : '조회 실패'
      return [key, []] as const
    }
  }))
  seriesByKey.value = new Map(results)
  const q = seriesByKey.value.get('BROKER_REQUEST_QUEUE')?.[0]?.points
  requestQueue.value = q && q.length > 0 ? q[q.length - 1]!.v : null
}

onMounted(async () => {
  try {
    const health = await api<ClusterHealth>('/cluster/health')
    snapshot.value = health.brokers.find((b) => b.id === id.value) ?? null
    if (!snapshot.value) error.value = `존재하지 않는 브로커입니다: ${id.value}`
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
  try {
    alerts.value = await api<AlertEvent[]>('/alerts')
  } catch {
    // 알림 표만 생략
  }
  if (snapshot.value) await loadSeries()
})

watch(range, () => { if (snapshot.value) loadSeries() })
</script>

<template>
  <main>
    <h1>브로커 {{ id }}<span v-if="snapshot" class="host">{{ snapshot.host }}</span></h1>
    <p v-if="error" class="error">{{ error }}</p>
    <template v-if="snapshot">
      <p v-if="!snapshot.scraped" class="hint">Prometheus 에 이 브로커의 시리즈가 없습니다 (스크랩 대상 확인).</p>
      <div class="cards">
        <div class="card"><span class="label">유입 / 유출</span><span class="value">{{ formatBytesPerSec(snapshot.bytesInPerSec) }} / {{ formatBytesPerSec(snapshot.bytesOutPerSec) }}</span></div>
        <div class="card"><span class="label">메시지/초</span><span class="value">{{ formatValue(snapshot.messagesInPerSec, 'msg/s') }}</span></div>
        <div class="card"><span class="label">Produce p99</span><span class="value">{{ formatMs(snapshot.p99ProduceMs) }}</span></div>
        <div class="card"><span class="label">Fetch p99</span><span class="value">{{ formatMs(snapshot.p99FetchMs) }}</span></div>
        <div class="card"><span class="label">핸들러 유휴율</span><span class="value">{{ formatPct(snapshot.handlerIdlePct) }}</span></div>
        <div class="card"><span class="label">네트워크 유휴율</span><span class="value">{{ formatPct(snapshot.networkIdlePct) }}</span></div>
        <div class="card"><span class="label">요청 큐</span><span class="value">{{ requestQueue === null ? '—' : formatCount(requestQueue) }}</span></div>
        <div class="card"><span class="label">힙</span><span class="value">{{ formatPct(snapshot.heapUsedPct) }}</span></div>
        <div class="card"><span class="label">CPU</span><span class="value">{{ formatPct(snapshot.cpuPct) }}</span></div>
        <div class="card"><span class="label">리더 / 파티션</span><span class="value">{{ snapshot.leaderCount }} / {{ snapshot.partitionCount }}</span></div>
        <div class="card"><span class="label">미복제 파티션</span><span class="value" :class="{ warn: snapshot.underReplicated > 0 }">{{ snapshot.underReplicated }}</span></div>
      </div>
      <div class="range-tabs">
        <button v-for="r in SERIES_RANGES" :key="r.value" type="button" :class="{ on: range === r.value }" @click="range = r.value">
          {{ r.label }}
        </button>
      </div>
      <MetricChart v-for="c in CHARTS" :key="c.title" :series="chartSeries(c.keys)" :unit="c.unit" :title="c.title" />
      <h2>이 브로커의 알림</h2>
      <p v-if="brokerAlerts.length === 0" class="hint">알림이 없습니다.</p>
      <table v-else class="broker-alerts">
        <thead><tr><th>시각</th><th>유형</th><th>내용</th></tr></thead>
        <tbody>
          <tr v-for="a in brokerAlerts" :key="a.occurredAt + a.ruleType">
            <td>{{ new Date(a.occurredAt).toLocaleString() }}</td>
            <td>{{ ALERT_RULE_LABELS[a.ruleType]?.label ?? a.ruleType }}</td>
            <td>{{ a.message }}</td>
          </tr>
        </tbody>
      </table>
    </template>
  </main>
</template>

<style scoped>
.host { margin-left: 0.75rem; font-size: 0.9rem; font-weight: normal; color: var(--ink-soft); }
.hint { font-size: 0.85rem; color: var(--ink-soft); }
.warn { color: var(--crit); font-weight: bold; }
.cards { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 0.6rem; margin: 0.75rem 0 1rem; }
.card { display: flex; flex-direction: column; gap: 0.2rem; padding: 0.6rem 0.75rem; background: var(--surface); border: 1px solid var(--line); border-radius: 8px; }
.card .label { font-size: 0.75rem; color: var(--ink-soft); }
.card .value { font-size: 1.05rem; font-weight: 600; font-variant-numeric: tabular-nums; }
.range-tabs { display: flex; gap: 0.5rem; margin-bottom: 0.75rem; }
.range-tabs button { padding: 0.25rem 0.75rem; border: 1px solid var(--line); border-radius: 6px; background: var(--surface); color: var(--ink); font-size: 0.85rem; }
.range-tabs button.on { border-color: var(--accent); color: var(--accent); font-weight: bold; }
</style>
```

- [ ] **Step 4: 테스트 통과·타입 확인**

Run: `cd web && npx vitest run src/views/__tests__/BrokerDetailView.spec.ts && npm run type-check`
Expected: PASS (4 tests)

- [ ] **Step 5: 커밋**

```bash
git add web/src/views/BrokerDetailView.vue web/src/views/__tests__/BrokerDetailView.spec.ts web/src/router/index.ts
git commit -m "feat(web): 브로커 상세 화면 /brokers/:id (카드·범위 선택·차트 4개·알림)"
```

---

### Task 13: 토픽 상세 유입 차트 교체 + 예시 데이터 제거

**Files:**
- Modify: `web/src/views/TopicDetailView.vue` (script 전체, 파티션 표 열, 유입 추이 섹션, 스타일)
- Modify: `web/src/views/__tests__/TopicDetailView.spec.ts` (전체 재작성)

**Interfaces:**
- Consumes: `usePrometheus().configured`, `GET /topics/{name}/series?key=TOPIC_MESSAGES_IN|TOPIC_BYTES_IN&range=`, `GET /topics/{name}/series?key=TOPIC_RETAINED_BY_PARTITION|TOPIC_LOG_SIZE_BY_PARTITION&range=1h`(현재값 = 파티션 시리즈의 마지막 포인트), `MetricChart`, `SERIES_RANGES`, `formatBytes`, `formatCount`. 폴백: 기존 `/topics/{name}/throughput`, `/metrics?type=PRODUCED_TOPIC…`, `TrendChart`, `toHourlyConsumption`.
- Produces: Prometheus 모드 — 파티션 표 열 `endOffset` 제거, "보유 메시지"·"로그 크기" 열, `.metric-tabs button.on`(메시지/바이트), `.range-tabs`, `MetricChart`(title `유입 추이`). 폴백 모드 — 기존 열(`endOffset`, `최근 1시간 유입`, `분당 속도`) + `TrendChart`, 데이터 없으면 "수집된 유입 샘플이 아직 없습니다". 두 모드 모두 `예시` 문구·`.demo` 없음. 파티션 드롭다운 제거.

- [ ] **Step 1: 테스트 재작성**

`TopicDetailView.spec.ts` 전체를 다음으로 교체한다:

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { ref } from 'vue'

vi.mock('@/api/client', () => ({ api: vi.fn() }))
vi.mock('@/composables/useSession', () => ({ useSession: () => ({ isAdmin: { value: false } }) }))
const configured = ref(false)
vi.mock('@/composables/usePrometheus', () => ({ usePrometheus: () => ({ configured }) }))
vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { name: 't' } }),
  useRouter: () => ({ push: vi.fn() }),
}))
vi.mock('@/components/TopicSchemaSection.vue', () => ({ default: { name: 'TopicSchemaSection', template: '<div />' } }))

import { api } from '@/api/client'
import TopicDetailView from '../TopicDetailView.vue'
import TrendChart from '@/components/TrendChart.vue'
import MetricChart from '@/components/MetricChart.vue'

const detail = {
  name: 't',
  partitions: [
    { partition: 0, leader: 1, replicas: [1], isr: [1] },
    { partition: 1, leader: 1, replicas: [1], isr: [1] },
  ],
  configs: {},
}
const throughput = [
  { partition: 0, endOffset: 120, count: 60, ratePerMin: 1.0 },
  { partition: 1, endOffset: 10, count: 0, ratePerMin: 0.0 },
]
const produced = [
  { sampledAt: '2026-08-20T09:00:00Z', value: 100 },
  { sampledAt: '2026-08-20T09:30:00Z', value: 130 },
  { sampledAt: '2026-08-20T10:00:00Z', value: 160 },
]

function partitionSeries(key: string, values: number[]) {
  return { key, unit: key.includes('LOG_SIZE') ? 'bytes' : 'count', range: '1h', stepSeconds: 30,
    series: values.map((v, i) => ({ name: String(i), points: [{ t: '2026-09-09T00:00:00Z', v: v / 2 }, { t: '2026-09-09T00:00:30Z', v }] })) }
}

function mockFallback(withSamples = true) {
  vi.mocked(api).mockImplementation((url: string) => {
    if (url.startsWith('/topics/t/messages')) return Promise.resolve([])
    if (url.startsWith('/topics/t/throughput')) return Promise.resolve(withSamples ? throughput : [])
    if (url.startsWith('/metrics')) return Promise.resolve(withSamples ? produced : [])
    if (url === '/topics/t') return Promise.resolve(detail)
    return Promise.reject(new Error(`unexpected url: ${url}`))
  })
}

function mockPrometheus() {
  vi.mocked(api).mockImplementation((url: string) => {
    if (url.startsWith('/topics/t/messages')) return Promise.resolve([])
    if (url === '/topics/t') return Promise.resolve(detail)
    if (url.startsWith('/topics/t/series')) {
      const key = new URL(url, 'http://x').searchParams.get('key') ?? ''
      const range = new URL(url, 'http://x').searchParams.get('range') ?? ''
      if (key === 'TOPIC_RETAINED_BY_PARTITION') return Promise.resolve(partitionSeries(key, [1500, 20]))
      if (key === 'TOPIC_LOG_SIZE_BY_PARTITION') return Promise.resolve(partitionSeries(key, [2048, 1024 * 1024]))
      return Promise.resolve({ key, unit: key === 'TOPIC_BYTES_IN' ? 'bytes/s' : 'msg/s', range, stepSeconds: 30,
        series: [{ name: key, points: [{ t: '2026-09-09T00:00:00Z', v: 3 }] }] })
    }
    return Promise.reject(new Error(`unexpected url: ${url}`))
  })
}

describe('TopicDetailView 폴백(수집기) 모드', () => {
  beforeEach(() => { vi.mocked(api).mockReset(); configured.value = false })

  it('파티션 표에 endOffset·최근 1시간 유입·분당 속도를 보여주고 추이를 TrendChart 로 그린다', async () => {
    mockFallback()
    const w = mount(TopicDetailView)
    await flushPromises()
    const rows = w.findAll('tbody tr')
    expect(rows[0]?.text()).toContain('120')
    expect(rows[0]?.text()).toContain('60')
    expect(rows[0]?.text()).toContain('1.0')
    const chart = w.findComponent(TrendChart)
    expect(chart.exists()).toBe(true)
    expect((chart.props('points') as { v: number }[]).map((p) => p.v)).toEqual([30, 30])
    expect(w.findComponent(MetricChart).exists()).toBe(false)
    expect(w.find('select.partition-select').exists()).toBe(false)
    expect(vi.mocked(api).mock.calls.map((c) => c[0] as string).some((u) => u.includes('/series'))).toBe(false)
  })

  it('샘플이 없으면 예시 데이터 없이 문구만 보여준다', async () => {
    mockFallback(false)
    const w = mount(TopicDetailView)
    await flushPromises()
    expect(w.text()).not.toContain('예시')
    expect(w.find('.demo').exists()).toBe(false)
    expect(w.findComponent(TrendChart).exists()).toBe(false)
    expect(w.text()).toContain('수집된 유입 샘플이 아직 없습니다')
    expect(w.findAll('tbody tr')[0]?.text()).toContain('—')
  })
})

describe('TopicDetailView Prometheus 모드', () => {
  beforeEach(() => { vi.mocked(api).mockReset(); configured.value = true })

  it('파티션 표에 보유 메시지·로그 크기를 보여주고 endOffset 열은 없다', async () => {
    mockPrometheus()
    const w = mount(TopicDetailView)
    await flushPromises()
    const headers = w.findAll('thead th').map((th) => th.text())
    expect(headers).toEqual(expect.arrayContaining(['보유 메시지', '로그 크기']))
    expect(headers).not.toContain('endOffset')
    expect(headers).not.toContain('최근 1시간 유입')
    const rows = w.findAll('table tbody tr')
    expect(rows[0]?.text()).toContain('1,500')
    expect(rows[0]?.text()).toContain('2.0 KB')
    expect(rows[1]?.text()).toContain('20')
    expect(rows[1]?.text()).toContain('1.0 MB')
    expect(w.text()).not.toContain('예시')
    expect(vi.mocked(api).mock.calls.map((c) => c[0] as string).some((u) => u.includes('/throughput'))).toBe(false)
  })

  it('유입 추이는 MetricChart 로 그리고 메시지/바이트 전환과 범위 변경 시 다시 조회한다', async () => {
    mockPrometheus()
    const w = mount(TopicDetailView)
    await flushPromises()
    const chart = w.findComponent(MetricChart)
    expect(chart.exists()).toBe(true)
    expect(chart.props('unit')).toBe('msg/s')
    expect(w.findComponent(TrendChart).exists()).toBe(false)
    const calls = () => vi.mocked(api).mock.calls.map((c) => c[0] as string)
    expect(calls()).toContain('/topics/t/series?key=TOPIC_MESSAGES_IN&range=1h')

    await w.findAll('.metric-tabs button').find((b) => b.text() === '바이트')?.trigger('click')
    await flushPromises()
    expect(calls()).toContain('/topics/t/series?key=TOPIC_BYTES_IN&range=1h')
    expect(w.findComponent(MetricChart).props('unit')).toBe('bytes/s')

    await w.findAll('.range-tabs button').find((b) => b.text() === '7일')?.trigger('click')
    await flushPromises()
    expect(calls()).toContain('/topics/t/series?key=TOPIC_BYTES_IN&range=7d')
  })

  it('Prometheus 조회가 실패하면 파티션 표는 유지되고 차트 자리에 오류 문구', async () => {
    vi.mocked(api).mockImplementation((url: string) => {
      if (url.startsWith('/topics/t/messages')) return Promise.resolve([])
      if (url === '/topics/t') return Promise.resolve(detail)
      if (url.startsWith('/topics/t/series')) return Promise.reject(new Error('Prometheus 접속 불가'))
      return Promise.reject(new Error(`unexpected url: ${url}`))
    })
    const w = mount(TopicDetailView)
    await flushPromises()
    expect(w.findAll('table tbody tr')).toHaveLength(2)
    expect(w.findAll('table tbody tr')[0]?.text()).toContain('—')
    expect(w.text()).toContain('Prometheus 접속 불가')
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/views/__tests__/TopicDetailView.spec.ts`
Expected: FAIL (예시 데이터·드롭다운 잔존, Prometheus 모드 없음)

- [ ] **Step 3: 구현**

`TopicDetailView.vue` script 를 다음으로 교체한다(메시지 조회·수정/삭제 부분은 기존과 같다):

```ts
import { ref, computed, onMounted, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { api } from '@/api/client'
import { useSession } from '@/composables/useSession'
import { usePrometheus } from '@/composables/usePrometheus'
import { toHourlyConsumption, type Point } from '@/lib/consumption'
import { SERIES_RANGES, formatBytes, formatCount, type Series, type SeriesResponse, type SeriesRange } from '@/lib/metrics'
import TopicEditModal from '@/components/TopicEditModal.vue'
import TopicDeleteModal from '@/components/TopicDeleteModal.vue'
import TrendChart from '@/components/TrendChart.vue'
import MetricChart from '@/components/MetricChart.vue'
import TopicSchemaSection from '@/components/TopicSchemaSection.vue'

interface PartitionInfo { partition: number; leader: number; replicas: number[]; isr: number[] }
interface TopicDetail { name: string; partitions: PartitionInfo[]; configs: Record<string, string> }
interface PartitionThroughput { partition: number; endOffset: number; count: number; ratePerMin: number }
interface SamplePoint { sampledAt: string; value: number }
interface MessageRecord { partition: number; offset: number; timestamp: string; key: string | null; value: string | null }

const route = useRoute()
const router = useRouter()
const { isAdmin } = useSession()
const { configured: prometheusConfigured } = usePrometheus()
const topicName = computed(() => String(route.params.name))
const detail = ref<TopicDetail | null>(null)
const error = ref('')
const messages = ref<MessageRecord[]>([])
const messagesError = ref('')
const messagesLoading = ref(false)
const showEdit = ref(false)
const showDelete = ref(false)

// --- 폴백(수집기) 모드: 저장된 PRODUCED_* 샘플 ---
const throughput = ref<Map<number, PartitionThroughput>>(new Map())
const producedTrend = ref<Point[]>([])

async function loadThroughput() {
  try {
    const list = await api<PartitionThroughput[]>(`/topics/${topicName.value}/throughput`)
    throughput.value = new Map(list.map((t) => [t.partition, t]))
    const samples = await api<SamplePoint[]>(
      `/metrics?type=PRODUCED_TOPIC&subject=${encodeURIComponent(topicName.value)}&hours=24`,
    )
    producedTrend.value = toHourlyConsumption(samples.map((s) => ({ t: s.sampledAt, v: s.value })))
  } catch {
    throughput.value = new Map()
    producedTrend.value = []
  }
}

// --- Prometheus 모드: 파티션 현재값 + 유입 추이 ---
const retained = ref<Map<number, number>>(new Map())
const logSize = ref<Map<number, number>>(new Map())
const intakeMetric = ref<'TOPIC_MESSAGES_IN' | 'TOPIC_BYTES_IN'>('TOPIC_MESSAGES_IN')
const intakeRange = ref<SeriesRange>('1h')
const intake = ref<SeriesResponse | null>(null)
const intakeError = ref('')

// 파티션별 시리즈의 마지막 포인트를 현재값으로 쓴다
function lastByPartition(series: Series[]): Map<number, number> {
  const m = new Map<number, number>()
  for (const s of series) {
    const p = s.points[s.points.length - 1]
    if (p) m.set(Number(s.name), p.v)
  }
  return m
}

function retainedText(partition: number): string {
  const v = retained.value.get(partition)
  return v === undefined ? '—' : formatCount(v)
}
function logSizeText(partition: number): string {
  const v = logSize.value.get(partition)
  return v === undefined ? '—' : formatBytes(v)
}

async function loadPartitionMetrics() {
  try {
    const [r, l] = await Promise.all([
      api<SeriesResponse>(`/topics/${topicName.value}/series?key=TOPIC_RETAINED_BY_PARTITION&range=1h`),
      api<SeriesResponse>(`/topics/${topicName.value}/series?key=TOPIC_LOG_SIZE_BY_PARTITION&range=1h`),
    ])
    retained.value = lastByPartition(r.series)
    logSize.value = lastByPartition(l.series)
  } catch {
    retained.value = new Map()
    logSize.value = new Map()
  }
}

async function loadIntake() {
  intakeError.value = ''
  try {
    intake.value = await api<SeriesResponse>(
      `/topics/${topicName.value}/series?key=${intakeMetric.value}&range=${intakeRange.value}`,
    )
  } catch (e) {
    intake.value = null
    intakeError.value = e instanceof Error ? e.message : '조회 실패'
  }
}

watch([intakeMetric, intakeRange], () => { if (prometheusConfigured.value) loadIntake() })

async function loadMessages() {
  messagesLoading.value = true
  messagesError.value = ''
  try {
    messages.value = await api<MessageRecord[]>(`/topics/${topicName.value}/messages?limit=50`)
  } catch (e) {
    messagesError.value = e instanceof Error ? e.message : '조회 실패'
  } finally {
    messagesLoading.value = false
  }
}

onMounted(async () => {
  try {
    detail.value = await api<TopicDetail>(`/topics/${topicName.value}`)
  } catch (e) {
    error.value = e instanceof Error ? e.message : '조회 실패'
  }
  const extra = prometheusConfigured.value ? [loadPartitionMetrics(), loadIntake()] : [loadThroughput()]
  await Promise.all([loadMessages(), ...extra])
})

async function reload() {
  showEdit.value = false
  detail.value = await api<TopicDetail>(`/topics/${topicName.value}`)
}

function onDeleted() {
  router.push('/topics')
}
```

템플릿의 파티션 표 헤더·행을 다음으로 교체:

```vue
      <table>
        <thead>
          <tr>
            <th>파티션</th><th>리더</th><th>복제본</th><th>ISR</th><th>상태</th>
            <template v-if="prometheusConfigured"><th>보유 메시지</th><th>로그 크기</th></template>
            <template v-else><th>endOffset</th><th>최근 1시간 유입</th><th>분당 속도</th></template>
          </tr>
        </thead>
        <tbody>
          <tr v-for="p in detail.partitions" :key="p.partition">
            <td>{{ p.partition }}</td>
            <td>{{ p.leader }}</td>
            <td>{{ p.replicas.join(', ') }}</td>
            <td>{{ p.isr.join(', ') }}</td>
            <td>
              <span v-if="p.isr.length < p.replicas.length" class="warn">복제 부족</span>
              <span v-else>정상</span>
            </td>
            <template v-if="prometheusConfigured">
              <td class="num">{{ retainedText(p.partition) }}</td>
              <td class="num">{{ logSizeText(p.partition) }}</td>
            </template>
            <template v-else>
              <td class="num">{{ throughput.get(p.partition)?.endOffset ?? '—' }}</td>
              <td class="num">{{ throughput.get(p.partition)?.count ?? '—' }}</td>
              <td class="num">{{ throughput.get(p.partition)?.ratePerMin.toFixed(1) ?? '—' }}</td>
            </template>
          </tr>
        </tbody>
      </table>
```

`<p v-if="isDemo" …>` 문단과 `<template v-if="isDemo || producedTrend.length > 0">…</template>` 블록 전체를 다음으로 교체:

```vue
      <template v-if="prometheusConfigured">
        <div class="trend-head">
          <h2>유입 추이</h2>
          <div class="metric-tabs">
            <button type="button" :class="{ on: intakeMetric === 'TOPIC_MESSAGES_IN' }" @click="intakeMetric = 'TOPIC_MESSAGES_IN'">메시지</button>
            <button type="button" :class="{ on: intakeMetric === 'TOPIC_BYTES_IN' }" @click="intakeMetric = 'TOPIC_BYTES_IN'">바이트</button>
          </div>
          <div class="range-tabs">
            <button v-for="r in SERIES_RANGES" :key="r.value" type="button" :class="{ on: intakeRange === r.value }" @click="intakeRange = r.value">
              {{ r.label }}
            </button>
          </div>
        </div>
        <p v-if="intakeError" class="error">{{ intakeError }}</p>
        <MetricChart v-else-if="intake" :series="intake.series" :unit="intake.unit" title="유입 추이" />
      </template>
      <template v-else>
        <h2>유입 추이 (시간대별)</h2>
        <TrendChart v-if="producedTrend.length > 0" :points="producedTrend" label="토픽 유입 추이 차트" />
        <p v-else class="hint">수집된 유입 샘플이 아직 없습니다. 수집이 시작되면 표시됩니다.</p>
      </template>
```

스타일: `.demo`, `.demo-badge`, `.partition-select` 규칙을 삭제하고 추가:

```css
.trend-head { display: flex; align-items: center; gap: 0.75rem; flex-wrap: wrap; }
.metric-tabs, .range-tabs { display: flex; gap: 0.4rem; margin-top: 1.6rem; }
.metric-tabs button, .range-tabs button {
  padding: 0.25rem 0.75rem; border: 1px solid var(--line); border-radius: 6px;
  background: var(--surface); color: var(--ink); font-size: 0.85rem;
}
.metric-tabs button.on, .range-tabs button.on { border-color: var(--accent); color: var(--accent); font-weight: bold; }
```

- [ ] **Step 4: 테스트 통과·타입 확인**

Run: `cd web && npx vitest run src/views/__tests__/TopicDetailView.spec.ts && npm run type-check`
Expected: PASS (6 tests). `isDemo`, `demoThroughput`, `demoTrend`, `selectedPartition`, `partitionTrends` 식별자가 파일에 남아 있지 않은지 `grep -n "demo\|selectedPartition" web/src/views/TopicDetailView.vue` 로 확인(결과 없음).

- [ ] **Step 5: 커밋**

```bash
git add web/src/views/TopicDetailView.vue web/src/views/__tests__/TopicDetailView.spec.ts
git commit -m "feat(web): 토픽 유입 차트를 Prometheus 로 교체(폴백 유지), 예시 데이터 제거"
```

---

### Task 14: 알림 화면 라벨·링크

**Files:**
- Modify: `web/src/views/AlertsView.vue`
- Test: `web/src/views/__tests__/AlertsView.spec.ts` (신규)

**Interfaces:**
- Consumes: `ALERT_RULE_LABELS`, `alertLink` (`@/lib/metrics`).
- Produces: 유형 셀에 한글 라벨(`title` 속성에 설명), 알 수 없는 타입은 원문; 대상 셀은 `alertLink` 가 있으면 `RouterLink`.

- [ ] **Step 1: 실패하는 테스트 작성**

```ts
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

vi.mock('@/api/client', () => ({ api: vi.fn() }))

import { api } from '@/api/client'
import AlertsView from '../AlertsView.vue'

const alerts = [
  { ruleType: 'LATENCY_HIGH', subjectKey: '2', message: 'm1', value: 1, threshold: 1, occurredAt: '2026-09-09T00:00:00Z' },
  { ruleType: 'OFFLINE_PARTITIONS', subjectKey: 'cluster', message: 'm2', value: 1, threshold: 0, occurredAt: '2026-09-09T00:01:00Z' },
  { ruleType: 'LAG_HIGH', subjectKey: 'g1', message: 'm3', value: 1, threshold: 1, occurredAt: '2026-09-09T00:02:00Z' },
  { ruleType: 'WEIRD', subjectKey: 'x', message: 'm4', value: 1, threshold: 1, occurredAt: '2026-09-09T00:03:00Z' },
]
const opts = { global: { stubs: { RouterLink: { props: ['to'], template: '<a :href="to"><slot /></a>' } } } }

describe('AlertsView', () => {
  beforeEach(() => { vi.mocked(api).mockReset() })

  it('규칙 타입에 한글 라벨을 붙이고 대상은 화면으로 링크한다', async () => {
    vi.mocked(api).mockResolvedValue(alerts)
    const w = mount(AlertsView, opts)
    await flushPromises()
    const rows = w.findAll('tbody tr')
    expect(rows[0]?.text()).toContain('요청 지연 초과')
    expect(rows[0]?.find('a').attributes('href')).toBe('/brokers/2')
    expect(rows[1]?.text()).toContain('오프라인 파티션')
    expect(rows[1]?.find('a').attributes('href')).toBe('/')
    expect(rows[2]?.find('a').attributes('href')).toBe('/groups/g1')
    expect(rows[3]?.text()).toContain('WEIRD')
    expect(rows[3]?.find('a').exists()).toBe(false)
    expect(rows[0]?.find('td:nth-child(2)').attributes('title')).toContain('p99')
  })
})
```

- [ ] **Step 2: 실패 확인**

Run: `cd web && npx vitest run src/views/__tests__/AlertsView.spec.ts`
Expected: FAIL

- [ ] **Step 3: 구현**

`AlertsView.vue` script 에 import·계산값 추가:

```ts
import { ref, computed, onMounted } from 'vue'
import { ALERT_RULE_LABELS, alertLink } from '@/lib/metrics'
```

```ts
// 템플릿에서 non-null 단언을 쓰지 않도록 라벨·링크를 미리 계산한다
const rows = computed(() =>
  alerts.value.map((a) => ({
    ...a,
    label: ALERT_RULE_LABELS[a.ruleType]?.label ?? a.ruleType,
    description: ALERT_RULE_LABELS[a.ruleType]?.description ?? '',
    link: alertLink(a.ruleType, a.subjectKey),
  })),
)
```

표 행을 다음으로 교체:

```vue
        <tr v-for="a in rows" :key="a.occurredAt + a.ruleType + a.subjectKey">
          <td>{{ new Date(a.occurredAt).toLocaleString() }}</td>
          <td :title="a.description">{{ a.label }}</td>
          <td>
            <RouterLink v-if="a.link" :to="a.link">{{ a.subjectKey }}</RouterLink>
            <template v-else>{{ a.subjectKey }}</template>
          </td>
          <td>{{ a.message }}</td>
        </tr>
```

- [ ] **Step 4: 테스트 통과·타입 확인**

Run: `cd web && npx vitest run src/views/__tests__/AlertsView.spec.ts && npm run type-check`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add web/src/views/AlertsView.vue web/src/views/__tests__/AlertsView.spec.ts
git commit -m "feat(web): 알림 화면에 규칙 한글 라벨과 대상 링크"
```

---

### Task 15: 전체 검증 + 운영 Prometheus 스모크

**Files:** 변경 없음(검증만). 수정이 필요하면 해당 파일을 고치고 이 Task 에서 커밋한다.

- [ ] **Step 1: 프론트 전체 테스트·타입·빌드**

Run: `cd web && npx vitest run && npm run type-check && npm run build`
Expected: 모두 PASS / 오류 없음

- [ ] **Step 2: 백엔드 전체 테스트 (Docker 켜진 상태, 단독 실행)**

Run: `cd was && ./gradlew test`
Expected: BUILD SUCCESSFUL. IT(`MetricsCollectorIT`, `SchemaRegistryIT`, `PrometheusIT` 등) 포함 전부 통과.

- [ ] **Step 3: 운영 Prometheus 스모크 (선택 — 로컬 설정이 있을 때)**

`was/config/application-local.yml` 에 `app.prometheus.url: http://10.10.10.19:9090` 이 있는 상태에서 백엔드를 띄우고(README 로컬 실행 절차, 반드시 `was/` 에서 실행) 다음을 확인한다:

```bash
CK=/tmp/ck-prom; B=http://localhost:8080
curl -s -c $CK -H 'Content-Type: application/json' -d '{"username":"admin","password":"devpw"}' -o /dev/null -w "login [%{http_code}]\n" $B/api/auth/login
curl -s -b $CK $B/api/prometheus/status; echo
curl -s -b $CK $B/api/cluster/health | python3 -c "import sys,json; h=json.load(sys.stdin); print(h['activeControllers'], h['activeBrokers'], [(b['id'], b['scraped'], round(b['handlerIdlePct'],1)) for b in h['brokers']])"
curl -s -b $CK "$B/api/brokers/1/series?key=BROKER_BYTES_IN&range=1h" | python3 -c "import sys,json; s=json.load(sys.stdin); print(s['unit'], s['stepSeconds'], len(s['series'][0]['points']) if s['series'] else 0)"
curl -s -b $CK "$B/api/topics/__consumer_offsets/series?key=TOPIC_LOG_SIZE_BY_PARTITION&range=1h" | python3 -c "import sys,json; s=json.load(sys.stdin); print(len(s['series']), '파티션')"
curl -s -b $CK -o /dev/null -w "bad key [%{http_code}]\n" "$B/api/brokers/1/series?key=TOPIC_BYTES_IN&range=1h"
curl -s -b $CK -o /dev/null -w "no broker [%{http_code}]\n" "$B/api/brokers/99/series?key=BROKER_BYTES_IN&range=1h"
```

Expected: `configured:true, healthy:true`; 활성 컨트롤러 1, 활성 브로커 3, 브로커 3대 `scraped=true`, 핸들러 유휴율 ≤ 100; 시계열 `bytes/s 30 N>0`; 파티션 50개; `[400]`, `[404]`. 브라우저에서 `/`(배지·열), `/brokers/1`(카드·차트 4개·범위 전환), 토픽 상세(보유 메시지·로그 크기·차트 전환), `/alerts` 를 확인한다.

- [ ] **Step 4: 남은 변경 커밋**

```bash
git status --short
# 수정이 있었다면:
git add -A && git commit -m "fix: 전체 검증에서 발견된 수정"
```

---

## 스펙 대비 결정 사항 (실행자 참고)

- `ClusterHealth` 에 `uncleanElectionsTotal` 필드 추가(수집기가 누적값을 별도 질의 없이 사용).
- 브로커 상세 "부하" 차트는 단위가 다른 `BROKER_REQUEST_QUEUE` 를 빼고 유휴율 2개만 그리며, 요청 큐는 카드로 표시(시계열 조회는 9개 유지).
- 알림 링크는 스펙(브로커 규칙 → `/brokers/{id}`, 파티션 규칙 → `/`)에 더해 `DISK_HIGH` → `/brokers/{id}`, `LAG_HIGH` → `/groups/{id}` 를 붙인다(기존 규칙에도 같은 편의를 제공).
- Prometheus 스냅샷에서 `scraped=false` 브로커의 브로커별 샘플은 저장하지 않는다(0 값이 `HANDLER_SATURATED` 거짓 알림을 만들기 때문).
- 토픽 상세 Prometheus 모드의 파티션 현재값은 `range=1h` 시계열의 마지막 포인트를 쓴다(현재값 전용 API 를 추가하지 않음 — 스펙 API 4개 유지).
