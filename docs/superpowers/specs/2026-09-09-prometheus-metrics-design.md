# Prometheus 지표 연동 설계 (클러스터 상태·브로커 상세·토픽 유입·알림 확장)

작성일: 2026-09-09

## 배경과 결정 사항

운영 서버에 JMX exporter(브로커 3대, `:7071`), kafka_exporter(`kafka-exporter:9308`), Prometheus 2.54(`http://10.10.10.19:9090`,
보존 30일, 스크랩 30초)가 설치되었다. 관리자 사이트가 이 지표를 화면과 알림에 활용한다.

| 항목 | 결정 | 이유 |
|---|---|---|
| 연동 방식 | 백엔드가 Prometheus HTTP API 를 호출하고 PromQL 은 서버의 질의 카탈로그에 고정 | Prometheus 를 브라우저에 노출하지 않고, 폴백·캐시·테스트를 서버에서 일관되게 처리 |
| 알림 | 화면 표시 + 기존 알림 체계 확장(규칙 7개) | 알림 이력·쿨다운·발송을 한 곳에서 관리 |
| 유입 추이 차트 | Prometheus 로 교체, 미설정 시 기존 수집기 폴백, "예시" 데이터 제거 | 브로커 집계값이 정확하고 30일 이력·해상도 선택이 가능 |
| 브로커 지표 위치 | 클러스터 화면에 현재값 요약 + 브로커 상세 화면(`/brokers/:id`) 신설 | 기존 화면 변화를 작게, 확장 여지 확보 |

## 확인된 지표 사실 (2026-09-09 운영 Prometheus 조회)

- 브로커 JMX `instance` 라벨은 `host:7071` (예 `10.10.10.17:7071`). `ClusterInfo.brokers[].host` 와 host 부분이 같다.
- `kafka_server_brokertopicmetrics_*_total` 은 토픽 라벨이 없는 **브로커 집계 시리즈**가 항상 있고, 토픽별 시리즈는 트래픽이 있어야 생긴다. 토픽 질의 결과가 비면 0 으로 본다.
- `kafka_network_socketserver_networkprocessoravgidlepercent` 는 0~1 비율. `kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent` 는 유휴 상태에서 약 2.0 으로 관측됨(미터 rate 노출로 보임). 화면·알림은 `clamp_max(value, 1)` 로 보정한 값을 쓰고, exporter 규칙 확인은 후속 과제로 남긴다.
- `kafka_log_log_size{topic,partition,instance}` 는 복제본마다 시리즈가 있다. 파티션 논리 크기는 `max by (partition)`, 디스크 총 점유는 `sum`.
- `kafka_controller_kafkacontroller_activebrokercount` 는 브로커 3대가 모두 같은 값을 보고한다 → `max()`.
- 컨슈머 랙 시리즈(`kafka_consumergroup_lag` 등)는 현재 없다(커밋 오프셋 없음). 랙은 기존 AdminClient 계산을 유지한다.
- node_exporter 없음 → 디스크 사용률은 기존 `describeLogDirs` 방식 유지.

## 백엔드 — `metrics` 패키지 신설

### 설정 `PrometheusProperties` (`app.prometheus.*`)

| 키 | 환경변수 | 기본값 | 설명 |
|---|---|---|---|
| `url` | `PROMETHEUS_URL` | (빈 값) | 비어 있으면 기능 비활성 |
| `timeout-ms` | `PROMETHEUS_TIMEOUT_MS` | 5000 | connect/read |
| `jmx-port` | `PROMETHEUS_JMX_PORT` | 7071 | 브로커 host → `instance` 라벨 조립용 |
| `cache-seconds` | `PROMETHEUS_CACHE_SECONDS` | 15 | 클러스터 상태 캐시 |

### 클라이언트 `PrometheusClient`

- `boolean configured()`, `String url()`
- `List<InstantSample> instant(String promql)` → `GET /api/v1/query?query=` ; `InstantSample(Map<String,String> labels, double value)`
- `List<RangeSeries> range(String promql, Instant start, Instant end, Duration step)` → `GET /api/v1/query_range` ; `RangeSeries(Map<String,String> labels, List<Point(Instant t, double v)>)`
- 오류: 연결 실패·5xx → `PrometheusUnavailableException`(503 "Prometheus 접속 불가"); 응답 `status != "success"` 또는 4xx → `PrometheusQueryException(message)`(500 — 카탈로그 질의 오류는 코드 결함); 미설정 → `PrometheusNotConfiguredException`(503 "Prometheus 가 설정되지 않았습니다").
- NaN/`+Inf` 값은 0 으로 정규화하지 않고 **건너뛴다**(포인트 제외).

### 질의 카탈로그 `MetricKey` (enum)

각 항목: 키 이름, PromQL 템플릿, 단위, 인자(`broker` 는 `instance="<host>:<jmxPort>"`, `topic` 은 `topic="<name>"`; 인자 값은 라벨 이스케이프 후 치환). `[$r]` 은 범위별 rate 창(1h→2m, 6h→5m, 24h→10m, 7d→1h). 현재값 질의는 `[5m]`.

클러스터(인자 없음, 현재값):

| 키 | PromQL | 단위 |
|---|---|---|
| `ACTIVE_CONTROLLERS` | `sum(kafka_controller_kafkacontroller_activecontrollercount)` | count |
| `OFFLINE_PARTITIONS` | `sum(kafka_controller_kafkacontroller_offlinepartitionscount)` | count |
| `UNDER_REPLICATED` | `sum(kafka_server_replicamanager_underreplicatedpartitions)` | count |
| `UNDER_MIN_ISR` | `sum(kafka_server_replicamanager_underminisrpartitioncount)` | count |
| `UNCLEAN_ELECTIONS_1H` | `sum(increase(kafka_controller_controllerstats_uncleanleaderelections_total[1h]))` | count |
| `UNCLEAN_ELECTIONS_TOTAL` | `sum(kafka_controller_controllerstats_uncleanleaderelections_total)` | count (알림용 누적) |
| `ACTIVE_BROKERS` | `max(kafka_controller_kafkacontroller_activebrokercount)` | count |
| `FENCED_BROKERS` | `max(kafka_controller_kafkacontroller_fencedbrokercount)` | count |

브로커(인자 `broker`, 현재값·시계열 공용):

| 키 | PromQL | 단위 |
|---|---|---|
| `BROKER_BYTES_IN` | `sum(rate(kafka_server_brokertopicmetrics_bytesin_total{instance="$b",topic=""}[$r]))` | bytes/s |
| `BROKER_BYTES_OUT` | `sum(rate(kafka_server_brokertopicmetrics_bytesout_total{instance="$b",topic=""}[$r]))` | bytes/s |
| `BROKER_MESSAGES_IN` | `sum(rate(kafka_server_brokertopicmetrics_messagesin_total{instance="$b",topic=""}[$r]))` | msg/s |
| `BROKER_P99_PRODUCE_MS` | `max(kafka_network_requestmetrics_totaltimems{instance="$b",request="Produce",quantile="0.99"})` | ms |
| `BROKER_P99_FETCH_MS` | `max(kafka_network_requestmetrics_totaltimems{instance="$b",request="FetchConsumer",quantile="0.99"})` | ms |
| `BROKER_HANDLER_IDLE_PCT` | `clamp_max(max(kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent{instance="$b"}), 1) * 100` | % |
| `BROKER_NETWORK_IDLE_PCT` | `clamp_max(max(kafka_network_socketserver_networkprocessoravgidlepercent{instance="$b"}), 1) * 100` | % |
| `BROKER_REQUEST_QUEUE` | `max(kafka_network_requestchannel_requestqueuesize{instance="$b"})` | count |
| `BROKER_HEAP_USED_PCT` | `max(jvm_memory_used_bytes{instance="$b",area="heap"}) / max(jvm_memory_max_bytes{instance="$b",area="heap"}) * 100` | % |
| `BROKER_GC_TIME_PCT` | `sum(rate(jvm_gc_collection_seconds_sum{instance="$b"}[$r])) * 100` | % |
| `BROKER_CPU_PCT` | `rate(process_cpu_seconds_total{instance="$b"}[$r]) * 100` | % |
| `BROKER_LEADER_COUNT` | `max(kafka_server_replicamanager_leadercount{instance="$b"})` | count |
| `BROKER_PARTITION_COUNT` | `max(kafka_server_replicamanager_partitioncount{instance="$b"})` | count |
| `BROKER_URP` | `max(kafka_server_replicamanager_underreplicatedpartitions{instance="$b"})` | count |

토픽(인자 `topic`):

| 키 | PromQL | 단위 | 시리즈 |
|---|---|---|---|
| `TOPIC_MESSAGES_IN` | `sum(rate(kafka_server_brokertopicmetrics_messagesin_total{topic="$t"}[$r]))` | msg/s | 1 |
| `TOPIC_BYTES_IN` | `sum(rate(kafka_server_brokertopicmetrics_bytesin_total{topic="$t"}[$r]))` | bytes/s | 1 |
| `TOPIC_LOG_SIZE_BY_PARTITION` | `max by (partition) (kafka_log_log_size{topic="$t"})` | bytes | 파티션별 |
| `TOPIC_LOG_SIZE_TOTAL` | `sum(kafka_log_log_size{topic="$t"})` | bytes (복제 포함) | 1 |
| `TOPIC_RETAINED_BY_PARTITION` | `max by (partition) (kafka_topic_partition_current_offset{topic="$t"} - kafka_topic_partition_oldest_offset{topic="$t"})` | count | 파티션별 |

### 브로커 매핑 `BrokerInstanceResolver`

`ClusterQueryService.getClusterInfo().brokers()` 의 `host` 로 `instance = host + ":" + jmxPort` 를 만들고, 반대로 `instance` 의 host 부분으로 브로커 id 를 찾는다. 없는 id 는 404 "존재하지 않는 브로커입니다".

### `ClusterHealthService`

`ClusterHealth health()`:

```
ClusterHealth(boolean configured, Instant asOf,
  int activeControllers, int offlinePartitions, int underReplicated, int underMinIsr,
  double uncleanElectionsLastHour, int activeBrokers, int fencedBrokers,
  List<BrokerSnapshot> brokers)
BrokerSnapshot(int id, String host, boolean scraped,
  double bytesInPerSec, double bytesOutPerSec, double messagesInPerSec,
  double p99ProduceMs, double p99FetchMs, double handlerIdlePct, double networkIdlePct,
  double heapUsedPct, double cpuPct, int leaderCount, int partitionCount, int underReplicated)
```

- 미설정이면 `configured=false` 와 빈 값. 클러스터 8개 질의 + 브로커 질의는 `instance` 라벨 없이 전체를 한 번에 받아 브로커별로 나눈다(질의 수는 브로커 수와 무관하게 약 20개). `scraped=false` 는 Prometheus 에 그 브로커 시리즈가 없을 때.
- 결과는 `cache-seconds` 동안 캐시(단일 항목, 동시 요청은 하나만 조회).

### `MetricSeriesService`

`SeriesResponse series(MetricKey key, Map<String,String> args, SeriesRange range)`:

| range | 창 | step | rate 창 |
|---|---|---|---|
| `1h` | 60분 | 30s | 2m |
| `6h` | 6시간 | 1m | 5m |
| `24h` | 24시간 | 5m | 10m |
| `7d` | 7일 | 30m | 1h |

```
SeriesResponse(String key, String unit, SeriesRange range, long stepSeconds,
  List<Series(String name, List<Point(Instant t, double v)>)>)
```

파티션별 키는 `name = partition`, 단일 키는 `name = key 이름`. 시계열 질의는 캐시하지 않는다.

### API (모두 인증만, 조회 전용)

| 메서드 | 경로 | 응답 |
|---|---|---|
| GET | `/api/prometheus/status` | `{configured, url, healthy}` — `healthy` 는 `GET /-/healthy` 결과(미설정이면 false) |
| GET | `/api/cluster/health` | `ClusterHealth` (미설정이어도 200, `configured=false`) |
| GET | `/api/brokers/{id}/series?key=&range=` | `SeriesResponse` — `key` 는 `BROKER_*` 만 허용, 그 외 400 |
| GET | `/api/topics/{name}/series?key=&range=` | `SeriesResponse` — `key` 는 `TOPIC_*` 만 허용 |

기존 `/api/metrics`, `/api/topics/{name}/throughput` 은 폴백용으로 유지한다.

### 오류 매핑 (`ApiExceptionHandler` 확장)

| 예외 | HTTP |
|---|---|
| `PrometheusNotConfiguredException` | 503 "Prometheus 가 설정되지 않았습니다" |
| `PrometheusUnavailableException` | 503 "Prometheus 접속 불가" |
| `PrometheusQueryException` | 500 "Prometheus 질의 오류: <message>" |
| 잘못된 key/range, 브로커 종류 불일치 | 400 (기존 `IllegalArgumentException` 매핑) |
| 없는 브로커 id | 404 "존재하지 않는 브로커입니다: <id>" |

## 백엔드 — 알림 확장 (`monitor` 패키지)

### 수집

`MetricsCollector.collectOnce()` 는 Kafka 수집·저장·평가를 마친 뒤 **별도 try/catch** 로 Prometheus 스냅샷을 수집한다(설정 시). `ClusterHealthService.health()` 를 캐시 무시로 호출해 아래 샘플을 같은 `sampledAt` 으로 저장하고 평가한다.

| metricType | subjectKey | 값 |
|---|---|---|
| `OFFLINE_PARTITIONS` | `cluster` | offlinePartitions |
| `UNDER_MIN_ISR` | `cluster` | underMinIsr |
| `UNCLEAN_ELECTIONS` | `cluster` | `UNCLEAN_ELECTIONS_TOTAL` 누적값 |
| `ACTIVE_BROKERS` | `cluster` | activeBrokers |
| `P99_PRODUCE_MS` | 브로커 id | p99ProduceMs |
| `P99_FETCH_MS` | 브로커 id | p99FetchMs |
| `HANDLER_IDLE_PCT` | 브로커 id | handlerIdlePct (보정값) |
| `HEAP_USED_PCT` | 브로커 id | heapUsedPct |

Prometheus 실패는 Kafka 수집 결과에 영향을 주지 않는다. 별도 실패 카운터로 3회 연속 실패 시 `PROMETHEUS_UNAVAILABLE` 알림 1건(쿨다운 적용), 복구 시 카운터 초기화. `MonitorStatus` 응답에 `prometheusLastCollectedAt`, `prometheusConsecutiveFailures` 를 추가한다.

### 규칙 (`AlertEvaluator` 확장, 임계치는 `MonitorProperties`)

| ruleType | 조건 | 설정 키 (환경변수) | 기본값 |
|---|---|---|---|
| `OFFLINE_PARTITIONS` | `OFFLINE_PARTITIONS` > 0 | — | — |
| `URP_HIGH` | 기존 `URP` 샘플 > 0 (AdminClient 값, Prometheus 없어도 동작) | — | — |
| `UNCLEAN_ELECTION` | `UNCLEAN_ELECTIONS` 가 직전 저장 샘플보다 큼 | — | — |
| `BROKER_DOWN` | `ACTIVE_BROKERS` 가 직전 저장 샘플보다 작음 | — | — |
| `LATENCY_HIGH` | `P99_PRODUCE_MS` > `p99ProduceMsThreshold` 또는 `P99_FETCH_MS` > `p99FetchMsThreshold` | `MONITOR_P99_PRODUCE_MS`, `MONITOR_P99_FETCH_MS` | 1000, 2000 |
| `HANDLER_SATURATED` | `HANDLER_IDLE_PCT` < `handlerIdleMinPct` | `MONITOR_HANDLER_IDLE_MIN_PCT` | 20 |
| `HEAP_HIGH` | `HEAP_USED_PCT` > `heapUsedPctThreshold` | `MONITOR_HEAP_USED_PCT` | 85 |

"직전 저장 샘플" 은 `MetricSampleRepository` 에서 같은 type/subject 의 최신 1건(이번 배치 이전)을 읽는다. 첫 샘플(직전 없음)은 증가·감소 판정을 하지 않는다. 메시지 형식은 기존 규칙과 같이 한글 문장 + 값/임계치. 브로커 알림의 `subjectKey` 는 브로커 id 문자열.

`AlertEvent.ruleType` 주석과 `MetricSample.metricType` 주석에 새 값을 추가한다. 보존은 기존 `retentionDays`(7일) 그대로.

## 프론트

### `usePrometheus` 컴포저블

`GET /prometheus/status` 를 App 진입 시와 로그인 성공 시 로드(`useSchemaRegistry` 와 같은 패턴). `configured`, `healthy` 노출. 미설정이면 아래 섹션·화면을 숨기고 기존 화면은 변화 없다.

### `lib/metrics.ts`

타입(`ClusterHealth`, `BrokerSnapshot`, `SeriesResponse`, `Series`, `Point`), 상수 `SERIES_RANGES`(`1h/6h/24h/7d` 라벨), 순수 함수 `formatBytesPerSec`, `formatMs`, `formatPct`, `healthBadge(health) → {label, level: 'ok'|'warn'|'crit'}[]` (규칙: 활성 컨트롤러 ≠ 1 crit, 오프라인 > 0 crit, 복제 부족 > 0 warn, min.isr 미달 > 0 crit, 언클린 1h > 0 warn, 활성 브로커 < 브로커 표 수 crit), `ALERT_RULE_LABELS`(규칙 타입 → 한글 라벨·설명·링크 대상).

### `MetricChart` 컴포넌트

기존 `TrendChart`(단일 시리즈, 라벨 없음)를 대체하지 않고 새로 만든다: props `series: Series[]`, `unit`, `title`; 여러 시리즈를 색상 구분해 그리고, 호버 시 시각과 각 시리즈 값 표시, 단위별 포맷(`bytes/s`→ MB/s, `ms`, `%`), 데이터 없음 문구. SVG 기반, 외부 라이브러리 없음(기존 관례).

### 클러스터 화면 (`ClusterView` 확장)

- 상단 `.health-badges`: `healthBadge()` 결과 6개. Prometheus 불가 시 배지 대신 "Prometheus 접속 불가" 한 줄.
- 브로커 표 열 추가: 유입 MB/s, 유출 MB/s, Produce p99, Fetch p99, 핸들러 유휴율, 힙. `scraped=false` 면 "—". 브로커 id 셀은 `/brokers/{id}` 링크.
- 기존 URP 추이·디스크 섹션 유지.

### 브로커 상세 화면 신설 (`/brokers/:id`, `BrokerDetailView`)

- 상단 카드: 유입/유출, 메시지/초, p99 두 개, 핸들러·네트워크 유휴율, 힙, CPU, 리더/파티션 수, 이 브로커 URP.
- 범위 선택 1개(`1h/6h/24h/7d`)가 4개 차트에 공통: 처리량(`BROKER_BYTES_IN`, `BROKER_BYTES_OUT`), 지연(`BROKER_P99_PRODUCE_MS`, `BROKER_P99_FETCH_MS`), 부하(`BROKER_HANDLER_IDLE_PCT`, `BROKER_NETWORK_IDLE_PCT`, `BROKER_REQUEST_QUEUE`), JVM(`BROKER_HEAP_USED_PCT`, `BROKER_GC_TIME_PCT`). 차트마다 시리즈 2~3개를 각각 `series` API 로 받아 합친다(요청 병렬).
- 알림 이력 중 이 브로커(subjectKey = id)의 최근 항목 표.

### 토픽 상세 유입 차트 교체 (`TopicDetailView`)

- `configured` 일 때: "유입 추이" 섹션은 `TOPIC_MESSAGES_IN` / `TOPIC_BYTES_IN` 전환 + 범위 선택으로 `MetricChart` 를 그린다. 파티션 표의 "최근 1시간 유입"·"분당 속도" 열을 "보유 메시지"(`TOPIC_RETAINED_BY_PARTITION` 현재값)·"로그 크기"(`TOPIC_LOG_SIZE_BY_PARTITION` 현재값)로 바꾼다. 파티션 선택 드롭다운은 제거한다(파티션별 유입은 Prometheus 토픽 지표에 없음).
- 미설정일 때: 지금의 `/topics/{name}/throughput` + `/metrics?type=PRODUCED_*` 경로를 그대로 쓴다.
- "예시" 데이터(`isDemo`, `demoThroughput`, `demoTrend`)는 두 경로 모두에서 제거하고, 데이터 없음은 문구로 표시한다.

### 알림 화면 (`AlertsView`)

`ALERT_RULE_LABELS` 로 규칙 타입에 한글 라벨·설명을 붙이고, 브로커 규칙(`LATENCY_HIGH`, `HANDLER_SATURATED`, `HEAP_HIGH`)은 `/brokers/{subjectKey}` 로, 파티션 규칙은 `/` 로 링크한다. 알 수 없는 타입은 원문 표시.

### 라우트·메뉴

`/brokers/:id` 추가. 최상위 메뉴 변경 없음.

## 테스트

- **단위 (백엔드)**: `PrometheusClientTest`(MockRestServiceServer: instant/range 파싱, NaN 건너뜀, `status:"error"` → QueryException, 연결 실패 → Unavailable, 미설정), `MetricKeyTest`(키별 PromQL 생성, 인자 이스케이프 `"`·`\`, 범위별 rate 창, 브로커/토픽 키 구분), `BrokerInstanceResolverTest`, `ClusterHealthServiceTest`(mock 클라이언트: 브로커 분배, scraped=false, 캐시 TTL), `MetricSeriesServiceTest`(step/창 계산, 파티션별 시리즈 이름), `AlertEvaluatorTest` 확장(규칙 7개: 경계값, 증가/감소 판정에 직전 샘플 사용, 첫 샘플 무판정, 쿨다운), `MetricsCollectorTest` 확장(Prometheus 실패가 Kafka 배치를 깨지 않음, 3회 연속 실패 알림 1건, 복구 시 초기화).
- **슬라이스**: 새 API 4개 — 200 형태, 미설정 503/`configured=false`, 잘못된 key/range 400, 브로커 키를 토픽 API 에 400, 비로그인 401.
- **통합**: `prom/prometheus:v2.54.1` 컨테이너 + 같은 네트워크의 **정적 metrics 서버**(`nginx:alpine` 에 exposition 텍스트 파일 1개를 마운트, 브로커 3대 분량의 라벨을 가진 고정값) 를 Testcontainers 로 띄워, 실제 `query`/`query_range` 왕복으로 `ClusterHealth` 조립과 시계열 범위 4종, `/-/healthy` 를 검증한다. 스크랩 간격 5초로 두고 첫 스크랩을 기다린다.
- **vitest**: `healthBadge` 규칙, `MetricChart`(다중 시리즈·호버·단위 포맷·빈 데이터), `ClusterView` 배지·열·링크·미설정 숨김, `BrokerDetailView` 범위 전환 시 4개 차트 재조회와 카드 값, `TopicDetailView` Prometheus/폴백 분기와 예시 데이터 부재, `AlertsView` 라벨·링크, `usePrometheus`.

## 비범위

- 디스크 사용률(node_exporter 없음), 컨슈머 랙의 Prometheus 전환(시리즈 없음), Grafana/Alertmanager 연동, 임계치 화면 편집, 브로커 간 비교 차트, JMX exporter 규칙 수정(핸들러 유휴율 2.0 문제는 보정으로 대응하고 exporter 설정 확인은 후속).

## 구현 시 주의

- 네트워크 코드는 connect/read 타임아웃 모두 명시. Prometheus 호출은 화면 요청당 최대 십수 개이므로 `ClusterHealth` 는 반드시 캐시하고, 시계열 API 는 한 요청에 질의 1개만 실행한다.
- PromQL 인자 치환 시 라벨 값의 `"`, `\`, 개행을 이스케이프한다. 토픽명·host 는 이미 패턴이 제한되어 있지만 방어적으로 처리한다.
- `MetricSample`/`AlertEvent` 의 새 타입 값은 문자열이므로 스키마 변경 없음.
- 기존 `PRODUCED_*`, `CONSUMED_*` 수집은 유지한다(폴백과 소비 내역 화면이 쓴다).
- 운영 반영: `deploy/.env` 에 `PROMETHEUS_URL=http://10.10.10.19:9090` 추가. `docker-compose` 의 `was` 컨테이너에서 `10.10.10.19:9090` 으로 나가는 경로가 열려 있어야 한다(같은 호스트라면 문제없음).
