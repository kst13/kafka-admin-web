package com.osstem.kafkaadmin.monitor;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.MonitorQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupSummary;
import com.osstem.kafkaadmin.kafka.dto.Dtos.PartitionLag;
import com.osstem.kafkaadmin.metrics.ClusterHealthService;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.BrokerSnapshot;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.ClusterHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

// 지표 수집 1회분. 스케줄링은 MonitorScheduler가 담당한다(테스트 용이성 분리).
@Service
public class MetricsCollector {

    private static final Logger log = LoggerFactory.getLogger(MetricsCollector.class);
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

    public void collectOnce() {
        collectKafka();
        collectPrometheus();
    }

    // 기존 collectOnce 본문 그대로 — Kafka 배치 수집·저장·평가, 실패는 독립 카운터로 알림
    private void collectKafka() {
        try {
            Instant now = Instant.now();
            List<MetricSample> batch = new ArrayList<>();
            for (GroupSummary g : groups.listGroups()) {
                GroupDetail detail = groups.describeGroup(g.groupId());
                batch.add(new MetricSample("LAG", g.groupId(), detail.totalLag(), now));
                // 누적 커밋 오프셋 합 — 소비량은 프론트가 증가분으로 계산한다
                long committedSum = detail.lags().stream()
                        .mapToLong(PartitionLag::committed).sum();
                batch.add(new MetricSample("CONSUMED_TOTAL", g.groupId(), committedSum, now));
                // 토픽별 합 — "어느 토픽에서 몇 건 가져왔는지" 소비 내역의 원천
                detail.lags().stream()
                        .collect(java.util.stream.Collectors.groupingBy(PartitionLag::topic,
                                java.util.stream.Collectors.summingLong(PartitionLag::committed)))
                        .forEach((topic, sum) -> batch.add(new MetricSample(
                                "CONSUMED_TOPIC", g.groupId() + "|" + topic, sum, now)));
            }
            // 파티션별 최신 오프셋(누적) — 유입량은 API/프론트가 증가분으로 계산한다
            Map<String, Long> endOffsets = monitorQuery.latestOffsetsByTopicPartition();
            endOffsets.forEach((topicPartition, offset) ->
                    batch.add(new MetricSample("PRODUCED_PARTITION", topicPartition, offset, now)));
            endOffsets.entrySet().stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            e -> e.getKey().substring(0, e.getKey().lastIndexOf('|')),
                            java.util.stream.Collectors.summingLong(Map.Entry::getValue)))
                    .forEach((topic, sum) ->
                            batch.add(new MetricSample("PRODUCED_TOPIC", topic, sum, now)));
            batch.add(new MetricSample("URP", "cluster",
                    monitorQuery.countUnderReplicatedPartitions(), now));
            monitorQuery.diskUsedPercentByBroker().forEach((brokerId, pct) ->
                    batch.add(new MetricSample("DISK_USED_PCT", String.valueOf(brokerId), pct, now)));
            batch.add(new MetricSample("BROKER_COUNT", "cluster",
                    cluster.getClusterInfo().brokers().size(), now));

            samples.saveAll(batch);
            evaluator.evaluate(batch);
            failures.set(0);
            lastSuccess.set(now);
        } catch (RuntimeException e) {
            int count = failures.incrementAndGet();
            log.warn("지표 수집 실패 ({}회 연속): {}", count, e.getMessage());
            // 연속 실패는 클러스터 전면 장애 신호일 수 있다 — 스펙 예외 처리 절
            if (count == FAILURE_ALERT_AT) {
                evaluator.raise("COLLECTOR_FAILURE", "collector",
                        "지표 수집이 %d회 연속 실패: %s".formatted(count, e.getMessage()),
                        count, FAILURE_ALERT_AT);
            }
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
}
