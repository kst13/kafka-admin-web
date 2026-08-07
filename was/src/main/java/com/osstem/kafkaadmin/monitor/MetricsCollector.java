package com.osstem.kafkaadmin.monitor;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.MonitorQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupSummary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
    private final AtomicInteger failures = new AtomicInteger();
    private final AtomicReference<Instant> lastSuccess = new AtomicReference<>();

    public MetricsCollector(GroupQueryService groups, MonitorQueryService monitorQuery,
                            ClusterQueryService cluster, MetricSampleRepository samples,
                            AlertEvaluator evaluator) {
        this.groups = groups;
        this.monitorQuery = monitorQuery;
        this.cluster = cluster;
        this.samples = samples;
        this.evaluator = evaluator;
    }

    public void collectOnce() {
        try {
            Instant now = Instant.now();
            List<MetricSample> batch = new ArrayList<>();
            for (GroupSummary g : groups.listGroups()) {
                batch.add(new MetricSample("LAG", g.groupId(),
                        groups.describeGroup(g.groupId()).totalLag(), now));
            }
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

    public Instant lastSuccessAt() { return lastSuccess.get(); }
    public int consecutiveFailures() { return failures.get(); }
}
