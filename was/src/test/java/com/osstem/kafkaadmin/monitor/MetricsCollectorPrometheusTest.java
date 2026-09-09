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
