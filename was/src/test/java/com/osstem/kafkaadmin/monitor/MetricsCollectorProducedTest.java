package com.osstem.kafkaadmin.monitor;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.MonitorQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// 파티션별 유입량 표·유입 추이 차트의 원천 지표: 파티션별 최신 오프셋을
// PRODUCED_PARTITION(파티션별)·PRODUCED_TOPIC(토픽 합) 누적값으로 저장한다.
// (증가분 계산은 API/프론트 담당 — CONSUMED_* 와 동일한 규약)
class MetricsCollectorProducedTest {

    private final GroupQueryService groups = mock(GroupQueryService.class);
    private final MonitorQueryService monitorQuery = mock(MonitorQueryService.class);
    private final ClusterQueryService cluster = mock(ClusterQueryService.class);
    private final MetricSampleRepository samples = mock(MetricSampleRepository.class);
    private final AlertEvaluator evaluator = mock(AlertEvaluator.class);
    private final MetricsCollector collector =
            new MetricsCollector(groups, monitorQuery, cluster, samples, evaluator);

    @Test
    @SuppressWarnings("unchecked")
    void 파티션별_최신_오프셋과_토픽_합을_PRODUCED_지표로_저장한다() {
        when(groups.listGroups()).thenReturn(List.of());
        when(monitorQuery.countUnderReplicatedPartitions()).thenReturn(0);
        when(monitorQuery.diskUsedPercentByBroker()).thenReturn(Map.of());
        when(monitorQuery.latestOffsetsByTopicPartition()).thenReturn(Map.of(
                "order-events|0", 120L,
                "order-events|1", 205L,
                "audit-log|0", 50L));
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("c", 1, List.of()));

        collector.collectOnce();

        ArgumentCaptor<List<MetricSample>> captor = ArgumentCaptor.forClass(List.class);
        verify(samples).saveAll(captor.capture());
        assertThat(captor.getValue())
                .filteredOn(s -> s.getMetricType().equals("PRODUCED_PARTITION"))
                .extracting(MetricSample::getSubjectKey, MetricSample::getValue)
                .containsExactlyInAnyOrder(
                        tuple("order-events|0", 120.0),
                        tuple("order-events|1", 205.0),
                        tuple("audit-log|0", 50.0));
        assertThat(captor.getValue())
                .filteredOn(s -> s.getMetricType().equals("PRODUCED_TOPIC"))
                .extracting(MetricSample::getSubjectKey, MetricSample::getValue)
                .containsExactlyInAnyOrder(
                        tuple("order-events", 325.0),
                        tuple("audit-log", 50.0));
    }
}
