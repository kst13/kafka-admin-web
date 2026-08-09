package com.osstem.kafkaadmin.monitor;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.MonitorQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupSummary;
import com.osstem.kafkaadmin.kafka.dto.Dtos.PartitionLag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// 시간대별 소비량 차트의 원천 지표: 그룹별 커밋 오프셋 합을 CONSUMED_TOTAL 누적값으로 저장한다.
// (증가분 계산은 프론트 lib/consumption.ts 담당)
class MetricsCollectorConsumedTotalTest {

    private final GroupQueryService groups = mock(GroupQueryService.class);
    private final MonitorQueryService monitorQuery = mock(MonitorQueryService.class);
    private final ClusterQueryService cluster = mock(ClusterQueryService.class);
    private final MetricSampleRepository samples = mock(MetricSampleRepository.class);
    private final AlertEvaluator evaluator = mock(AlertEvaluator.class);
    private final MetricsCollector collector =
            new MetricsCollector(groups, monitorQuery, cluster, samples, evaluator);

    @Test
    @SuppressWarnings("unchecked")
    void 그룹별_커밋_오프셋_합을_CONSUMED_TOTAL_지표로_저장한다() {
        when(groups.listGroups()).thenReturn(List.of(new GroupSummary("g1", "Stable", 1)));
        when(groups.describeGroup("g1")).thenReturn(new GroupDetail("g1", "Stable",
                List.of(new PartitionLag("t", 0, 100, 120, 20),
                        new PartitionLag("t", 1, 200, 205, 5)), 25, List.of()));
        when(monitorQuery.countUnderReplicatedPartitions()).thenReturn(0);
        when(monitorQuery.diskUsedPercentByBroker()).thenReturn(Map.of());
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("c", 1, List.of()));

        collector.collectOnce();

        ArgumentCaptor<List<MetricSample>> captor = ArgumentCaptor.forClass(List.class);
        verify(samples).saveAll(captor.capture());
        assertThat(captor.getValue())
                .filteredOn(s -> s.getMetricType().equals("CONSUMED_TOTAL"))
                .extracting(MetricSample::getSubjectKey, MetricSample::getValue)
                .containsExactly(tuple("g1", 300.0));
        // 기존 LAG 지표도 그대로 저장되어야 한다
        assertThat(captor.getValue())
                .filteredOn(s -> s.getMetricType().equals("LAG"))
                .extracting(MetricSample::getValue).containsExactly(25.0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void 토픽별_커밋_오프셋_합을_CONSUMED_TOPIC_지표로_저장한다() {
        when(groups.listGroups()).thenReturn(List.of(new GroupSummary("g1", "Stable", 1)));
        when(groups.describeGroup("g1")).thenReturn(new GroupDetail("g1", "Stable",
                List.of(new PartitionLag("order-events", 0, 100, 120, 20),
                        new PartitionLag("order-events", 1, 200, 205, 5),
                        new PartitionLag("audit-log", 0, 50, 50, 0)), 25, List.of()));
        when(monitorQuery.countUnderReplicatedPartitions()).thenReturn(0);
        when(monitorQuery.diskUsedPercentByBroker()).thenReturn(Map.of());
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("c", 1, List.of()));

        collector.collectOnce();

        ArgumentCaptor<List<MetricSample>> captor = ArgumentCaptor.forClass(List.class);
        verify(samples).saveAll(captor.capture());
        assertThat(captor.getValue())
                .filteredOn(s -> s.getMetricType().equals("CONSUMED_TOPIC"))
                .extracting(MetricSample::getSubjectKey, MetricSample::getValue)
                .containsExactlyInAnyOrder(
                        tuple("g1|order-events", 300.0),
                        tuple("g1|audit-log", 50.0));
    }
}
