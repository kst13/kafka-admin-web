package com.osstem.kafkaadmin.monitor;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
import com.osstem.kafkaadmin.kafka.MonitorQueryService;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

// 수집 3회 연속 실패 시 정확히 1회만 COLLECTOR_FAILURE 알림 — 스펙 예외 처리 절
class MetricsCollectorFailureTest {

    private final GroupQueryService groups = mock(GroupQueryService.class);
    private final MonitorQueryService monitorQuery = mock(MonitorQueryService.class);
    private final ClusterQueryService cluster = mock(ClusterQueryService.class);
    private final MetricSampleRepository samples = mock(MetricSampleRepository.class);
    private final AlertEvaluator evaluator = mock(AlertEvaluator.class);
    private final MetricsCollector collector =
            new MetricsCollector(groups, monitorQuery, cluster, samples, evaluator);

    @Test
    void 삼회_연속_실패에_정확히_한_번만_알림이_발생한다() {
        when(groups.listGroups())
                .thenThrow(new KafkaUnavailableException(new RuntimeException("down")));

        for (int i = 0; i < 5; i++) {
            collector.collectOnce();
        }

        assertThat(collector.consecutiveFailures()).isEqualTo(5);
        assertThat(collector.lastSuccessAt()).isNull();
        verify(evaluator, times(1)).raise(
                eq("COLLECTOR_FAILURE"), eq("collector"), anyString(), anyDouble(), anyDouble());
        verify(samples, never()).saveAll(any());
    }
}
