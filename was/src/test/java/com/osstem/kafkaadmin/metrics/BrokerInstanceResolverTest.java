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
