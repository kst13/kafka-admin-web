package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import static org.assertj.core.api.Assertions.assertThat;

class ClusterQueryServiceIT extends KafkaIntegrationTestBase {

    @Autowired ClusterQueryService service;

    @Test
    void 단일_브로커_클러스터_정보를_조회한다() {
        ClusterInfo info = service.getClusterInfo();
        assertThat(info.clusterId()).isNotBlank();
        assertThat(info.brokers()).hasSize(1);
        assertThat(info.controllerId()).isEqualTo(info.brokers().get(0).id());
    }
}
