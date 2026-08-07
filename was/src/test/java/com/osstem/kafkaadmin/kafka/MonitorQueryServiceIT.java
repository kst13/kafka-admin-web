package com.osstem.kafkaadmin.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class MonitorQueryServiceIT extends KafkaIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired MonitorQueryService service;

    @Test
    void 단일_브로커_정상_클러스터는_URP가_0이다() throws Exception {
        if (!admin.listTopics().names().get().contains("monitor-topic")) {
            admin.createTopics(List.of(new NewTopic("monitor-topic", 2, (short) 1))).all().get();
        }
        assertThat(service.countUnderReplicatedPartitions()).isZero();
    }

    @Test
    void 브로커별_디스크_사용률을_0에서_100_사이로_반환한다() {
        Map<Integer, Double> disk = service.diskUsedPercentByBroker();
        assertThat(disk).isNotEmpty();
        assertThat(disk.values()).allSatisfy(pct ->
                assertThat(pct).isBetween(0.0, 100.0));
    }
}
