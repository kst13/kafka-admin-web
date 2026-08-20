package com.osstem.kafkaadmin.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
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
    void 전체_토픽의_파티션별_최신_오프셋을_조회한다() throws Exception {
        admin.createTopics(List.of(new NewTopic("inflow-topic", 2, (short) 1))).all().get();
        Map<String, Object> p = Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.serializer", StringSerializer.class,
                "value.serializer", StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            for (int i = 0; i < 3; i++) {
                producer.send(new ProducerRecord<>("inflow-topic", 0, null, "v" + i)).get();
            }
            producer.send(new ProducerRecord<>("inflow-topic", 1, null, "v")).get();
        }

        Map<String, Long> offsets = service.latestOffsetsByTopicPartition();

        assertThat(offsets)
                .containsEntry("inflow-topic|0", 3L)
                .containsEntry("inflow-topic|1", 1L);
    }

    @Test
    void 브로커별_디스크_사용률을_0에서_100_사이로_반환한다() {
        Map<Integer, Double> disk = service.diskUsedPercentByBroker();
        assertThat(disk).isNotEmpty();
        assertThat(disk.values()).allSatisfy(pct ->
                assertThat(pct).isBetween(0.0, 100.0));
    }
}
