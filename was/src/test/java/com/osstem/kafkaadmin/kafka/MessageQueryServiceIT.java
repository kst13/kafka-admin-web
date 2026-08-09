package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.MessageRecord;
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

// 메시지 열람(읽기 전용): 토픽의 최근 메시지를 최신순으로 읽는다.
// 테스트 간 오프셋 간섭을 피하려고 테스트마다 전용 토픽을 쓴다.
class MessageQueryServiceIT extends KafkaIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired MessageQueryService service;

    // 타임스탬프를 1초 간격으로 명시해 최신순 정렬을 결정적으로 만든다
    private void produce(String topic, int partitions, String... values) throws Exception {
        admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        Map<String, Object> p = Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.serializer", StringSerializer.class,
                "value.serializer", StringSerializer.class);
        long base = System.currentTimeMillis() - values.length * 1_000L;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            for (int i = 0; i < values.length; i++) {
                producer.send(new ProducerRecord<>(topic, i % partitions, base + i * 1_000L,
                        "k" + (i + 1), values[i])).get();
            }
        }
    }

    @Test
    void 최근_메시지를_모든_파티션에서_모아_최신순으로_읽는다() throws Exception {
        produce("msg-read-order", 2, "v1", "v2", "v3", "v4", "v5");

        List<MessageRecord> records = service.readRecent("msg-read-order", 10);

        assertThat(records).extracting(MessageRecord::value)
                .containsExactly("v5", "v4", "v3", "v2", "v1");
        assertThat(records.get(0).key()).isEqualTo("k5");
        assertThat(records.get(0).timestamp()).isAfter(records.get(4).timestamp());
    }

    @Test
    void limit만큼만_최신순으로_잘라낸다() throws Exception {
        produce("msg-read-limit", 2, "a", "b", "c", "d");

        List<MessageRecord> records = service.readRecent("msg-read-limit", 2);

        assertThat(records).extracting(MessageRecord::value).containsExactly("d", "c");
    }
}
