package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupDetail;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class GroupQueryServiceIT extends KafkaIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired GroupQueryService service;

    @Test
    void 커밋_이후_추가된_메시지_수만큼_랙이_계산된다() throws Exception {
        admin.createTopics(List.of(new NewTopic("lag-topic", 1, (short) 1))).all().get();

        try (var producer = new KafkaProducer<String, String>(producerProps())) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>("lag-topic", "m" + i)).get();
            }
        }
        // 그룹 g1로 5건 전부 소비·커밋
        try (var consumer = new KafkaConsumer<String, String>(consumerProps("g1"))) {
            consumer.subscribe(List.of("lag-topic"));
            int seen = 0;
            while (seen < 5) {
                seen += consumer.poll(Duration.ofSeconds(1)).count();
            }
            consumer.commitSync();
        }
        // 2건 추가 생산 → lag 2
        try (var producer = new KafkaProducer<String, String>(producerProps())) {
            producer.send(new ProducerRecord<>("lag-topic", "m5")).get();
            producer.send(new ProducerRecord<>("lag-topic", "m6")).get();
        }

        GroupDetail detail = service.describeGroup("g1");
        assertThat(detail.totalLag()).isEqualTo(2);
        assertThat(detail.lags()).hasSize(1);
        assertThat(detail.lags().get(0).committed()).isEqualTo(5);
        assertThat(detail.lags().get(0).end()).isEqualTo(7);

        assertThat(service.listGroups())
                .anySatisfy(g -> assertThat(g.groupId()).isEqualTo("g1"));
    }

    @Test
    void 활성_멤버의_클라이언트ID와_담당_파티션이_보인다() throws Exception {
        admin.createTopics(List.of(new NewTopic("member-topic", 2, (short) 1))).all().get();

        try (var consumer = new KafkaConsumer<String, String>(consumerProps("member-g1"))) {
            consumer.subscribe(List.of("member-topic"));
            // 파티션이 배정될 때까지 폴링 (리밸런스 완료 대기)
            while (consumer.assignment().isEmpty()) {
                consumer.poll(Duration.ofMillis(200));
            }

            GroupDetail detail = service.describeGroup("member-g1");

            assertThat(detail.members()).hasSize(1);
            var member = detail.members().get(0);
            assertThat(member.memberId()).isNotBlank();
            assertThat(member.clientId()).isNotBlank();
            assertThat(member.host()).isNotBlank();
            assertThat(member.assignedPartitions())
                    .containsExactly("member-topic-0", "member-topic-1");
        }
    }

    private Map<String, Object> producerProps() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer",
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
    }

    private Map<String, Object> consumerProps(String groupId) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
    }
}
