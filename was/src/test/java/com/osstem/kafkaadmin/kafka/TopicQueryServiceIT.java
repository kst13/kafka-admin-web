package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicSummary;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class TopicQueryServiceIT extends KafkaIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired TopicQueryService service;

    @BeforeEach
    void createTopic() throws Exception {
        if (!admin.listTopics().names().get().contains("orders")) {
            admin.createTopics(List.of(new NewTopic("orders", 3, (short) 1))).all().get();
        }
    }

    @Test
    void 토픽_목록에_파티션수와_복제팩터가_나온다() {
        List<TopicSummary> topics = service.listTopics();
        TopicSummary orders = topics.stream()
                .filter(t -> t.name().equals("orders")).findFirst().orElseThrow();
        assertThat(orders.partitionCount()).isEqualTo(3);
        assertThat(orders.replicationFactor()).isEqualTo(1);
    }

    @Test
    void 토픽_상세에_파티션별_리더와_isr_그리고_주요_설정이_나온다() {
        TopicDetail detail = service.describeTopic("orders");
        assertThat(detail.partitions()).hasSize(3);
        assertThat(detail.partitions().get(0).isr()).isNotEmpty();
        assertThat(detail.configs()).containsKeys("retention.ms", "min.insync.replicas", "cleanup.policy");
    }
}
