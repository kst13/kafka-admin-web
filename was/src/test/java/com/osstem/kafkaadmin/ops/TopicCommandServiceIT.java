package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.KafkaIntegrationTestBase;
import com.osstem.kafkaadmin.kafka.TopicQueryService;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;

class TopicCommandServiceIT extends KafkaIntegrationTestBase {

    @Autowired TopicCommandService commands;
    @Autowired TopicQueryService queries;

    @Test
    void 생성_수정_삭제_왕복() {
        String topic = "ops-t-roundtrip";
        commands.createTopic(topic, 3, (short) 1, Map.of("retention.ms", "3600000"));
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.describeTopic(topic).partitions()).hasSize(3));
        assertThat(queries.describeTopic(topic).configs()).containsEntry("retention.ms", "3600000");

        commands.updateTopic(topic, 6, Map.of("retention.ms", "7200000"));
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.describeTopic(topic).partitions()).hasSize(6));
        assertThat(queries.describeTopic(topic).configs()).containsEntry("retention.ms", "7200000");

        commands.deleteTopic(topic);
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.listTopics()).noneMatch(t -> t.name().equals(topic)));
    }

    @Test
    void 중복_생성은_TopicExists_예외() {
        commands.createTopic("ops-t-dup", 1, (short) 1, null);
        assertThatThrownBy(() -> commands.createTopic("ops-t-dup", 1, (short) 1, null))
                .isInstanceOf(TopicExistsException.class);
    }

    @Test
    void 없는_토픽_수정은_UnknownTopic_예외() {
        assertThatThrownBy(() -> commands.updateTopic("ops-t-missing", null, Map.of("retention.ms", "1000")))
                .isInstanceOf(UnknownTopicOrPartitionException.class);
    }

    @Test
    void 파티션_감소는_거부된다() {
        commands.createTopic("ops-t-shrink", 3, (short) 1, null);
        assertThatThrownBy(() -> commands.updateTopic("ops-t-shrink", 2, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("감소");
    }

    @Test
    void 잘못된_토픽명은_거부된다() {
        assertThatThrownBy(() -> commands.createTopic("bad name!", 1, (short) 1, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 수정_요청에_변경_내용이_없으면_거부된다() {
        assertThatThrownBy(() -> commands.updateTopic("ops-t-any", null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
