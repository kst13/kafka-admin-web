package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.KafkaIntegrationTestBase;
import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.PartitionLag;
import com.osstem.kafkaadmin.ops.GroupCommandService.StartFrom;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;

class GroupCommandServiceIT extends KafkaIntegrationTestBase {

    private static final String TOPIC = "grp-reg-topic";
    private static boolean prepared;

    @Autowired Admin admin;
    @Autowired GroupCommandService commands;
    @Autowired GroupQueryService queries;

    // 토픽 1개(파티션 2)에 메시지 3건을 넣어 earliest(0)와 latest(끝) 가 구분되게 한다
    @BeforeAll
    static void prepare(@Autowired Admin admin) throws Exception {
        if (prepared) return;
        admin.createTopics(List.of(new NewTopic(TOPIC, 2, (short) 1))).all().get();
        try (var producer = new KafkaProducer<String, String>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", StringSerializer.class.getName()))) {
            for (int i = 0; i < 3; i++) {
                producer.send(new ProducerRecord<>(TOPIC, 0, "k", "v" + i)).get();
            }
        }
        prepared = true;
    }

    @Test
    void latest_로_등록하면_그룹이_Empty_로_보이고_랙이_0이다() {
        commands.registerGroup("grp-reg-latest", List.of(TOPIC), StartFrom.LATEST);

        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.listGroups()).anyMatch(g -> g.groupId().equals("grp-reg-latest")));
        GroupDetail detail = queries.describeGroup("grp-reg-latest");
        assertThat(detail.state()).isEqualToIgnoringCase("Empty");
        assertThat(detail.lags()).hasSize(2);
        assertThat(detail.totalLag()).isZero();
    }

    @Test
    void earliest_로_등록하면_처음부터_읽도록_커밋되어_랙이_메시지_수와_같다() {
        commands.registerGroup("grp-reg-earliest", List.of(TOPIC), StartFrom.EARLIEST);

        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.listGroups()).anyMatch(g -> g.groupId().equals("grp-reg-earliest")));
        GroupDetail detail = queries.describeGroup("grp-reg-earliest");
        assertThat(detail.lags()).extracting(PartitionLag::committed).containsOnly(0L);
        assertThat(detail.totalLag()).isEqualTo(3);
    }

    @Test
    void 이미_있는_그룹은_GroupExists_예외() {
        commands.registerGroup("grp-reg-dup", List.of(TOPIC), StartFrom.LATEST);
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.listGroups()).anyMatch(g -> g.groupId().equals("grp-reg-dup")));
        assertThatThrownBy(() -> commands.registerGroup("grp-reg-dup", List.of(TOPIC), StartFrom.LATEST))
                .isInstanceOf(GroupExistsException.class);
    }

    @Test
    void 없는_토픽이면_UnknownTopic_예외() {
        assertThatThrownBy(() -> commands.registerGroup("grp-reg-ghost", List.of("no-such-topic"), StartFrom.LATEST))
                .isInstanceOf(UnknownTopicOrPartitionException.class);
    }

    @Test
    void 그룹명이_비었거나_토픽이_없으면_거부된다() {
        assertThatThrownBy(() -> commands.registerGroup(" ", List.of(TOPIC), StartFrom.LATEST))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> commands.registerGroup("grp-reg-x", List.of(), StartFrom.LATEST))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
