package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.KafkaAppQueryService;
import com.osstem.kafkaadmin.kafka.KafkaSecureIntegrationTestBase;
import com.osstem.kafkaadmin.kafka.PermissionMode;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicPermission;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;

class KafkaAppCommandServiceIT extends KafkaSecureIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired KafkaAppCommandService commands;
    @Autowired KafkaAppQueryService queries;
    @Autowired KafkaAppRepository repository;

    private static void produce(Map<String, Object> props, String topic) throws Exception {
        try (KafkaProducer<String, String> p = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            p.send(new ProducerRecord<>(topic, "k", "v")).get();
        }
    }

    private static ConsumerRecords<String, String> consume(Map<String, Object> props, String topic, String group) {
        Map<String, Object> c = new java.util.HashMap<>(props);
        c.put("group.id", group);
        c.put("auto.offset.reset", "earliest");
        try (KafkaConsumer<String, String> k = new KafkaConsumer<>(c, new StringDeserializer(), new StringDeserializer())) {
            k.subscribe(List.of(topic));
            return k.poll(Duration.ofSeconds(5));
        }
    }

    @Test
    void 생성_권한부여_변경_회수_재발급_삭제_왕복() throws Exception {
        admin.createTopics(List.of(new NewTopic("cmd-t-orders", 1, (short) 1))).all().get();

        String pw = commands.create("cmd-t-app", "dev1", "왕복");
        assertThat(pw).hasSize(24);
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.scramUsers()).contains("cmd-t-app"));
        Map<String, Object> client = saslClientProps("cmd-t-app", pw);

        // 권한 없음 -> produce 거부
        assertThatThrownBy(() -> produce(client, "cmd-t-orders"))
                .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(TopicAuthorizationException.class);

        // produce 부여 -> 성공, consume 은 거부
        commands.setTopicPermission("cmd-t-app", "cmd-t-orders", PermissionMode.PRODUCE);
        await().atMost(ofSeconds(10)).untilAsserted(() -> produce(client, "cmd-t-orders"));
        assertThatThrownBy(() -> consume(client, "cmd-t-orders", "cmd-t-app-reader"))
                .isInstanceOfAny(TopicAuthorizationException.class, GroupAuthorizationException.class);

        // both 로 변경 -> 앱 이름 접두어 그룹으로 consume 성공
        commands.setTopicPermission("cmd-t-app", "cmd-t-orders", PermissionMode.BOTH);
        await().atMost(ofSeconds(15)).untilAsserted(() ->
                assertThat(consume(client, "cmd-t-orders", "cmd-t-app-reader").count()).isGreaterThan(0));
        KafkaAppDetail d = queries.describeApp("cmd-t-app");
        assertThat(d.permissions()).containsExactly(new TopicPermission("cmd-t-orders", "both"));

        // 접두어가 다른 그룹은 거부
        assertThatThrownBy(() -> consume(client, "cmd-t-orders", "other-reader"))
                .isInstanceOf(GroupAuthorizationException.class);

        // 회수 -> 권한·그룹 ACL 모두 사라짐
        commands.revokeTopicPermission("cmd-t-app", "cmd-t-orders");
        await().atMost(ofSeconds(10)).untilAsserted(() -> {
            KafkaAppDetail after = queries.describeApp("cmd-t-app");
            assertThat(after.permissions()).isEmpty();
            assertThat(after.otherAcls()).isEmpty();
        });
        assertThat(admin.describeAcls(com.osstem.kafkaadmin.kafka.AclMapping.principalFilter("cmd-t-app")).values().get()).isEmpty();

        // 재발급 -> 옛 비밀번호 실패, 새 비밀번호 성공(권한 다시 부여)
        String pw2 = commands.resetPassword("cmd-t-app");
        assertThat(pw2).isNotEqualTo(pw);
        commands.setTopicPermission("cmd-t-app", "cmd-t-orders", PermissionMode.PRODUCE);
        await().atMost(ofSeconds(10)).untilAsserted(() -> produce(saslClientProps("cmd-t-app", pw2), "cmd-t-orders"));
        // 인증 실패는 kafka-clients 버전에 따라 send() 에서 바로 던지거나 future 에 실린다
        assertThatThrownBy(() -> produce(saslClientProps("cmd-t-app", pw), "cmd-t-orders"))
                .isInstanceOfAny(ExecutionException.class,
                        org.apache.kafka.common.errors.AuthenticationException.class,
                        org.apache.kafka.common.errors.TimeoutException.class);

        // 삭제 -> SCRAM·ACL·메타데이터 모두 제거
        commands.delete("cmd-t-app");
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(queries.scramUsers()).doesNotContain("cmd-t-app"));
        assertThat(repository.existsByName("cmd-t-app")).isFalse();
        assertThat(admin.describeAcls(com.osstem.kafkaadmin.kafka.AclMapping.principalFilter("cmd-t-app")).values().get()).isEmpty();
    }

    @Test
    void 중복_생성은_409이고_미등록_계정은_등록으로_편입한다() throws Exception {
        commands.create("cmd-t-dup", null, null);
        assertThatThrownBy(() -> commands.create("cmd-t-dup", null, null))
                .isInstanceOf(KafkaAppExistsException.class);

        // 브로커에만 있는 계정 (메타데이터 삭제로 흉내)
        repository.deleteByName("cmd-t-dup");
        assertThatThrownBy(() -> commands.resetPassword("cmd-t-dup"))
                .isInstanceOf(KafkaAppNotFoundException.class);
        commands.register("cmd-t-dup", "dev2", "편입");
        assertThat(queries.describeApp("cmd-t-dup").registered()).isTrue();
        assertThat(queries.describeApp("cmd-t-dup").owner()).isEqualTo("dev2");
    }
}
