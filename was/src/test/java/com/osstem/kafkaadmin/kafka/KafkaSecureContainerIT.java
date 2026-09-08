package com.osstem.kafkaadmin.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;

// 베이스 컨테이너가 의도대로 떠 있는지: PLAINTEXT 는 super user, SASL 은 SCRAM 인증 + ACL 인가.
class KafkaSecureContainerIT extends KafkaSecureIntegrationTestBase {

    @Autowired Admin admin;

    @Test
    void SCRAM_계정으로_인증하고_ACL_없는_토픽은_거부된다() throws Exception {
        admin.createTopics(List.of(new NewTopic("secure-t-smoke", 1, (short) 1))).all().get();
        admin.alterUserScramCredentials(List.of(new UserScramCredentialUpsertion("secure-t-user",
                new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096), "secure-t-pw"))).all().get();
        await().atMost(ofSeconds(10)).untilAsserted(() ->
                assertThat(admin.describeUserScramCredentials().all().get()).containsKey("secure-t-user"));

        Map<String, Object> props = saslClientProps("secure-t-user", "secure-t-pw");
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>("secure-t-smoke", "k", "v")).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
        }

        admin.createAcls(List.of(AclMapping.topicBindings("secure-t-user", "secure-t-smoke",
                PermissionMode.PRODUCE).get(0))).all().get();
        await().atMost(ofSeconds(10)).untilAsserted(() -> {
            try (KafkaProducer<String, String> producer =
                         new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
                producer.send(new ProducerRecord<>("secure-t-smoke", "k", "v")).get();
            }
        });
        List<AclBinding> acls = List.copyOf(admin.describeAcls(AclMapping.principalFilter("secure-t-user")).values().get());
        assertThat(acls).hasSize(1);
    }
}
