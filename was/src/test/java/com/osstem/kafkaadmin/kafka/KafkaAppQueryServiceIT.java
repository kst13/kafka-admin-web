package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppSummary;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicPermission;
import com.osstem.kafkaadmin.ops.KafkaApp;
import com.osstem.kafkaadmin.ops.KafkaAppNotFoundException;
import com.osstem.kafkaadmin.ops.KafkaAppRepository;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.acl.AclBinding;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static java.time.Duration.ofSeconds;

class KafkaAppQueryServiceIT extends KafkaSecureIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired KafkaAppQueryService queries;
    @Autowired KafkaAppRepository repository;

    private void scram(String user) throws Exception {
        admin.alterUserScramCredentials(List.of(new UserScramCredentialUpsertion(user,
                new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096), "pw"))).all().get();
    }

    @Test
    void 등록_계정과_미등록_계정을_구분해_나열하고_상세는_권한을_역매핑한다() throws Exception {
        scram("q-t-registered");
        scram("q-t-unregistered");
        repository.save(new KafkaApp("q-t-registered", "dev1", "조회 테스트", Instant.now()));
        List<AclBinding> acls = new ArrayList<>(AclMapping.topicBindings("q-t-registered", "q-t-orders", PermissionMode.BOTH));
        acls.addAll(AclMapping.topicBindings("q-t-registered", "q-t-events", PermissionMode.CONSUME));
        acls.add(AclMapping.groupBinding("q-t-registered"));
        admin.createAcls(acls).all().get();

        await().atMost(ofSeconds(10)).untilAsserted(() -> {
            List<KafkaAppSummary> list = queries.listApps();
            assertThat(list).anySatisfy(s -> {
                assertThat(s.name()).isEqualTo("q-t-registered");
                assertThat(s.registered()).isTrue();
                assertThat(s.owner()).isEqualTo("dev1");
                assertThat(s.topicCount()).isEqualTo(2);
            });
            assertThat(list).anySatisfy(s -> {
                assertThat(s.name()).isEqualTo("q-t-unregistered");
                assertThat(s.registered()).isFalse();
                assertThat(s.topicCount()).isZero();
            });
        });

        KafkaAppDetail d = queries.describeApp("q-t-registered");
        assertThat(d.permissions()).containsExactly(
                new TopicPermission("q-t-events", "consume"),
                new TopicPermission("q-t-orders", "both"));
        assertThat(d.otherAcls()).isEmpty();
        assertThat(d.registered()).isTrue();
        assertThat(d.createdAt()).isNotNull();

        KafkaAppDetail u = queries.describeApp("q-t-unregistered");
        assertThat(u.registered()).isFalse();
        assertThat(u.owner()).isNull();
        assertThat(u.permissions()).isEmpty();
    }

    @Test
    void 어디에도_없는_이름은_NotFound() {
        assertThatThrownBy(() -> queries.describeApp("q-t-ghost"))
                .isInstanceOf(KafkaAppNotFoundException.class);
    }
}
