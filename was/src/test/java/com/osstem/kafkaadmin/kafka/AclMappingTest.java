package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.RawAcl;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicPermission;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class AclMappingTest {

    private static AclBinding acl(ResourceType type, String name, PatternType pattern, AclOperation op) {
        return new AclBinding(new ResourcePattern(type, name, pattern),
                new AccessControlEntry("User:order-api", "*", op, AclPermissionType.ALLOW));
    }

    @Test
    void produce는_토픽_WRITE_하나() {
        List<AclBinding> b = AclMapping.topicBindings("order-api", "orders", PermissionMode.PRODUCE);
        assertThat(b).containsExactly(acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.WRITE));
    }

    @Test
    void consume는_토픽_READ_하나이고_그룹은_별도_바인딩() {
        assertThat(AclMapping.topicBindings("order-api", "orders", PermissionMode.CONSUME))
                .containsExactly(acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ));
        assertThat(AclMapping.groupBinding("order-api"))
                .isEqualTo(acl(ResourceType.GROUP, "order-api", PatternType.PREFIXED, AclOperation.READ));
    }

    @Test
    void both는_WRITE와_READ() {
        assertThat(AclMapping.topicBindings("order-api", "orders", PermissionMode.BOTH))
                .containsExactlyInAnyOrder(
                        acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.WRITE),
                        acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ));
    }

    @Test
    void 역매핑은_토픽별_모드를_계산하고_그룹_ACL은_기타에_넣지_않는다() {
        AclMapping.Derived d = AclMapping.derive("order-api", List.of(
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.WRITE),
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.READ),
                acl(ResourceType.TOPIC, "events", PatternType.LITERAL, AclOperation.READ),
                acl(ResourceType.GROUP, "order-api", PatternType.PREFIXED, AclOperation.READ)));
        assertThat(d.permissions()).containsExactly(
                new TopicPermission("events", "consume"),
                new TopicPermission("orders", "both"));
        assertThat(d.otherAcls()).isEmpty();
        assertThat(d.hasConsume()).isTrue();
    }

    @Test
    void 규칙_밖_ACL은_기타로_노출된다() {
        AclMapping.Derived d = AclMapping.derive("order-api", List.of(
                acl(ResourceType.TOPIC, "ord", PatternType.PREFIXED, AclOperation.WRITE),
                acl(ResourceType.TOPIC, "orders", PatternType.LITERAL, AclOperation.DESCRIBE),
                acl(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL, AclOperation.IDEMPOTENT_WRITE)));
        assertThat(d.permissions()).isEmpty();
        assertThat(d.otherAcls()).containsExactlyInAnyOrder(
                new RawAcl("TOPIC", "PREFIXED", "ord", "WRITE"),
                new RawAcl("TOPIC", "LITERAL", "orders", "DESCRIBE"),
                new RawAcl("CLUSTER", "LITERAL", "kafka-cluster", "IDEMPOTENT_WRITE"));
        assertThat(d.hasConsume()).isFalse();
    }

    @Test
    void 필터는_principal과_리소스를_정확히_지정한다() {
        assertThat(AclMapping.principalFilter("order-api").entryFilter().principal()).isEqualTo("User:order-api");
        assertThat(AclMapping.topicFilter("order-api", "orders").patternFilter().name()).isEqualTo("orders");
        assertThat(AclMapping.topicFilter("order-api", "orders").patternFilter().patternType()).isEqualTo(PatternType.LITERAL);
        assertThat(AclMapping.groupFilter("order-api").patternFilter().resourceType()).isEqualTo(ResourceType.GROUP);
        assertThat(AclMapping.groupFilter("order-api").patternFilter().patternType()).isEqualTo(PatternType.PREFIXED);
    }

    @Test
    void 모드_파싱은_대소문자를_무시하고_이상값은_IllegalArgument() {
        assertThat(PermissionMode.parse("Produce")).isEqualTo(PermissionMode.PRODUCE);
        assertThat(PermissionMode.BOTH.value()).isEqualTo("both");
        assertThatThrownBy(() -> PermissionMode.parse("admin")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PermissionMode.parse(null)).isInstanceOf(IllegalArgumentException.class);
    }
}
