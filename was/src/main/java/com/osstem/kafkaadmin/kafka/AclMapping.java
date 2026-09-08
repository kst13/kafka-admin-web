package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.RawAcl;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicPermission;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// 권한(produce/consume/both) <-> Kafka ACL 변환의 단일 출처. 조회(역매핑)와 변경(바인딩 생성)이 같은 규칙을 쓴다.
// 규칙: Topic LITERAL WRITE = produce, READ = consume, 둘 다 = both. consume 이 하나라도 있으면 Group PREFIXED <app> READ.
// DESCRIBE 는 READ/WRITE 가 암묵 허용하므로 만들지 않는다. host "*", ALLOW 고정.
public final class AclMapping {
    private AclMapping() {}

    public static String principal(String app) { return "User:" + app; }

    public static List<AclBinding> topicBindings(String app, String topic, PermissionMode mode) {
        List<AclBinding> out = new ArrayList<>();
        if (mode.canWrite()) out.add(topicAcl(app, topic, AclOperation.WRITE));
        if (mode.canRead()) out.add(topicAcl(app, topic, AclOperation.READ));
        return out;
    }

    public static AclBinding groupBinding(String app) {
        return new AclBinding(new ResourcePattern(ResourceType.GROUP, app, PatternType.PREFIXED),
                allow(app, AclOperation.READ));
    }

    public static AclBindingFilter principalFilter(String app) {
        return new AclBindingFilter(ResourcePatternFilter.ANY, entryFilter(app));
    }

    public static AclBindingFilter topicFilter(String app, String topic) {
        return new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.TOPIC, topic, PatternType.LITERAL), entryFilter(app));
    }

    public static AclBindingFilter groupFilter(String app) {
        return new AclBindingFilter(
                new ResourcePatternFilter(ResourceType.GROUP, app, PatternType.PREFIXED), entryFilter(app));
    }

    public record Derived(List<TopicPermission> permissions, List<RawAcl> otherAcls) {
        public boolean hasConsume() {
            return permissions.stream().anyMatch(p -> !p.mode().equals(PermissionMode.PRODUCE.value()));
        }
    }

    // 토픽명 오름차순 권한 목록 + 규칙 밖 ACL(기타). 그룹 PREFIXED <app> READ 는 규칙의 일부라 기타에 넣지 않는다.
    public static Derived derive(String app, Collection<AclBinding> acls) {
        Map<String, EnumSet<AclOperation>> ops = new TreeMap<>();
        List<RawAcl> others = new ArrayList<>();
        AclBinding group = groupBinding(app);
        for (AclBinding b : acls) {
            if (!b.entry().principal().equals(principal(app))) continue;
            if (b.equals(group)) continue;
            ResourcePattern p = b.pattern();
            AclOperation op = b.entry().operation();
            boolean topicRule = p.resourceType() == ResourceType.TOPIC
                    && p.patternType() == PatternType.LITERAL
                    && (op == AclOperation.READ || op == AclOperation.WRITE)
                    && b.entry().permissionType() == AclPermissionType.ALLOW
                    && "*".equals(b.entry().host());
            if (topicRule) {
                ops.computeIfAbsent(p.name(), k -> EnumSet.noneOf(AclOperation.class)).add(op);
            } else {
                others.add(new RawAcl(p.resourceType().name(), p.patternType().name(), p.name(), op.name()));
            }
        }
        List<TopicPermission> perms = new ArrayList<>();
        ops.forEach((topic, set) -> {
            PermissionMode mode = set.contains(AclOperation.WRITE)
                    ? (set.contains(AclOperation.READ) ? PermissionMode.BOTH : PermissionMode.PRODUCE)
                    : PermissionMode.CONSUME;
            perms.add(new TopicPermission(topic, mode.value()));
        });
        return new Derived(perms, others);
    }

    private static AclBinding topicAcl(String app, String topic, AclOperation op) {
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL), allow(app, op));
    }

    private static AccessControlEntry allow(String app, AclOperation op) {
        return new AccessControlEntry(principal(app), "*", op, AclPermissionType.ALLOW);
    }

    private static AccessControlEntryFilter entryFilter(String app) {
        return new AccessControlEntryFilter(principal(app), null, AclOperation.ANY, AclPermissionType.ANY);
    }
}
