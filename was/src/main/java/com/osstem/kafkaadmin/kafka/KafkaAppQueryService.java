package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppSummary;
import com.osstem.kafkaadmin.ops.KafkaApp;
import com.osstem.kafkaadmin.ops.KafkaAppNotFoundException;
import com.osstem.kafkaadmin.ops.KafkaAppRepository;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.springframework.stereotype.Service;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

// Kafka 앱 계정 조회. 원본은 브로커(SCRAM 사용자 목록 + ACL), 메타데이터(담당자·설명)는 H2.
// 브로커에는 있는데 메타데이터가 없는 계정(kafka-admin, admin 등)은 registered=false 로 나열만 한다.
@Service
public class KafkaAppQueryService {

    private final Admin admin;
    private final KafkaAppRepository repository;

    public KafkaAppQueryService(Admin admin, KafkaAppRepository repository) {
        this.admin = admin;
        this.repository = repository;
    }

    public Set<String> scramUsers() {
        return KafkaFutures.await(admin.describeUserScramCredentials().all()).keySet();
    }

    public List<KafkaAppSummary> listApps() {
        Set<String> names = new TreeSet<>(scramUsers());
        Map<String, KafkaApp> meta = repository.findAll().stream()
                .collect(Collectors.toMap(KafkaApp::getName, a -> a));
        names.addAll(meta.keySet());
        Collection<AclBinding> allAcls = KafkaFutures.await(admin.describeAcls(AclBindingFilter.ANY).values());
        return names.stream().map(name -> {
            KafkaApp app = meta.get(name);
            int topicCount = AclMapping.derive(name, allAcls).permissions().size();
            return new KafkaAppSummary(name,
                    app == null ? null : app.getOwnerUsername(),
                    app == null ? null : app.getDescription(),
                    app != null, topicCount);
        }).toList();
    }

    public KafkaAppDetail describeApp(String name) {
        Optional<KafkaApp> app = repository.findByName(name);
        if (app.isEmpty() && !scramUsers().contains(name)) {
            throw new KafkaAppNotFoundException(name);
        }
        Collection<AclBinding> acls = KafkaFutures.await(
                admin.describeAcls(AclMapping.principalFilter(name)).values());
        AclMapping.Derived derived = AclMapping.derive(name, acls);
        return new KafkaAppDetail(name,
                app.map(KafkaApp::getOwnerUsername).orElse(null),
                app.map(KafkaApp::getDescription).orElse(null),
                app.map(KafkaApp::getCreatedAt).orElse(null),
                app.isPresent(), derived.permissions(), derived.otherAcls());
    }
}
