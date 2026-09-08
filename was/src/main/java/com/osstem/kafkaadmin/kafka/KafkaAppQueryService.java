package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppSummary;
import com.osstem.kafkaadmin.ops.KafkaApp;
import com.osstem.kafkaadmin.ops.KafkaAppNotFoundException;
import com.osstem.kafkaadmin.ops.KafkaAppRepository;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

// Kafka 앱 계정 조회. 원본은 브로커(SCRAM 사용자 목록 + ACL), 메타데이터(담당자·설명)는 H2.
// 브로커에는 있는데 메타데이터가 없는 계정(kafka-admin, admin 등)은 registered=false 로 나열만 한다.
@Service
public class KafkaAppQueryService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAppQueryService.class);

    // AdminClient 는 호출마다 least-loaded 브로커로 라우팅하므로, 방금 쓴 브로커와 다른 브로커가 응답할 수
    // 있다(교차 브로커 전파 지연). 변경 직후 응답에 자기 쓰기를 반영하기 위해 최대 약 3초(30 x 100ms) 재조회한다.
    private static final int VISIBILITY_ATTEMPTS = 30;
    private static final long VISIBILITY_INTERVAL_MS = 100;

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

    // 변경 응답용: 자기 쓰기 읽기(read-your-write)를 보장하기 위해 기대 상태가 보일 때까지 재조회한다.
    // describeApp 은 매 호출마다 AdminClient 가 고른 브로커로 갈 수 있어, 한 번의 대기만으로는 그 브로커가
    // 여전히 뒤처져 있을 수 있다 — 그래서 매번 다시 조회해 실제로 기대한 상태가 보이는지 확인한다.
    public KafkaAppDetail describeAppUntil(String name, Predicate<KafkaAppDetail> expected) {
        KafkaAppDetail lastGood = null;
        for (int i = 0; i < VISIBILITY_ATTEMPTS; i++) {
            KafkaAppDetail detail;
            try {
                detail = describeApp(name);
            } catch (KafkaUnavailableException e) {
                // 쓰기는 이미 성공했다 — 그 뒤의 재조회 한 번이 실패했다고 503 으로 번지게 두지 않는다.
                // 이전에 읽은 상태가 있으면 그걸 돌려주고, 첫 조회부터 실패하면(돌려줄 게 없음) 그대로 던진다.
                if (lastGood != null) {
                    log.warn("앱 {} 상태 재조회 실패 (마지막으로 읽은 상태 반환): {}", name, e.toString());
                    return lastGood;
                }
                throw e;
            }
            lastGood = detail;
            if (expected.test(detail)) return detail;
            try {
                Thread.sleep(VISIBILITY_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return detail;
            }
        }
        log.warn("앱 {} 상세가 {}ms 안에 기대 상태로 전파되지 않았다 (마지막 조회 결과 반환)",
                name, VISIBILITY_ATTEMPTS * VISIBILITY_INTERVAL_MS);
        return lastGood;
    }
}
