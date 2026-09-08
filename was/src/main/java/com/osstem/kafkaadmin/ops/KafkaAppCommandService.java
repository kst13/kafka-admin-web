package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.AclMapping;
import com.osstem.kafkaadmin.kafka.PermissionMode;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

// Kafka 앱 계정 변경. 브로커(SCRAM·ACL)가 원본이고 메타데이터는 H2. 감사 로그는 컨트롤러가 AuditRecorder 로 감싼다.
// 비밀번호는 여기서 생성해 반환값으로만 나간다 — 입력 DTO 에도, 로그에도 두지 않는다.
@Service
public class KafkaAppCommandService {

    private static final Pattern APP_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,64}");
    private static final ScramCredentialInfo SCRAM = new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096);
    private static final Logger log = LoggerFactory.getLogger(KafkaAppCommandService.class);

    private final Admin admin;
    private final KafkaAppRepository repository;

    public KafkaAppCommandService(Admin admin, KafkaAppRepository repository) {
        this.admin = admin;
        this.repository = repository;
    }

    public String create(String name, String owner, String description) {
        validateName(name);
        if (repository.existsByName(name) || scramExists(name)) {
            throw new KafkaAppExistsException(name);
        }
        String password = PasswordGenerator.generate();
        upsertScram(name, password);
        repository.save(new KafkaApp(name, blankToNull(owner), blankToNull(description), Instant.now()));
        return password;
    }

    // 브로커에만 있는 계정에 메타데이터를 붙인다 (생성 도중 실패 복구, 수동 생성 계정 편입). 비밀번호는 바꾸지 않는다.
    public void register(String name, String owner, String description) {
        validateName(name);
        if (repository.existsByName(name)) throw new KafkaAppExistsException(name);
        if (!scramExists(name)) throw new KafkaAppNotFoundException(name);
        repository.save(new KafkaApp(name, blankToNull(owner), blankToNull(description), Instant.now()));
    }

    public String resetPassword(String name) {
        requireRegistered(name);
        String password = PasswordGenerator.generate();
        upsertScram(name, password);
        return password;
    }

    // ACL 전부 제거 -> SCRAM 삭제 -> 메타데이터 삭제. SCRAM 이 이미 없어도(수동 삭제) 나머지는 진행한다.
    public void delete(String name) {
        requireRegistered(name);
        OpsFutures.await(admin.deleteAcls(List.of(AclMapping.principalFilter(name))).all());
        try {
            OpsFutures.await(admin.alterUserScramCredentials(
                    List.of(new UserScramCredentialDeletion(name, ScramMechanism.SCRAM_SHA_512))).all());
        } catch (ResourceNotFoundException e) {
            log.warn("SCRAM 계정 {} 이 브로커에 없어 메타데이터만 삭제한다", name);
        }
        repository.deleteByName(name);
    }

    public void setTopicPermission(String name, String topic, PermissionMode mode) {
        requireRegistered(name);
        if (mode == null) throw new IllegalArgumentException("mode 는 produce, consume, both 중 하나여야 합니다");
        OpsFutures.await(admin.describeTopics(List.of(topic)).allTopicNames()); // 없는 토픽 -> UnknownTopicOrPartition
        OpsFutures.await(admin.deleteAcls(List.of(AclMapping.topicFilter(name, topic))).all());
        OpsFutures.await(admin.createAcls(AclMapping.topicBindings(name, topic, mode)).all());
        reconcileGroupAcl(name);
    }

    public void revokeTopicPermission(String name, String topic) {
        requireRegistered(name);
        OpsFutures.await(admin.deleteAcls(List.of(AclMapping.topicFilter(name, topic))).all());
        reconcileGroupAcl(name);
    }

    // consume 이 하나라도 남아 있으면 그룹 READ 를 보장(중복 생성은 브로커가 무시), 없으면 제거
    private void reconcileGroupAcl(String name) {
        Collection<AclBinding> acls = OpsFutures.await(admin.describeAcls(AclMapping.principalFilter(name)).values());
        if (AclMapping.derive(name, acls).hasConsume()) {
            OpsFutures.await(admin.createAcls(List.of(AclMapping.groupBinding(name))).all());
        } else {
            OpsFutures.await(admin.deleteAcls(List.of(AclMapping.groupFilter(name))).all());
        }
    }

    private void upsertScram(String name, String password) {
        OpsFutures.await(admin.alterUserScramCredentials(
                List.of(new UserScramCredentialUpsertion(name, SCRAM, password))).all());
    }

    private boolean scramExists(String name) {
        return OpsFutures.await(admin.describeUserScramCredentials().all()).containsKey(name);
    }

    private void requireRegistered(String name) {
        repository.findByName(name).orElseThrow(() -> new KafkaAppNotFoundException(name));
    }

    private static void validateName(String name) {
        if (name == null || !APP_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException("앱 이름은 영문·숫자·'.', '_', '-' 만 사용해 64자 이하로 지정합니다");
        }
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s.trim();
    }
}
