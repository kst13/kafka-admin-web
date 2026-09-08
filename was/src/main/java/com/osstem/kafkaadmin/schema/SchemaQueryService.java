package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

// Schema Registry 조회 조합. 호환성은 서브젝트 지정(SUBJECT)이 있으면 그것, 없으면 전역(GLOBAL)을 상속한다.
@Service
public class SchemaQueryService {

    private static final Logger log = LoggerFactory.getLogger(SchemaQueryService.class);

    private final SchemaRegistryClient client;

    public SchemaQueryService(SchemaRegistryClient client) {
        this.client = client;
    }

    // globalConfig() 실패(Registry 장애)로 메뉴·기능 전체가 숨겨지지 않도록, 설정은 됐으나 조회는 실패한 상태를 구분해 돌려준다.
    public SchemaRegistryStatus status() {
        if (!client.configured()) return new SchemaRegistryStatus(false, List.of(), null);
        try {
            return new SchemaRegistryStatus(true, client.urls(), client.globalConfig().name());
        } catch (SchemaRegistryUnavailableException e) {
            log.warn("Schema Registry 전역 설정 조회 실패, 메뉴는 유지: {}", e.getMessage());
            return new SchemaRegistryStatus(true, client.urls(), null);
        }
    }

    public List<SubjectSummary> listSubjects() {
        CompatibilityLevel global = client.globalConfig();
        List<SubjectSummary> out = new ArrayList<>();
        for (String subject : client.subjects()) {
            out.add(summary(subject, global));
        }
        return out;
    }

    public SubjectDetail describeSubject(String subject) {
        CompatibilityLevel global = client.globalConfig();
        List<Integer> versions = client.versions(subject); // 없으면 SubjectNotFoundException
        List<SchemaVersionSummary> summaries = versions.stream()
                .map(v -> client.version(subject, String.valueOf(v)))
                .map(v -> new SchemaVersionSummary(v.version(), v.id(), v.schemaType()))
                .sorted(Comparator.comparingInt(SchemaVersionSummary::version).reversed())
                .toList();
        SubjectName name = SubjectName.parse(subject);
        Optional<CompatibilityLevel> cfg = client.subjectConfig(subject);
        return new SubjectDetail(subject, name.topic(), name.kind().value(),
                cfg.orElse(global).name(), cfg.isPresent() ? "SUBJECT" : "GLOBAL", summaries);
    }

    public SchemaVersion getVersion(String subject, String version) {
        return client.version(subject, version);
    }

    public TopicSchemas topicSchemas(String topic) {
        String keySubject = SubjectName.of(topic, SubjectKind.KEY);     // 토픽명 검증 포함
        String valueSubject = SubjectName.of(topic, SubjectKind.VALUE);
        CompatibilityLevel global = client.globalConfig();
        return new TopicSchemas(topic, summaryOrNull(keySubject, global), summaryOrNull(valueSubject, global));
    }

    private SubjectSummary summaryOrNull(String subject, CompatibilityLevel global) {
        try {
            return summary(subject, global);
        } catch (SubjectNotFoundException e) {
            return null;
        }
    }

    private SubjectSummary summary(String subject, CompatibilityLevel global) {
        SubjectName name = SubjectName.parse(subject);
        SchemaVersion latest = client.version(subject, "latest");
        Optional<CompatibilityLevel> cfg = client.subjectConfig(subject);
        return new SubjectSummary(subject, name.topic(), name.kind().value(), latest.version(), latest.schemaType(),
                cfg.orElse(global).name(), cfg.isPresent() ? "SUBJECT" : "GLOBAL");
    }
}
