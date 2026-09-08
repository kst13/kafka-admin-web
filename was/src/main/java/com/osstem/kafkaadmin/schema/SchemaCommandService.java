package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.kafka.TopicQueryService;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.CompatibilityResult;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.RegisteredSchema;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SchemaVersion;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Service;
import java.nio.charset.StandardCharsets;
import java.util.List;

// Schema Registry 변경. 등록은 호환성 검사를 통과해야 하고, 감사 로그는 컨트롤러가 AuditRecorder 로 감싼다.
@Service
public class SchemaCommandService {

    public static final int MAX_SCHEMA_BYTES = 1_048_576;

    private final SchemaRegistryClient client;
    private final TopicQueryService topics;

    public SchemaCommandService(SchemaRegistryClient client, TopicQueryService topics) {
        this.client = client;
        this.topics = topics;
    }

    public CompatibilityResult checkCompatibility(String topic, SubjectKind kind, SchemaType type, String schema) {
        validateSchema(schema);
        return client.testCompatibility(SubjectName.of(topic, kind), type, schema);
    }

    public RegisteredSchema register(String topic, SubjectKind kind, SchemaType type, String schema) {
        validateSchema(schema);
        String subject = SubjectName.of(topic, kind);
        requireTopic(topic);
        CompatibilityResult check = client.testCompatibility(subject, type, schema);
        if (!check.compatible()) throw new IncompatibleSchemaException(check.messages());
        int id = client.register(subject, type, schema);
        SchemaVersion latest = client.version(subject, "latest");
        return new RegisteredSchema(subject, id, latest.version());
    }

    // level 이 null 이면 서브젝트 설정을 지워 전역을 상속한다. 없는 서브젝트에 고아 설정이 남지 않도록 존재를 먼저 확인.
    public void setCompatibility(String subject, CompatibilityLevel level) {
        client.versions(subject);
        if (level == null) client.deleteSubjectConfig(subject);
        else client.setSubjectConfig(subject, level);
    }

    public void setGlobalCompatibility(CompatibilityLevel level) {
        client.setGlobalConfig(level);
    }

    public List<Integer> deleteSubject(String subject) {
        return client.deleteSubject(subject);
    }

    // listTopics 는 없는 토픽을 조용히 건너뛰므로 여기서 직접 404 용 예외로 바꾼다 (describeTopic 은 접속불가로 감싸 503 이 된다)
    private void requireTopic(String topic) {
        boolean exists = topics.listTopics().stream().anyMatch(t -> t.name().equals(topic));
        if (!exists) throw new UnknownTopicOrPartitionException("topic " + topic + " not found");
    }

    private static void validateSchema(String schema) {
        if (schema == null || schema.isBlank()) throw new IllegalArgumentException("스키마 본문을 입력하세요");
        if (schema.getBytes(StandardCharsets.UTF_8).length > MAX_SCHEMA_BYTES) {
            throw new IllegalArgumentException("스키마 본문은 1 MB 이하여야 합니다");
        }
    }
}
