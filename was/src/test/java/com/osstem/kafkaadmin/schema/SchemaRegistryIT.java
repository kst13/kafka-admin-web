package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestClient;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class SchemaRegistryIT extends SchemaRegistryIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired SchemaQueryService queries;
    @Autowired SchemaCommandService commands;

    private static final String V1 = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
    private static final String V2_COMPATIBLE = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"qty\",\"type\":\"int\",\"default\":0}]}";
    // BACKWARD 위반: 기본값 없는 필드 추가 (새 스키마로 옛 데이터를 읽을 수 없다)
    private static final String V3_INCOMPATIBLE = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"qty\",\"type\":\"int\",\"default\":0},{\"name\":\"price\",\"type\":\"double\"}]}";

    @Test
    void 등록_조회_호환성_삭제_왕복() throws Exception {
        admin.createTopics(List.of(new NewTopic("sr-t-orders", 1, (short) 1))).all().get();
        String subject = "sr-t-orders-value";

        assertThat(commands.checkCompatibility("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V1).compatible()).isTrue();
        RegisteredSchema r1 = commands.register("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V1);
        assertThat(r1.subject()).isEqualTo(subject);
        assertThat(r1.version()).isEqualTo(1);

        assertThat(queries.status().configured()).isTrue();
        assertThat(queries.listSubjects()).anySatisfy(s -> {
            assertThat(s.subject()).isEqualTo(subject);
            assertThat(s.topic()).isEqualTo("sr-t-orders");
            assertThat(s.kind()).isEqualTo("value");
            assertThat(s.latestVersion()).isEqualTo(1);
            assertThat(s.schemaType()).isEqualTo("AVRO");
            assertThat(s.compatibilitySource()).isEqualTo("GLOBAL");
        });
        assertThat(queries.getVersion(subject, "latest").schema()).contains("\"name\":\"Order\"");
        TopicSchemas ts = queries.topicSchemas("sr-t-orders");
        assertThat(ts.key()).isNull();
        assertThat(ts.value().latestVersion()).isEqualTo(1);

        RegisteredSchema r2 = commands.register("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V2_COMPATIBLE);
        assertThat(r2.version()).isEqualTo(2);

        CompatibilityResult bad = commands.checkCompatibility("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V3_INCOMPATIBLE);
        assertThat(bad.compatible()).isFalse();
        assertThat(bad.messages()).isNotEmpty();
        assertThatThrownBy(() -> commands.register("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V3_INCOMPATIBLE))
                .isInstanceOf(IncompatibleSchemaException.class);
        assertThat(queries.describeSubject(subject).versions()).extracting(SchemaVersionSummary::version).containsExactly(2, 1);

        commands.setCompatibility(subject, CompatibilityLevel.NONE);
        SubjectDetail d = queries.describeSubject(subject);
        assertThat(d.compatibility()).isEqualTo("NONE");
        assertThat(d.compatibilitySource()).isEqualTo("SUBJECT");
        assertThat(commands.register("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, V3_INCOMPATIBLE).version()).isEqualTo(3);

        commands.setCompatibility(subject, null);
        assertThat(queries.describeSubject(subject).compatibilitySource()).isEqualTo("GLOBAL");

        commands.setGlobalCompatibility(CompatibilityLevel.FULL);
        assertThat(queries.status().globalCompatibility()).isEqualTo("FULL");
        commands.setGlobalCompatibility(CompatibilityLevel.BACKWARD);

        // 실서버 관찰: 기존 버전이 있는 subject 에 파싱 불가 스키마를 등록하면 Registry 의 compatibility 체크가
        // 422 파싱 오류가 아니라 HTTP 200 + is_compatible=false 로 응답한다 → SchemaCommandService.register() 는
        // 먼저 호출하는 testCompatibility() 단계에서 IncompatibleSchemaException 을 던진다.
        // InvalidSchemaException(422) 매핑 자체는 SchemaRegistryClientTest#잘못된_스키마_42201은_400용_예외() 로 검증됨.
        assertThatThrownBy(() -> commands.register("sr-t-orders", SubjectKind.VALUE, SchemaType.AVRO, "not a schema"))
                .isInstanceOf(IncompatibleSchemaException.class);

        assertThat(commands.deleteSubject(subject)).containsExactly(1, 2, 3);
        assertThatThrownBy(() -> queries.describeSubject(subject)).isInstanceOf(SubjectNotFoundException.class);
        assertThat(queries.topicSchemas("sr-t-orders").value()).isNull();
    }

    @Test
    void 없는_토픽_등록은_404용_예외이고_Registry는_호출하지_않는다() {
        assertThatThrownBy(() -> commands.register("sr-t-ghost", SubjectKind.VALUE, SchemaType.AVRO, V1))
                .isInstanceOf(org.apache.kafka.common.errors.UnknownTopicOrPartitionException.class);
        assertThat(queries.listSubjects()).noneMatch(s -> s.subject().startsWith("sr-t-ghost"));
    }

    @Test
    void 모든_URL이_죽어_있으면_접속불가() {
        SchemaRegistryClient dead = new SchemaRegistryClient(
                new SchemaRegistryProperties("http://127.0.0.1:1,http://127.0.0.1:2", 2000), RestClient.builder());
        assertThatThrownBy(dead::subjects).isInstanceOf(SchemaRegistryUnavailableException.class);
    }
}
