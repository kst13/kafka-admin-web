package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Optional;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class SchemaQueryServiceTest {

    private final SchemaRegistryClient client = mock(SchemaRegistryClient.class);
    private final SchemaQueryService service = new SchemaQueryService(client);

    private static SchemaVersion v(String subject, int version, int id, String type) {
        return new SchemaVersion(subject, version, id, type, "{}", List.of());
    }

    @Test
    void 미설정이면_configured_false() {
        when(client.configured()).thenReturn(false);
        SchemaRegistryStatus s = service.status();
        assertThat(s.configured()).isFalse();
        assertThat(s.urls()).isEmpty();
        assertThat(s.globalCompatibility()).isNull();
        verify(client, never()).globalConfig();
    }

    @Test
    void 목록은_토픽_종류_최신버전_호환성_출처를_채운다() {
        when(client.configured()).thenReturn(true);
        when(client.globalConfig()).thenReturn(CompatibilityLevel.BACKWARD);
        when(client.subjects()).thenReturn(List.of("orders-value", "orders-key", "com.x.Y"));
        when(client.version("orders-value", "latest")).thenReturn(v("orders-value", 3, 12, "AVRO"));
        when(client.version("orders-key", "latest")).thenReturn(v("orders-key", 1, 4, "JSON"));
        when(client.version("com.x.Y", "latest")).thenReturn(v("com.x.Y", 2, 9, "PROTOBUF"));
        when(client.subjectConfig("orders-value")).thenReturn(Optional.of(CompatibilityLevel.FULL));
        when(client.subjectConfig("orders-key")).thenReturn(Optional.empty());
        when(client.subjectConfig("com.x.Y")).thenReturn(Optional.empty());

        List<SubjectSummary> list = service.listSubjects();
        assertThat(list).containsExactly(
                new SubjectSummary("orders-value", "orders", "value", 3, "AVRO", "FULL", "SUBJECT"),
                new SubjectSummary("orders-key", "orders", "key", 1, "JSON", "BACKWARD", "GLOBAL"),
                new SubjectSummary("com.x.Y", null, "other", 2, "PROTOBUF", "BACKWARD", "GLOBAL"));
        verify(client, times(1)).globalConfig();
    }

    @Test
    void 상세는_버전을_최신_우선으로_나열한다() {
        when(client.globalConfig()).thenReturn(CompatibilityLevel.BACKWARD);
        when(client.versions("orders-value")).thenReturn(List.of(1, 2, 3));
        when(client.version("orders-value", "1")).thenReturn(v("orders-value", 1, 4, "AVRO"));
        when(client.version("orders-value", "2")).thenReturn(v("orders-value", 2, 8, "AVRO"));
        when(client.version("orders-value", "3")).thenReturn(v("orders-value", 3, 12, "AVRO"));
        when(client.subjectConfig("orders-value")).thenReturn(Optional.empty());
        SubjectDetail d = service.describeSubject("orders-value");
        assertThat(d.topic()).isEqualTo("orders");
        assertThat(d.kind()).isEqualTo("value");
        assertThat(d.compatibility()).isEqualTo("BACKWARD");
        assertThat(d.compatibilitySource()).isEqualTo("GLOBAL");
        assertThat(d.versions()).extracting(SchemaVersionSummary::version).containsExactly(3, 2, 1);
        assertThat(d.versions().get(0).id()).isEqualTo(12);
    }

    @Test
    void 없는_서브젝트_상세는_클라이언트_예외를_그대로_올린다() {
        when(client.globalConfig()).thenReturn(CompatibilityLevel.BACKWARD);
        when(client.versions("ghost-value")).thenThrow(new SubjectNotFoundException("ghost-value"));
        assertThatThrownBy(() -> service.describeSubject("ghost-value")).isInstanceOf(SubjectNotFoundException.class);
    }

    @Test
    void 토픽별_조회는_없는_쪽을_null로_둔다() {
        when(client.globalConfig()).thenReturn(CompatibilityLevel.BACKWARD);
        when(client.version("orders-key", "latest")).thenThrow(new SubjectNotFoundException("orders-key"));
        when(client.version("orders-value", "latest")).thenReturn(v("orders-value", 2, 8, "AVRO"));
        when(client.subjectConfig("orders-value")).thenReturn(Optional.empty());
        TopicSchemas t = service.topicSchemas("orders");
        assertThat(t.topic()).isEqualTo("orders");
        assertThat(t.key()).isNull();
        assertThat(t.value().latestVersion()).isEqualTo(2);
        assertThatThrownBy(() -> service.topicSchemas("bad topic")).isInstanceOf(IllegalArgumentException.class);
    }
}
