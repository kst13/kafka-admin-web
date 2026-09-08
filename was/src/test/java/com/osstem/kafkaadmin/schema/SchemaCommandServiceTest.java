package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.kafka.TopicQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicSummary;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SchemaCommandServiceTest {

    private final SchemaRegistryClient client = mock(SchemaRegistryClient.class);
    private final TopicQueryService topics = mock(TopicQueryService.class);
    private final SchemaCommandService service = new SchemaCommandService(client, topics);

    @BeforeEach
    void topicsExist() {
        when(topics.listTopics()).thenReturn(List.of(new TopicSummary("orders", 3, 3)));
    }

    @Test
    void 등록은_호환성_검사_후_등록하고_최신_버전을_돌려준다() {
        when(client.testCompatibility("orders-value", SchemaType.AVRO, "{}")).thenReturn(new CompatibilityResult(true, List.of()));
        when(client.register("orders-value", SchemaType.AVRO, "{}")).thenReturn(12);
        when(client.version("orders-value", "latest")).thenReturn(new SchemaVersion("orders-value", 3, 12, "AVRO", "{}", List.of()));
        RegisteredSchema r = service.register("orders", SubjectKind.VALUE, SchemaType.AVRO, "{}");
        assertThat(r).isEqualTo(new RegisteredSchema("orders-value", 12, 3));
        var order = inOrder(client);
        order.verify(client).testCompatibility("orders-value", SchemaType.AVRO, "{}");
        order.verify(client).register("orders-value", SchemaType.AVRO, "{}");
    }

    @Test
    void 호환성_실패면_등록하지_않고_409용_예외() {
        when(client.testCompatibility(any(), any(), any())).thenReturn(new CompatibilityResult(false, List.of("READER_FIELD_MISSING_DEFAULT_VALUE")));
        assertThatThrownBy(() -> service.register("orders", SubjectKind.VALUE, SchemaType.AVRO, "{}"))
                .isInstanceOf(IncompatibleSchemaException.class)
                .satisfies(e -> assertThat(((IncompatibleSchemaException) e).getMessages()).containsExactly("READER_FIELD_MISSING_DEFAULT_VALUE"));
        verify(client, never()).register(any(), any(), any());
    }

    @Test
    void 없는_토픽이면_Registry를_호출하지_않고_UnknownTopic() {
        assertThatThrownBy(() -> service.register("ghost", SubjectKind.VALUE, SchemaType.AVRO, "{}"))
                .isInstanceOf(UnknownTopicOrPartitionException.class);
        verifyNoInteractions(client);
    }

    @Test
    void 본문_검증_빈값과_1MB_초과는_400용_예외() {
        assertThatThrownBy(() -> service.register("orders", SubjectKind.VALUE, SchemaType.AVRO, " "))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("본문");
        String big = "x".repeat(SchemaCommandService.MAX_SCHEMA_BYTES + 1);
        assertThatThrownBy(() -> service.checkCompatibility("orders", SubjectKind.VALUE, SchemaType.JSON, big))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("1 MB");
        verifyNoInteractions(client);
    }

    @Test
    void 호환성_검사는_토픽_존재를_요구하지_않는다() {
        when(client.testCompatibility("new-value", SchemaType.JSON, "{}")).thenReturn(new CompatibilityResult(true, List.of()));
        assertThat(service.checkCompatibility("new", SubjectKind.VALUE, SchemaType.JSON, "{}").compatible()).isTrue();
        verify(topics, never()).listTopics();
    }

    @Test
    void 서브젝트_호환성은_존재_확인_후_설정하거나_해제한다() {
        when(client.versions("orders-value")).thenReturn(List.of(1));
        service.setCompatibility("orders-value", CompatibilityLevel.NONE);
        verify(client).setSubjectConfig("orders-value", CompatibilityLevel.NONE);
        service.setCompatibility("orders-value", null);
        verify(client).deleteSubjectConfig("orders-value");

        when(client.versions("ghost-value")).thenThrow(new SubjectNotFoundException("ghost-value"));
        assertThatThrownBy(() -> service.setCompatibility("ghost-value", CompatibilityLevel.FULL))
                .isInstanceOf(SubjectNotFoundException.class);
        verify(client, never()).setSubjectConfig(eq("ghost-value"), any());
    }

    @Test
    void 전역_호환성_변경과_삭제는_클라이언트에_위임한다() {
        service.setGlobalCompatibility(CompatibilityLevel.FULL_TRANSITIVE);
        verify(client).setGlobalConfig(CompatibilityLevel.FULL_TRANSITIVE);
        when(client.deleteSubject("orders-value")).thenReturn(List.of(1, 2));
        assertThat(service.deleteSubject("orders-value")).containsExactly(1, 2);
    }
}
