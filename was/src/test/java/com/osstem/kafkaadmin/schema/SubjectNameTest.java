package com.osstem.kafkaadmin.schema;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class SubjectNameTest {

    @Test
    void 토픽_value_서브젝트를_파싱한다() {
        SubjectName n = SubjectName.parse("orders-value");
        assertThat(n.topic()).isEqualTo("orders");
        assertThat(n.kind()).isEqualTo(SubjectKind.VALUE);
        assertThat(n.isTopicBound()).isTrue();
        assertThat(SubjectName.parse("order.events_v2-key").topic()).isEqualTo("order.events_v2");
    }

    @Test
    void 규칙_밖_이름은_other이고_topic이_null() {
        SubjectName n = SubjectName.parse("com.example.Order");
        assertThat(n.kind()).isEqualTo(SubjectKind.OTHER);
        assertThat(n.topic()).isNull();
        assertThat(n.isTopicBound()).isFalse();
        assertThat(SubjectName.parse("orders-VALUE").kind()).isEqualTo(SubjectKind.OTHER);
        assertThat(SubjectName.parse("-value").kind()).isEqualTo(SubjectKind.OTHER);
    }

    @Test
    void 토픽과_종류로_서브젝트를_만든다() {
        assertThat(SubjectName.of("orders", SubjectKind.KEY)).isEqualTo("orders-key");
        assertThatThrownBy(() -> SubjectName.of("orders", SubjectKind.OTHER)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SubjectName.of("bad topic", SubjectKind.VALUE)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SubjectName.of("", SubjectKind.VALUE)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void enum_파싱은_대소문자를_무시하고_이상값을_거부한다() {
        assertThat(SubjectKind.parse("Key")).isEqualTo(SubjectKind.KEY);
        assertThatThrownBy(() -> SubjectKind.parse("other")).isInstanceOf(IllegalArgumentException.class);
        assertThat(SchemaType.parse("json")).isEqualTo(SchemaType.JSON);
        assertThatThrownBy(() -> SchemaType.parse("xml")).isInstanceOf(IllegalArgumentException.class);
        assertThat(CompatibilityLevel.parse("full_transitive")).isEqualTo(CompatibilityLevel.FULL_TRANSITIVE);
        assertThatThrownBy(() -> CompatibilityLevel.parse(null)).isInstanceOf(IllegalArgumentException.class);
        assertThat(SubjectKind.VALUE.value()).isEqualTo("value");
    }
}
