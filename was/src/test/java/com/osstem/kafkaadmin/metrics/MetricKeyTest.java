package com.osstem.kafkaadmin.metrics;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;

class MetricKeyTest {

    @Test
    void 클러스터_키는_인자_없이_생성된다() {
        assertThat(MetricKey.ACTIVE_CONTROLLERS.promqlCurrent(Map.of()))
                .isEqualTo("sum(kafka_controller_kafkacontroller_activecontrollercount)");
        assertThat(MetricKey.ACTIVE_BROKERS.promqlCurrent(Map.of()))
                .isEqualTo("max(kafka_controller_kafkacontroller_activebrokercount)");
        assertThat(MetricKey.UNCLEAN_ELECTIONS_1H.scope()).isEqualTo(MetricKey.Scope.CLUSTER);
    }

    @Test
    void 브로커_키는_instance_라벨과_rate_창을_치환한다() {
        String q = MetricKey.BROKER_BYTES_IN.promql(Map.of("broker", "10.0.0.1:7071"), SeriesRange.H6.rateWindow());
        assertThat(q).isEqualTo(
                "sum(rate(kafka_server_brokertopicmetrics_bytesin_total{instance=\"10.0.0.1:7071\",topic=\"\"}[5m]))");
        assertThat(MetricKey.BROKER_HANDLER_IDLE_PCT.promqlCurrent(Map.of("broker", "b:7071"))).isEqualTo(
                "clamp_max(max(kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent{instance=\"b:7071\"}), 1) * 100");
        assertThat(MetricKey.BROKER_CPU_PCT.promqlCurrent(Map.of("broker", "b:7071")))
                .isEqualTo("max(rate(process_cpu_seconds_total{instance=\"b:7071\"}[5m])) * 100");
        assertThat(MetricKey.BROKER_BYTES_IN.unit()).isEqualTo("bytes/s");
    }

    @Test
    void 브로커_키의_전체_질의는_instance로_그룹한다() {
        assertThat(MetricKey.BROKER_BYTES_IN.promqlByInstance("5m")).isEqualTo(
                "sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesin_total{topic=\"\"}[5m]))");
        assertThat(MetricKey.BROKER_HEAP_USED_PCT.promqlByInstance("5m")).isEqualTo(
                "max by (instance) (jvm_memory_used_bytes{area=\"heap\"}) / max by (instance) (jvm_memory_max_bytes{area=\"heap\"}) * 100");
        assertThatThrownBy(() -> MetricKey.TOPIC_BYTES_IN.promqlByInstance("5m"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 토픽_키는_topic_라벨을_치환하고_파티션별_여부를_안다() {
        assertThat(MetricKey.TOPIC_MESSAGES_IN.promql(Map.of("topic", "orders"), "2m"))
                .isEqualTo("sum(rate(kafka_server_brokertopicmetrics_messagesin_total{topic=\"orders\"}[2m]))");
        assertThat(MetricKey.TOPIC_LOG_SIZE_BY_PARTITION.promqlCurrent(Map.of("topic", "orders")))
                .isEqualTo("max by (partition) (kafka_log_log_size{topic=\"orders\"})");
        assertThat(MetricKey.TOPIC_LOG_SIZE_BY_PARTITION.perPartition()).isTrue();
        assertThat(MetricKey.TOPIC_RETAINED_BY_PARTITION.perPartition()).isTrue();
        assertThat(MetricKey.TOPIC_MESSAGES_IN.perPartition()).isFalse();
        assertThat(MetricKey.TOPIC_LOG_SIZE_TOTAL.unit()).isEqualTo("bytes");
    }

    @Test
    void 라벨_값의_따옴표_역슬래시_개행을_이스케이프한다() {
        assertThat(MetricKey.escapeLabel("a\"b\\c\nd")).isEqualTo("a\\\"b\\\\c\\nd");
        assertThat(MetricKey.escapeLabel("a\rb")).isEqualTo("a\\rb");
        assertThat(MetricKey.TOPIC_BYTES_IN.promqlCurrent(Map.of("topic", "x\"y")))
                .contains("{topic=\"x\\\"y\"}");
    }

    @Test
    void 필요한_인자가_없거나_키_이름이_틀리면_400용_예외() {
        assertThatThrownBy(() -> MetricKey.BROKER_BYTES_IN.promqlCurrent(Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("broker");
        assertThatThrownBy(() -> MetricKey.TOPIC_BYTES_IN.promqlCurrent(Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("topic");
        assertThatThrownBy(() -> MetricKey.parse("NOPE"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("지원하지 않는 key 입니다: NOPE");
        assertThat(MetricKey.parse("BROKER_URP")).isEqualTo(MetricKey.BROKER_URP);
    }

    @Test
    void 범위별_창_step_rate창() {
        assertThat(SeriesRange.parse("1h")).isEqualTo(SeriesRange.H1);
        assertThat(SeriesRange.H1.window().toMinutes()).isEqualTo(60);
        assertThat(SeriesRange.H1.step().getSeconds()).isEqualTo(30);
        assertThat(SeriesRange.H1.rateWindow()).isEqualTo("2m");
        assertThat(SeriesRange.H6.step().toMinutes()).isEqualTo(1);
        assertThat(SeriesRange.H6.rateWindow()).isEqualTo("5m");
        assertThat(SeriesRange.H24.window().toHours()).isEqualTo(24);
        assertThat(SeriesRange.H24.step().toMinutes()).isEqualTo(5);
        assertThat(SeriesRange.H24.rateWindow()).isEqualTo("10m");
        assertThat(SeriesRange.D7.window().toDays()).isEqualTo(7);
        assertThat(SeriesRange.D7.step().toMinutes()).isEqualTo(30);
        assertThat(SeriesRange.D7.rateWindow()).isEqualTo("1h");
        assertThat(SeriesRange.D7.label()).isEqualTo("7d");
        assertThatThrownBy(() -> SeriesRange.parse("2d"))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("지원하지 않는 range 입니다: 2d");
    }
}
