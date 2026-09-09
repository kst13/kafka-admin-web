package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.metrics.PrometheusClient.Point;
import com.osstem.kafkaadmin.metrics.PrometheusClient.RangeSeries;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.Series;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.SeriesResponse;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MetricSeriesServiceTest {

    private final PrometheusClient client = mock(PrometheusClient.class);
    private final Instant now = Instant.parse("2026-09-09T12:00:00Z");
    private final MetricSeriesService service =
            new MetricSeriesService(client, Clock.fixed(now, ZoneOffset.UTC));

    @Test
    void 범위에_맞는_창_step_rate창으로_질의하고_단일_시리즈는_키_이름을_쓴다() {
        when(client.range(anyString(), any(), any(), any())).thenReturn(List.of(
                new RangeSeries(Map.of(), List.of(new Point(now.minusSeconds(60), 5.0), new Point(now, 6.0)))));

        SeriesResponse r = service.series(MetricKey.BROKER_BYTES_IN, Map.of("broker", "h:7071"), SeriesRange.H24);

        ArgumentCaptor<String> q = ArgumentCaptor.forClass(String.class);
        verify(client).range(q.capture(), eq(now.minus(Duration.ofHours(24))), eq(now), eq(Duration.ofMinutes(5)));
        assertThat(q.getValue()).contains("[10m]").contains("instance=\"h:7071\"");
        assertThat(r.key()).isEqualTo("BROKER_BYTES_IN");
        assertThat(r.unit()).isEqualTo("bytes/s");
        assertThat(r.range()).isEqualTo("24h");
        assertThat(r.stepSeconds()).isEqualTo(300);
        assertThat(r.series()).hasSize(1);
        assertThat(r.series().get(0).name()).isEqualTo("BROKER_BYTES_IN");
        assertThat(r.series().get(0).points()).extracting(p -> p.v()).containsExactly(5.0, 6.0);
    }

    @Test
    void 파티션별_키는_partition_라벨을_이름으로_쓰고_숫자순으로_정렬한다() {
        when(client.range(anyString(), any(), any(), any())).thenReturn(List.of(
                new RangeSeries(Map.of("partition", "10"), List.of(new Point(now, 1.0))),
                new RangeSeries(Map.of("partition", "2"), List.of(new Point(now, 2.0))),
                new RangeSeries(Map.of("partition", "0"), List.of())));

        SeriesResponse r = service.series(MetricKey.TOPIC_LOG_SIZE_BY_PARTITION, Map.of("topic", "orders"), SeriesRange.H1);

        assertThat(r.series()).extracting(Series::name).containsExactly("0", "2", "10");
        assertThat(r.stepSeconds()).isEqualTo(30);
        verify(client).range(anyString(), eq(now.minus(Duration.ofHours(1))), eq(now), eq(Duration.ofSeconds(30)));
    }

    @Test
    void 결과가_없으면_빈_시리즈_목록() {
        when(client.range(anyString(), any(), any(), any())).thenReturn(List.of());
        SeriesResponse r = service.series(MetricKey.TOPIC_BYTES_IN, Map.of("topic", "t"), SeriesRange.D7);
        assertThat(r.series()).isEmpty();
        assertThat(r.stepSeconds()).isEqualTo(1800);
    }
}
