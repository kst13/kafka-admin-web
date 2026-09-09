package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.BrokerSnapshot;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.ClusterHealth;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.Series;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.SeriesResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Duration;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.awaitility.Awaitility.await;

class PrometheusIT extends PrometheusIntegrationTestBase {

    @Autowired PrometheusClient client;
    @Autowired ClusterHealthService healthService;
    @Autowired MetricSeriesService seriesService;

    // 첫 스크랩(5초 간격)과 rate 창(5m 안에 샘플 2개)을 기다린다
    @BeforeEach
    void waitForScrapes() {
        await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(client.instant("count(up{job=\"kafka-broker\"} == 1)")).isNotEmpty()
                        .first().satisfies(s -> assertThat(s.value()).isEqualTo(3.0)));
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(client.instant("min(count_over_time(up{job=\"kafka-broker\"}[5m]))")).isNotEmpty()
                        .first().satisfies(s -> assertThat(s.value()).isGreaterThanOrEqualTo(2.0)));
    }

    @Test
    void healthy_와_클러스터_건강_스냅샷을_실제_Prometheus에서_조립한다() {
        assertThat(client.healthy()).isTrue();

        ClusterHealth h = healthService.refresh();
        assertThat(h.configured()).isTrue();
        assertThat(h.activeControllers()).isEqualTo(1);
        assertThat(h.offlinePartitions()).isZero();
        assertThat(h.activeBrokers()).isEqualTo(3);
        assertThat(h.uncleanElectionsTotal()).isEqualTo(12.0); // 3대 합
        assertThat(h.brokers()).extracting(BrokerSnapshot::id).containsExactly(1, 2, 3);
        assertThat(h.brokers()).allMatch(BrokerSnapshot::scraped);
        assertThat(h.brokers()).extracting(BrokerSnapshot::p99ProduceMs).containsExactly(10.0, 20.0, 30.0);
        assertThat(h.brokers()).extracting(BrokerSnapshot::heapUsedPct).containsExactly(40.0, 50.0, 60.0);
        assertThat(h.brokers()).extracting(BrokerSnapshot::leaderCount).containsExactly(5, 6, 7);
        assertThat(h.brokers().get(0).handlerIdlePct()).isCloseTo(100.0, within(0.01)); // 2.0 → clamp → 100%
        assertThat(h.brokers().get(0).networkIdlePct()).isCloseTo(90.0, within(0.01));
        assertThat(h.brokers().get(0).bytesInPerSec()).isZero(); // 고정 카운터의 rate 는 0
    }

    @Test
    void 시계열_범위_4종과_파티션별_시리즈를_왕복한다() {
        Map<String, String> broker = Map.of("broker", "10.0.0.2:7071");
        for (SeriesRange r : SeriesRange.values()) {
            SeriesResponse s = seriesService.series(MetricKey.BROKER_P99_PRODUCE_MS, broker, r);
            assertThat(s.range()).isEqualTo(r.label());
            assertThat(s.stepSeconds()).isEqualTo(r.step().getSeconds());
            assertThat(s.series()).hasSize(1);
            assertThat(s.series().get(0).name()).isEqualTo("BROKER_P99_PRODUCE_MS");
            assertThat(s.series().get(0).points()).isNotEmpty().allMatch(p -> p.v() == 20.0);
        }
        SeriesResponse parts = seriesService.series(MetricKey.TOPIC_LOG_SIZE_BY_PARTITION, Map.of("topic", "orders"), SeriesRange.H1);
        assertThat(parts.unit()).isEqualTo("bytes");
        assertThat(parts.series()).extracting(Series::name).containsExactly("0", "1");
        assertThat(parts.series().get(1).points()).isNotEmpty().allMatch(p -> p.v() == 2048.0);

        SeriesResponse retained = seriesService.series(MetricKey.TOPIC_RETAINED_BY_PARTITION, Map.of("topic", "orders"), SeriesRange.H1);
        assertThat(retained.series()).extracting(Series::name).containsExactly("0", "1");
        assertThat(retained.series().get(0).points()).allMatch(p -> p.v() == 100.0);

        SeriesResponse none = seriesService.series(MetricKey.TOPIC_BYTES_IN, Map.of("topic", "ghost"), SeriesRange.H1);
        assertThat(none.series()).isEmpty();
    }
}
