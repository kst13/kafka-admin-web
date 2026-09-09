package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import com.osstem.kafkaadmin.metrics.PrometheusClient.InstantSample;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.BrokerSnapshot;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.ClusterHealth;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class ClusterHealthServiceTest {

    private final PrometheusClient client = mock(PrometheusClient.class);
    private final ClusterQueryService cluster = mock(ClusterQueryService.class);
    private final PrometheusProperties props = new PrometheusProperties("http://p", null, 7071, 15);
    private final BrokerInstanceResolver resolver = new BrokerInstanceResolver(cluster, props);
    private final MutableClock clock = new MutableClock(Instant.parse("2026-09-09T00:00:00Z"));
    private ClusterHealthService service;

    static class MutableClock extends Clock {
        Instant now;
        MutableClock(Instant now) { this.now = now; }
        @Override public java.time.ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(java.time.ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }

    private static InstantSample single(double v) { return new InstantSample(Map.of(), v); }
    private static InstantSample at(String instance, double v) { return new InstantSample(Map.of("instance", instance), v); }

    @BeforeEach
    void setUp() {
        when(client.configured()).thenReturn(true);
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("c", 1, List.of(
                new BrokerInfo(1, "10.0.0.1", 9094), new BrokerInfo(2, "10.0.0.2", 9094))));
        // 기본: 모든 질의는 빈 결과 → 0
        when(client.instant(anyString())).thenReturn(List.of());
        when(client.instant(MetricKey.ACTIVE_CONTROLLERS.promqlCurrent(Map.of()))).thenReturn(List.of(single(1)));
        when(client.instant(MetricKey.UNDER_REPLICATED.promqlCurrent(Map.of()))).thenReturn(List.of(single(2)));
        when(client.instant(MetricKey.UNCLEAN_ELECTIONS_TOTAL.promqlCurrent(Map.of()))).thenReturn(List.of(single(7)));
        when(client.instant(MetricKey.ACTIVE_BROKERS.promqlCurrent(Map.of()))).thenReturn(List.of(single(2)));
        when(client.instant(MetricKey.BROKER_BYTES_IN.promqlByInstance("5m")))
                .thenReturn(List.of(at("10.0.0.1:7071", 1024.0), at("10.0.0.2:7071", 2048.0), at("unknown:7071", 9)));
        when(client.instant(MetricKey.BROKER_P99_PRODUCE_MS.promqlByInstance("5m")))
                .thenReturn(List.of(at("10.0.0.1:7071", 12.5)));
        when(client.instant(MetricKey.BROKER_HANDLER_IDLE_PCT.promqlByInstance("5m")))
                .thenReturn(List.of(at("10.0.0.1:7071", 100.0)));
        when(client.instant(MetricKey.BROKER_LEADER_COUNT.promqlByInstance("5m")))
                .thenReturn(List.of(at("10.0.0.1:7071", 10), at("10.0.0.2:7071", 11)));
        service = new ClusterHealthService(client, resolver, props, clock);
    }

    @Test
    void 클러스터_값과_브로커별_값을_한_번의_질의_묶음으로_조립한다() {
        ClusterHealth h = service.health();

        assertThat(h.configured()).isTrue();
        assertThat(h.asOf()).isEqualTo(clock.now);
        assertThat(h.activeControllers()).isEqualTo(1);
        assertThat(h.underReplicated()).isEqualTo(2);
        assertThat(h.offlinePartitions()).isZero();
        assertThat(h.uncleanElectionsTotal()).isEqualTo(7.0);
        assertThat(h.activeBrokers()).isEqualTo(2);
        assertThat(h.brokers()).extracting(BrokerSnapshot::id).containsExactly(1, 2);
        BrokerSnapshot b1 = h.brokers().get(0);
        assertThat(b1.host()).isEqualTo("10.0.0.1");
        assertThat(b1.scraped()).isTrue();
        assertThat(b1.bytesInPerSec()).isEqualTo(1024.0);
        assertThat(b1.p99ProduceMs()).isEqualTo(12.5);
        assertThat(b1.handlerIdlePct()).isEqualTo(100.0);
        assertThat(b1.leaderCount()).isEqualTo(10);
        BrokerSnapshot b2 = h.brokers().get(1);
        assertThat(b2.bytesInPerSec()).isEqualTo(2048.0);
        assertThat(b2.p99ProduceMs()).isZero(); // 시리즈 없음 → 0
        // 클러스터 8개 + 브로커 12개 = 20개 질의, 브로커 수와 무관
        verify(client, times(20)).instant(anyString());
    }

    @Test
    void Prometheus에_시리즈가_전혀_없는_브로커는_scraped_false() {
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("c", 1, List.of(
                new BrokerInfo(1, "10.0.0.1", 9094), new BrokerInfo(3, "10.0.0.3", 9094))));
        ClusterHealth h = service.health();
        assertThat(h.brokers()).extracting(BrokerSnapshot::scraped).containsExactly(true, false);
    }

    @Test
    void TTL_안에서는_캐시를_돌려주고_지나면_다시_조회하며_refresh는_항상_조회한다() {
        service.health();
        service.health();
        verify(client, times(20)).instant(anyString());

        clock.now = clock.now.plusSeconds(16);
        service.health();
        verify(client, times(40)).instant(anyString());

        service.refresh();
        verify(client, times(60)).instant(anyString());
        service.health(); // refresh 결과가 캐시에 남는다
        verify(client, times(60)).instant(anyString());
    }

    @Test
    void 미설정이면_질의_없이_configured_false() {
        when(client.configured()).thenReturn(false);
        ClusterHealth h = service.health();
        assertThat(h.configured()).isFalse();
        assertThat(h.brokers()).isEmpty();
        assertThat(service.configured()).isFalse();
        verify(client, never()).instant(anyString());
    }
}
