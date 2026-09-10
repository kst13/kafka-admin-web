package com.osstem.kafkaadmin.monitor;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class DashboardServiceTest {
    final MetricSampleRepository repository = mock(MetricSampleRepository.class);
    final DashboardService service = new DashboardService(repository);
    final Instant now = Instant.now();
    void points(String type, MetricSample... values) {
        when(repository.findByMetricTypeAndSubjectKeyStartingWithAndSampledAtAfterOrderBySampledAt(eq(type), eq(""), any()))
                .thenReturn(List.of(values));
    }
    MetricSample point(String type, String key, double value, int secondsAgo) {
        return new MetricSample(type, key, value, now.minusSeconds(secondsAgo));
    }
    @Test void noSamplesAreUnknownRatherThanHealthyZero() {
        var result = service.snapshot();
        assertThat(result.stale()).isTrue();
        assertThat(result.incomingPerSec()).isNull();
        assertThat(result.laggingGroups()).isNull();
        assertThat(result.underReplicated()).isNull();
    }
    @Test void ranksCurrentBatchAndExcludesDisappearedGroups() {
        points("URP", point("URP", "cluster", 0, 0));
        points("LAG", point("LAG", "gone", 900, 60), point("LAG", "orders", 30, 60),
                point("LAG", "orders", 150, 0), point("LAG", "idle", 0, 0));
        points("PRODUCED_TOPIC", point("PRODUCED_TOPIC", "orders", 100, 60), point("PRODUCED_TOPIC", "orders", 220, 0));
        var result = service.snapshot();
        assertThat(result.laggingGroups()).isEqualTo(1);
        assertThat(result.lagTop()).containsExactly(new DashboardService.RankedValue("orders", 150));
        assertThat(result.growingLagTop()).containsExactly(new DashboardService.RankedValue("orders", 2));
        assertThat(result.incomingPerSec()).isEqualTo(2);
        assertThat(result.maxDiskUsedPct()).isNull();
    }
    @Test void resetOrMissingBaselineMakesTotalUnknown() {
        points("URP", point("URP", "cluster", 0, 0));
        points("PRODUCED_TOPIC", point("PRODUCED_TOPIC", "reset", 100, 60), point("PRODUCED_TOPIC", "reset", 20, 0),
                point("PRODUCED_TOPIC", "new", 80, 0));
        assertThat(service.snapshot().incomingPerSec()).isNull();
        assertThat(service.snapshot().incomingTop()).isEmpty();
    }
    @Test void oldBatchIsMarkedStale() {
        points("URP", point("URP", "cluster", 1, 240));
        assertThat(service.snapshot().stale()).isTrue();
    }
    @Test void rankingIsDescendingAndLimitedToFive() {
        points("URP", point("URP", "cluster", 0, 0));
        points("LAG", java.util.stream.IntStream.rangeClosed(1, 8)
                .mapToObj(i -> point("LAG", "group-" + i, i, 0)).toArray(MetricSample[]::new));
        var result = service.snapshot();
        assertThat(result.laggingGroups()).isEqualTo(8);
        assertThat(result.lagTop()).extracting(DashboardService.RankedValue::value)
                .containsExactly(8.0, 7.0, 6.0, 5.0, 4.0);
    }
    @Test void samplesArrivingDuringReadDoNotMixBatches() {
        points("URP", point("URP", "cluster", 0, 60));
        points("PRODUCED_TOPIC", point("PRODUCED_TOPIC", "orders", 100, 120),
                point("PRODUCED_TOPIC", "orders", 220, 60), point("PRODUCED_TOPIC", "orders", 1000, 0));
        assertThat(service.snapshot().incomingPerSec()).isEqualTo(2);
    }
}
