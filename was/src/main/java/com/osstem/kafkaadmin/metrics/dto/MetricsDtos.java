package com.osstem.kafkaadmin.metrics.dto;

import java.time.Instant;
import java.util.List;

public final class MetricsDtos {
    private MetricsDtos() {}

    public record BrokerSnapshot(int id, String host, boolean scraped,
                                 double bytesInPerSec, double bytesOutPerSec, double messagesInPerSec,
                                 double p99ProduceMs, double p99FetchMs, double handlerIdlePct, double networkIdlePct,
                                 double heapUsedPct, double cpuPct, int leaderCount, int partitionCount,
                                 int underReplicated) {}

    // configured=false 면 나머지는 0/빈 목록
    public record ClusterHealth(boolean configured, Instant asOf,
                                int activeControllers, int offlinePartitions, int underReplicated, int underMinIsr,
                                double uncleanElectionsLastHour, double uncleanElectionsTotal,
                                int activeBrokers, int fencedBrokers, List<BrokerSnapshot> brokers) {
        public static ClusterHealth notConfigured(Instant asOf) {
            return new ClusterHealth(false, asOf, 0, 0, 0, 0, 0, 0, 0, 0, List.of());
        }
    }

    public record SeriesPoint(Instant t, double v) {}
    public record Series(String name, List<SeriesPoint> points) {}
    public record SeriesResponse(String key, String unit, String range, long stepSeconds, List<Series> series) {}
    public record PrometheusStatus(boolean configured, String url, boolean healthy) {}
}
