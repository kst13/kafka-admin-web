package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.metrics.PrometheusClient.InstantSample;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.BrokerSnapshot;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.ClusterHealth;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 클러스터 8개 + 브로커 12개(instance 그룹) 질의로 스냅샷을 조립한다. 화면 요청마다 20개 질의가 나가므로
// cache-seconds 동안 단일 항목 캐시 (동시 요청은 synchronized 로 하나만 조회).
@Service
public class ClusterHealthService {

    private record Cached(ClusterHealth value, Instant expiresAt) {}

    private static final List<MetricKey> BROKER_KEYS = List.of(
            MetricKey.BROKER_BYTES_IN, MetricKey.BROKER_BYTES_OUT, MetricKey.BROKER_MESSAGES_IN,
            MetricKey.BROKER_P99_PRODUCE_MS, MetricKey.BROKER_P99_FETCH_MS,
            MetricKey.BROKER_HANDLER_IDLE_PCT, MetricKey.BROKER_NETWORK_IDLE_PCT,
            MetricKey.BROKER_HEAP_USED_PCT, MetricKey.BROKER_CPU_PCT,
            MetricKey.BROKER_LEADER_COUNT, MetricKey.BROKER_PARTITION_COUNT, MetricKey.BROKER_URP);

    private final PrometheusClient client;
    private final BrokerInstanceResolver resolver;
    private final PrometheusProperties props;
    private final Clock clock;
    private volatile Cached cache;

    @Autowired
    public ClusterHealthService(PrometheusClient client, BrokerInstanceResolver resolver, PrometheusProperties props) {
        this(client, resolver, props, Clock.systemUTC());
    }

    ClusterHealthService(PrometheusClient client, BrokerInstanceResolver resolver, PrometheusProperties props, Clock clock) {
        this.client = client;
        this.resolver = resolver;
        this.props = props;
        this.clock = clock;
    }

    public boolean configured() { return client.configured(); }

    public ClusterHealth health() {
        Cached c = cache;
        if (c != null && c.expiresAt().isAfter(clock.instant())) return c.value();
        synchronized (this) {
            c = cache;
            if (c != null && c.expiresAt().isAfter(clock.instant())) return c.value();
            return refresh();
        }
    }

    // 캐시를 무시하고 조회한 뒤 캐시를 갱신한다 (수집기용)
    public ClusterHealth refresh() {
        Instant now = clock.instant();
        ClusterHealth h = client.configured() ? load(now) : ClusterHealth.notConfigured(now);
        cache = new Cached(h, now.plus(props.cacheTtl()));
        return h;
    }

    private ClusterHealth load(Instant now) {
        List<BrokerInfo> brokers = resolver.brokers();
        Map<MetricKey, Map<String, Double>> byKey = new HashMap<>();
        for (MetricKey k : BROKER_KEYS) {
            Map<String, Double> m = new HashMap<>();
            for (InstantSample s : client.instant(k.promqlByInstance(MetricKey.CURRENT_RATE_WINDOW))) {
                String inst = s.labels().get("instance");
                if (inst != null) m.put(inst, s.value());
            }
            byKey.put(k, m);
        }
        List<BrokerSnapshot> snapshots = new ArrayList<>();
        for (BrokerInfo b : brokers) {
            String inst = resolver.instance(b);
            boolean scraped = byKey.values().stream().anyMatch(m -> m.containsKey(inst));
            snapshots.add(new BrokerSnapshot(b.id(), b.host(), scraped,
                    v(byKey, MetricKey.BROKER_BYTES_IN, inst), v(byKey, MetricKey.BROKER_BYTES_OUT, inst),
                    v(byKey, MetricKey.BROKER_MESSAGES_IN, inst),
                    v(byKey, MetricKey.BROKER_P99_PRODUCE_MS, inst), v(byKey, MetricKey.BROKER_P99_FETCH_MS, inst),
                    v(byKey, MetricKey.BROKER_HANDLER_IDLE_PCT, inst), v(byKey, MetricKey.BROKER_NETWORK_IDLE_PCT, inst),
                    v(byKey, MetricKey.BROKER_HEAP_USED_PCT, inst), v(byKey, MetricKey.BROKER_CPU_PCT, inst),
                    i(byKey, MetricKey.BROKER_LEADER_COUNT, inst), i(byKey, MetricKey.BROKER_PARTITION_COUNT, inst),
                    i(byKey, MetricKey.BROKER_URP, inst)));
        }
        return new ClusterHealth(true, now,
                (int) Math.round(scalar(MetricKey.ACTIVE_CONTROLLERS)),
                (int) Math.round(scalar(MetricKey.OFFLINE_PARTITIONS)),
                (int) Math.round(scalar(MetricKey.UNDER_REPLICATED)),
                (int) Math.round(scalar(MetricKey.UNDER_MIN_ISR)),
                scalar(MetricKey.UNCLEAN_ELECTIONS_1H),
                scalar(MetricKey.UNCLEAN_ELECTIONS_TOTAL),
                (int) Math.round(scalar(MetricKey.ACTIVE_BROKERS)),
                (int) Math.round(scalar(MetricKey.FENCED_BROKERS)),
                snapshots);
    }

    // 클러스터 집계 질의: 결과가 비면 0
    private double scalar(MetricKey key) {
        List<InstantSample> r = client.instant(key.promqlCurrent(Map.of()));
        return r.isEmpty() ? 0.0 : r.get(0).value();
    }

    private static double v(Map<MetricKey, Map<String, Double>> byKey, MetricKey k, String inst) {
        return byKey.getOrDefault(k, Map.of()).getOrDefault(inst, 0.0);
    }

    private static int i(Map<MetricKey, Map<String, Double>> byKey, MetricKey k, String inst) {
        return (int) Math.round(v(byKey, k, inst));
    }
}
