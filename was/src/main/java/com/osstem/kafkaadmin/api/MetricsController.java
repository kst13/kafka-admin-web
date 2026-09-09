package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.metrics.BrokerInstanceResolver;
import com.osstem.kafkaadmin.metrics.ClusterHealthService;
import com.osstem.kafkaadmin.metrics.MetricKey;
import com.osstem.kafkaadmin.metrics.MetricSeriesService;
import com.osstem.kafkaadmin.metrics.PrometheusClient;
import com.osstem.kafkaadmin.metrics.SeriesRange;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.ClusterHealth;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.PrometheusStatus;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.SeriesResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

// Prometheus 기반 조회 API. 모두 인증만 요구(조회 전용).
@RestController
@RequestMapping("/api")
public class MetricsController {

    private final PrometheusClient client;
    private final ClusterHealthService health;
    private final MetricSeriesService series;
    private final BrokerInstanceResolver resolver;

    public MetricsController(PrometheusClient client, ClusterHealthService health,
                             MetricSeriesService series, BrokerInstanceResolver resolver) {
        this.client = client;
        this.health = health;
        this.series = series;
        this.resolver = resolver;
    }

    @GetMapping("/prometheus/status")
    public PrometheusStatus status() {
        return new PrometheusStatus(client.configured(), client.url(), client.healthy());
    }

    @GetMapping("/cluster/health")
    public ClusterHealth clusterHealth() { return health.health(); }

    @GetMapping("/brokers/{id}/series")
    public SeriesResponse brokerSeries(@PathVariable int id, @RequestParam String key, @RequestParam String range) {
        MetricKey k = MetricKey.parse(key);
        if (k.scope() != MetricKey.Scope.BROKER) throw new IllegalArgumentException("브로커 지표 키가 아닙니다: " + key);
        SeriesRange r = SeriesRange.parse(range);
        return series.series(k, Map.of("broker", resolver.instanceOf(id)), r);
    }

    @GetMapping("/topics/{name}/series")
    public SeriesResponse topicSeries(@PathVariable String name, @RequestParam String key, @RequestParam String range) {
        MetricKey k = MetricKey.parse(key);
        if (k.scope() != MetricKey.Scope.TOPIC) throw new IllegalArgumentException("토픽 지표 키가 아닙니다: " + key);
        SeriesRange r = SeriesRange.parse(range);
        return series.series(k, Map.of("topic", name), r);
    }
}
