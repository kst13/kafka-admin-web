package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.metrics.PrometheusClient.RangeSeries;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.Series;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.SeriesPoint;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.SeriesResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

// 시계열 1개 조회 (한 요청에 질의 1개, 캐시 없음)
@Service
public class MetricSeriesService {

    private final PrometheusClient client;
    private final Clock clock;

    @Autowired
    public MetricSeriesService(PrometheusClient client) { this(client, Clock.systemUTC()); }

    MetricSeriesService(PrometheusClient client, Clock clock) {
        this.client = client;
        this.clock = clock;
    }

    public SeriesResponse series(MetricKey key, Map<String, String> args, SeriesRange range) {
        Instant end = clock.instant();
        Instant start = end.minus(range.window());
        List<RangeSeries> raw = client.range(key.promql(args, range.rateWindow()), start, end, range.step());
        List<Series> series = raw.stream()
                .map(s -> new Series(
                        key.perPartition() ? s.labels().getOrDefault("partition", "?") : key.name(),
                        s.points().stream().map(p -> new SeriesPoint(p.t(), p.v())).toList()))
                .sorted(Comparator.comparing(Series::name, MetricSeriesService::compareNames))
                .toList();
        return new SeriesResponse(key.name(), key.unit(), range.label(), range.step().getSeconds(), series);
    }

    // 파티션 번호는 숫자순, 숫자가 아니면 문자열순
    private static int compareNames(String a, String b) {
        try {
            return Integer.compare(Integer.parseInt(a), Integer.parseInt(b));
        } catch (NumberFormatException e) {
            return a.compareTo(b);
        }
    }
}
