package com.osstem.kafkaadmin.monitor;

import org.springframework.stereotype.Service;
import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DashboardService {
    public record RankedValue(String name, double value) {}
    public record Dashboard(Instant sampledAt, boolean stale, Double incomingPerSec,
                            Integer laggingGroups, Double underReplicated, Double maxDiskUsedPct,
                            List<RankedValue> lagTop, List<RankedValue> growingLagTop,
                            List<RankedValue> incomingTop) {}
    private final MetricSampleRepository samples;
    public DashboardService(MetricSampleRepository samples) { this.samples = samples; }

    public Dashboard snapshot() {
        Instant now = Instant.now();
        Map<String, List<MetricSample>> data = new HashMap<>();
        for (String type : List.of("URP", "LAG", "PRODUCED_TOPIC", "DISK_USED_PCT")) {
            data.put(type, samples.findByMetricTypeAndSubjectKeyStartingWithAndSampledAtAfterOrderBySampledAt(
                    type, "", now.minusSeconds(300)));
        }
        // URP는 빈 클러스터에서도 매 Kafka 수집 배치에 존재한다. 다른 배치의 오래된 대상을 섞지 않는다.
        Instant at = data.get("URP").stream().map(MetricSample::getSampledAt).max(Comparator.naturalOrder()).orElse(null);
        if (at == null) return new Dashboard(null, true, null, null, null, null, List.of(), List.of(), List.of());
        List<RankedValue> lag = current(data.get("LAG"), at);
        List<RankedValue> incoming = rates(data.get("PRODUCED_TOPIC"), at, true);
        List<RankedValue> growth = rates(data.get("LAG"), at, false);
        int topics = current(data.get("PRODUCED_TOPIC"), at).size();
        Double total = topics > 0 && incoming.size() == topics ? incoming.stream().mapToDouble(RankedValue::value).sum() : null;
        return new Dashboard(at, at.isBefore(now.minusSeconds(180)), total,
                (int) lag.stream().filter(v -> v.value() > 0).count(),
                current(data.get("URP"), at).stream().map(RankedValue::value).findFirst().orElse(null),
                current(data.get("DISK_USED_PCT"), at).stream().map(RankedValue::value).max(Double::compare).orElse(null),
                top(lag), top(growth), top(incoming));
    }

    private static List<RankedValue> current(List<MetricSample> points, Instant at) {
        return points.stream().filter(s -> s.getSampledAt().equals(at))
                .map(s -> new RankedValue(s.getSubjectKey(), s.getValue())).toList();
    }
    private static List<RankedValue> rates(List<MetricSample> points, Instant at, boolean counter) {
        List<RankedValue> out = new ArrayList<>();
        Map<String, List<MetricSample>> groups = points.stream().filter(s -> !s.getSampledAt().isAfter(at)).collect(Collectors.groupingBy(MetricSample::getSubjectKey));
        for (var entry : groups.entrySet()) {
            List<MetricSample> p = entry.getValue();
            MetricSample last = p.get(p.size() - 1);
            if (!last.getSampledAt().equals(at) || p.size() < 2) continue;
            MetricSample prev = p.get(p.size() - 2);
            double seconds = Duration.between(prev.getSampledAt(), at).toMillis() / 1000.0;
            double delta = last.getValue() - prev.getValue();
            // 오프셋 감소는 재생성 등으로 간주한다. 관측 불가를 0건으로 합산하지 않는다.
            if (seconds <= 0 || seconds > 180 || (counter && delta < 0)) continue;
            out.add(new RankedValue(entry.getKey(), delta / seconds));
        }
        return out;
    }
    private static List<RankedValue> top(List<RankedValue> values) {
        return values.stream().filter(v -> v.value() > 0)
                .sorted(Comparator.comparingDouble(RankedValue::value).reversed().thenComparing(RankedValue::name))
                .limit(5).toList();
    }
}
