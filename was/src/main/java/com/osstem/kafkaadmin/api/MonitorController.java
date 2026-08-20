package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.MonitorQueryService;
import com.osstem.kafkaadmin.monitor.AlertEvent;
import com.osstem.kafkaadmin.monitor.AlertEventRepository;
import com.osstem.kafkaadmin.monitor.CertExpiryChecker;
import com.osstem.kafkaadmin.monitor.CertExpiryChecker.CertStatus;
import com.osstem.kafkaadmin.monitor.MetricSample;
import com.osstem.kafkaadmin.monitor.MetricSampleRepository;
import com.osstem.kafkaadmin.monitor.MetricsCollector;
import com.osstem.kafkaadmin.monitor.MonitorProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
public class MonitorController {

    public record SamplePoint(Instant sampledAt, double value) {}
    public record PartitionThroughput(int partition, long endOffset, long count,
                                      double ratePerMin) {}
    public record MonitorStatus(Instant lastCollectedAt, int consecutiveFailures,
                                List<CertStatus> certs) {}
    public record BrokerDisk(int brokerId, double usedPercent) {}
    public record DiskStatus(int thresholdPct, List<BrokerDisk> brokers) {}

    private final MetricSampleRepository samples;
    private final AlertEventRepository alerts;
    private final MetricsCollector collector;
    private final CertExpiryChecker certChecker;
    private final MonitorQueryService monitorQuery;
    private final MonitorProperties monitorProps;

    public MonitorController(MetricSampleRepository samples, AlertEventRepository alerts,
                             MetricsCollector collector, CertExpiryChecker certChecker,
                             MonitorQueryService monitorQuery, MonitorProperties monitorProps) {
        this.samples = samples;
        this.alerts = alerts;
        this.collector = collector;
        this.certChecker = certChecker;
        this.monitorQuery = monitorQuery;
        this.monitorProps = monitorProps;
    }

    @GetMapping("/metrics")
    public List<SamplePoint> metrics(@RequestParam String type,
                                     @RequestParam String subject,
                                     @RequestParam(defaultValue = "24") int hours) {
        return samples.findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
                        type, subject, Instant.now().minus(hours, ChronoUnit.HOURS)).stream()
                .map(s -> new SamplePoint(s.getSampledAt(), s.getValue()))
                .toList();
    }

    // 파티션별 유입량: 최근 1시간의 PRODUCED_PARTITION 누적 샘플에서 증가분을 계산한다.
    // 저장된 샘플만 쓰므로 브로커 무응답 중에도 마지막 수집값을 보여줄 수 있다.
    @GetMapping("/topics/{name}/throughput")
    public List<PartitionThroughput> topicThroughput(@PathVariable String name) {
        List<MetricSample> found =
                samples.findByMetricTypeAndSubjectKeyStartingWithAndSampledAtAfterOrderBySampledAt(
                        "PRODUCED_PARTITION", name + "|",
                        Instant.now().minus(1, ChronoUnit.HOURS));
        Map<Integer, List<MetricSample>> byPartition = found.stream()
                .collect(Collectors.groupingBy(s -> Integer.parseInt(
                        s.getSubjectKey().substring(s.getSubjectKey().lastIndexOf('|') + 1))));
        return byPartition.entrySet().stream()
                .map(e -> {
                    List<MetricSample> points = e.getValue(); // sampledAt 오름차순
                    MetricSample first = points.get(0);
                    MetricSample last = points.get(points.size() - 1);
                    // 토픽 재생성 등으로 오프셋이 줄어든 경우는 유입 0으로 취급 (consumption.ts와 동일)
                    long count = Math.max(0, (long) (last.getValue() - first.getValue()));
                    long elapsedMs = last.getSampledAt().toEpochMilli()
                            - first.getSampledAt().toEpochMilli();
                    double ratePerMin = elapsedMs > 0 ? count * 60_000.0 / elapsedMs : 0.0;
                    return new PartitionThroughput(e.getKey(), (long) last.getValue(),
                            count, ratePerMin);
                })
                .sorted(Comparator.comparingInt(PartitionThroughput::partition))
                .toList();
    }

    @GetMapping("/alerts")
    public List<AlertEvent> alerts() {
        return alerts.findTop50ByOrderByOccurredAtDesc();
    }

    @GetMapping("/monitor/status")
    public MonitorStatus status() {
        return new MonitorStatus(collector.lastSuccessAt(),
                collector.consecutiveFailures(), certChecker.lastStatuses());
    }

    // 실시간 조회 — 브로커 무응답이면 503(KafkaUnavailableException). 화면은 이 카드만 생략한다.
    @GetMapping("/monitor/disk")
    public DiskStatus disk() {
        List<BrokerDisk> brokers = monitorQuery.diskUsedPercentByBroker().entrySet().stream()
                .map(e -> new BrokerDisk(e.getKey(), e.getValue()))
                .sorted(Comparator.comparingInt(BrokerDisk::brokerId))
                .toList();
        return new DiskStatus(monitorProps.diskUsedPctThreshold(), brokers);
    }
}
