package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.MonitorQueryService;
import com.osstem.kafkaadmin.monitor.AlertEvent;
import com.osstem.kafkaadmin.monitor.AlertEventRepository;
import com.osstem.kafkaadmin.monitor.CertExpiryChecker;
import com.osstem.kafkaadmin.monitor.CertExpiryChecker.CertStatus;
import com.osstem.kafkaadmin.monitor.MetricSampleRepository;
import com.osstem.kafkaadmin.monitor.MetricsCollector;
import com.osstem.kafkaadmin.monitor.MonitorProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;

@RestController
@RequestMapping("/api")
public class MonitorController {

    public record SamplePoint(Instant sampledAt, double value) {}
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
