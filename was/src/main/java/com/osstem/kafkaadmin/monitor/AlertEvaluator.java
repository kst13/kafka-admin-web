package com.osstem.kafkaadmin.monitor;

import org.springframework.stereotype.Service;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

@Service
public class AlertEvaluator {

    private final AlertEventRepository alerts;
    private final AlertNotifier notifier;
    private final MonitorProperties props;
    private final MetricSampleRepository samples;

    public AlertEvaluator(AlertEventRepository alerts, AlertNotifier notifier,
                          MonitorProperties props, MetricSampleRepository samples) {
        this.alerts = alerts;
        this.notifier = notifier;
        this.props = props;
        this.samples = samples;
    }

    public void evaluate(List<MetricSample> batch) {
        for (MetricSample s : batch) {
            double v = s.getValue();
            String subject = s.getSubjectKey();
            switch (s.getMetricType()) {
                case "LAG" -> {
                    if (v > props.lagThreshold()) {
                        raise("LAG_HIGH", subject,
                                "컨슈머 그룹 %s 랙 %.0f (임계치 %d)".formatted(subject, v, props.lagThreshold()),
                                v, props.lagThreshold());
                    }
                }
                case "DISK_USED_PCT" -> {
                    if (v > props.diskUsedPctThreshold()) {
                        raise("DISK_HIGH", subject,
                                "브로커 %s 디스크 사용률 %.1f%% (임계치 %d%%)".formatted(subject, v, props.diskUsedPctThreshold()),
                                v, props.diskUsedPctThreshold());
                    }
                }
                // --- 클러스터 (Prometheus 스냅샷 + 기존 URP) ---
                case "OFFLINE_PARTITIONS" -> {
                    if (v > 0) raise("OFFLINE_PARTITIONS", subject, "오프라인 파티션 %.0f개".formatted(v), v, 0);
                }
                case "URP" -> {
                    if (v > 0) raise("URP_HIGH", subject, "미복제 파티션(URP) %.0f개".formatted(v), v, 0);
                }
                case "UNCLEAN_ELECTIONS" -> previous(s).ifPresent(prev -> {
                    if (v > prev) {
                        raise("UNCLEAN_ELECTION", subject,
                                "언클린 리더 선출 %.0f회 발생 (누적 %.0f)".formatted(v - prev, v), v - prev, prev);
                    }
                });
                case "ACTIVE_BROKERS" -> previous(s).ifPresent(prev -> {
                    if (v < prev) {
                        raise("BROKER_DOWN", subject,
                                "활성 브로커 수 감소 %.0f → %.0f".formatted(prev, v), v, prev);
                    }
                });
                // --- 브로커 (subjectKey = 브로커 id) ---
                case "P99_PRODUCE_MS" -> {
                    if (v > props.p99ProduceMsThreshold()) {
                        raise("LATENCY_HIGH", subject,
                                "브로커 %s Produce p99 %.0fms (임계치 %dms)".formatted(subject, v, props.p99ProduceMsThreshold()),
                                v, props.p99ProduceMsThreshold());
                    }
                }
                case "P99_FETCH_MS" -> {
                    if (v > props.p99FetchMsThreshold()) {
                        raise("LATENCY_HIGH", subject,
                                "브로커 %s Fetch p99 %.0fms (임계치 %dms)".formatted(subject, v, props.p99FetchMsThreshold()),
                                v, props.p99FetchMsThreshold());
                    }
                }
                case "HANDLER_IDLE_PCT" -> {
                    if (v < props.handlerIdleMinPct()) {
                        raise("HANDLER_SATURATED", subject,
                                "브로커 %s 요청 핸들러 유휴율 %.1f%% (최소 %d%%)".formatted(subject, v, props.handlerIdleMinPct()),
                                v, props.handlerIdleMinPct());
                    }
                }
                case "HEAP_USED_PCT" -> {
                    if (v > props.heapUsedPctThreshold()) {
                        raise("HEAP_HIGH", subject,
                                "브로커 %s 힙 사용률 %.1f%% (임계치 %d%%)".formatted(subject, v, props.heapUsedPctThreshold()),
                                v, props.heapUsedPctThreshold());
                    }
                }
                default -> { /* BROKER_COUNT, UNDER_MIN_ISR, CONSUMED_*, PRODUCED_* 는 이력만 */ }
            }
        }
    }

    // 직전 저장 샘플(이번 샘플 시각보다 앞선 것) — 첫 샘플이면 empty 라 증감 판정을 하지 않는다
    private Optional<Double> previous(MetricSample s) {
        return samples.findTopByMetricTypeAndSubjectKeyAndSampledAtBeforeOrderBySampledAtDesc(
                        s.getMetricType(), s.getSubjectKey(), s.getSampledAt())
                .map(MetricSample::getValue);
    }

    // 쿨다운: 동일 (ruleType, subjectKey) 알림이 cooldownMinutes 내에 있으면 중복 발생 억제
    public void raise(String ruleType, String subjectKey, String message,
                      double value, double threshold) {
        Instant cutoff = Instant.now().minus(props.cooldownMinutes(), ChronoUnit.MINUTES);
        if (alerts.existsByRuleTypeAndSubjectKeyAndOccurredAtAfter(ruleType, subjectKey, cutoff)) {
            return;
        }
        AlertEvent event = alerts.save(
                new AlertEvent(ruleType, subjectKey, message, value, threshold, Instant.now()));
        notifier.send(event);
    }
}
