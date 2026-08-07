package com.osstem.kafkaadmin.monitor;

import org.springframework.stereotype.Service;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

@Service
public class AlertEvaluator {

    private final AlertEventRepository alerts;
    private final AlertNotifier notifier;
    private final MonitorProperties props;

    public AlertEvaluator(AlertEventRepository alerts, AlertNotifier notifier,
                          MonitorProperties props) {
        this.alerts = alerts;
        this.notifier = notifier;
        this.props = props;
    }

    public void evaluate(List<MetricSample> samples) {
        for (MetricSample s : samples) {
            switch (s.getMetricType()) {
                case "LAG" -> {
                    if (s.getValue() > props.lagThreshold()) {
                        raise("LAG_HIGH", s.getSubjectKey(),
                                "컨슈머 그룹 %s 랙 %.0f (임계치 %d)".formatted(
                                        s.getSubjectKey(), s.getValue(), props.lagThreshold()),
                                s.getValue(), props.lagThreshold());
                    }
                }
                case "DISK_USED_PCT" -> {
                    if (s.getValue() > props.diskUsedPctThreshold()) {
                        raise("DISK_HIGH", s.getSubjectKey(),
                                "브로커 %s 디스크 사용률 %.1f%% (임계치 %d%%)".formatted(
                                        s.getSubjectKey(), s.getValue(), props.diskUsedPctThreshold()),
                                s.getValue(), props.diskUsedPctThreshold());
                    }
                }
                default -> { /* URP, BROKER_COUNT 는 이력만 (증감 알림은 이후 단계) */ }
            }
        }
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
