package com.osstem.kafkaadmin.monitor;

import org.springframework.data.jpa.repository.JpaRepository;
import java.time.Instant;
import java.util.List;

public interface AlertEventRepository extends JpaRepository<AlertEvent, Long> {
    List<AlertEvent> findTop50ByOrderByOccurredAtDesc();
    boolean existsByRuleTypeAndSubjectKeyAndOccurredAtAfter(
            String ruleType, String subjectKey, Instant after);
}
