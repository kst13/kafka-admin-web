package com.osstem.kafkaadmin.monitor;

import org.springframework.stereotype.Component;

@Component
public class NoopAlertNotifier implements AlertNotifier {
    @Override
    public void send(AlertEvent event) {
        // 의도적으로 아무 것도 하지 않는다 — 알림은 DB 이력으로만 남는다.
    }
}
