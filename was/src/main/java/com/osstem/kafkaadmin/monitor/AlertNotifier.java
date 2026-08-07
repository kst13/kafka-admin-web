package com.osstem.kafkaadmin.monitor;

// 알림 외부 발송 확장점. 2단계는 이력만 남기고(Noop), 웹훅/메일은 이후 단계에서 구현체 추가.
public interface AlertNotifier {
    void send(AlertEvent event);
}
