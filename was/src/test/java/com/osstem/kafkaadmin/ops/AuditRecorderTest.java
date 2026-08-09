package com.osstem.kafkaadmin.ops;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import static org.assertj.core.api.Assertions.*;

// H2 인메모리로 JPA 저장까지 검증한다. named DB 공유를 피하려고 고유 이름을 쓴다.
@SpringBootTest
@TestPropertySource(properties = "spring.datasource.url=jdbc:h2:mem:audit-recorder-test;DB_CLOSE_DELAY=-1")
class AuditRecorderTest {

    @Autowired AuditRecorder recorder;
    @Autowired AuditLogRepository repository;

    @Test
    void 성공한_조치는_SUCCESS로_기록된다() {
        recorder.record("audit-t-admin", "TOPIC_CREATE", "audit-t-orders", "{\"partitions\":3}",
                () -> {});
        AuditLog log = repository.findAll().stream()
                .filter(l -> l.getTarget().equals("audit-t-orders")).findFirst().orElseThrow();
        assertThat(log.getActor()).isEqualTo("audit-t-admin");
        assertThat(log.getResult()).isEqualTo("SUCCESS");
        assertThat(log.getErrorMessage()).isNull();
        assertThat(log.getExecutedAt()).isNotNull();
    }

    @Test
    void 실패한_조치는_FAILED로_기록되고_예외는_다시_던진다() {
        assertThatThrownBy(() ->
                recorder.record("audit-t-admin", "TOPIC_DELETE", "audit-t-broken", "{}",
                        () -> { throw new IllegalStateException("boom"); }))
                .isInstanceOf(IllegalStateException.class);
        AuditLog log = repository.findAll().stream()
                .filter(l -> l.getTarget().equals("audit-t-broken")).findFirst().orElseThrow();
        assertThat(log.getResult()).isEqualTo("FAILED");
        assertThat(log.getErrorMessage()).isEqualTo("boom");
    }

    @Test
    void params가_2000자를_초과하면_잘려서_저장된다() {
        String hugeParams = "x".repeat(3000);
        recorder.record("audit-t-admin", "TOPIC_CREATE", "audit-t-huge-params", hugeParams,
                () -> {});
        AuditLog log = repository.findAll().stream()
                .filter(l -> l.getTarget().equals("audit-t-huge-params")).findFirst().orElseThrow();
        assertThat(log.getParams()).hasSize(2000);
    }
}
