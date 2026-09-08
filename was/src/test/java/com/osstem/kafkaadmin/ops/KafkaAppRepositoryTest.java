package com.osstem.kafkaadmin.ops;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.test.context.TestPropertySource;
import java.time.Instant;
import static org.assertj.core.api.Assertions.*;

// H2 인메모리로 유니크 제약과 이름 조회를 검증한다. named DB 공유를 피하려고 고유 이름을 쓴다.
@SpringBootTest
@TestPropertySource(properties = "spring.datasource.url=jdbc:h2:mem:kafka-app-repo-test;DB_CLOSE_DELAY=-1")
class KafkaAppRepositoryTest {

    @Autowired KafkaAppRepository repository;

    @Test
    void 이름으로_조회하고_메타데이터를_갱신한다() {
        repository.save(new KafkaApp("repo-t-order-api", "dev1", "주문", Instant.now()));
        KafkaApp found = repository.findByName("repo-t-order-api").orElseThrow();
        assertThat(found.getOwnerUsername()).isEqualTo("dev1");
        found.updateMeta("dev2", "주문 서비스");
        repository.save(found);
        assertThat(repository.findByName("repo-t-order-api").orElseThrow().getDescription()).isEqualTo("주문 서비스");
        assertThat(repository.existsByName("repo-t-order-api")).isTrue();
        assertThat(repository.existsByName("repo-t-missing")).isFalse();
    }

    @Test
    void 같은_이름은_저장할_수_없다() {
        repository.save(new KafkaApp("repo-t-dup", null, null, Instant.now()));
        assertThatThrownBy(() -> repository.saveAndFlush(new KafkaApp("repo-t-dup", null, null, Instant.now())))
                .isInstanceOf(DataIntegrityViolationException.class);
    }
}
