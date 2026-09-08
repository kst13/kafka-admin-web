package com.osstem.kafkaadmin.ops;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;
import java.util.Optional;

public interface KafkaAppRepository extends JpaRepository<KafkaApp, Long> {
    Optional<KafkaApp> findByName(String name);
    boolean existsByName(String name);
    @Transactional
    void deleteByName(String name);
}
