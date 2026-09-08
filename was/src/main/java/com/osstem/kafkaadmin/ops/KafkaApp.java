package com.osstem.kafkaadmin.ops;

import jakarta.persistence.*;
import java.time.Instant;

// Kafka 앱 계정의 메타데이터. 원본(SCRAM·ACL)은 브로커에 있고 여기엔 이름·담당자·설명만 둔다.
// name 은 SCRAM 사용자명과 같다. 비밀번호와 권한은 저장하지 않는다.
@Entity
@Table(name = "kafka_app", uniqueConstraints = @UniqueConstraint(name = "uk_kafka_app_name", columnNames = "name"))
public class KafkaApp {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(nullable = false, length = 64)
    private String name;
    private String ownerUsername;
    @Column(length = 500)
    private String description;
    private Instant createdAt;

    protected KafkaApp() {}

    public KafkaApp(String name, String ownerUsername, String description, Instant createdAt) {
        this.name = name;
        this.ownerUsername = ownerUsername;
        this.description = description;
        this.createdAt = createdAt;
    }

    public void updateMeta(String ownerUsername, String description) {
        this.ownerUsername = ownerUsername;
        this.description = description;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public String getOwnerUsername() { return ownerUsername; }
    public String getDescription() { return description; }
    public Instant getCreatedAt() { return createdAt; }
}
