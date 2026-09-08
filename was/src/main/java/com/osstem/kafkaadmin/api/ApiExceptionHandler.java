package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
import com.osstem.kafkaadmin.ops.GroupExistsException;
import com.osstem.kafkaadmin.ops.KafkaAppExistsException;
import com.osstem.kafkaadmin.ops.KafkaAppNotFoundException;
import com.osstem.kafkaadmin.schema.IncompatibleSchemaException;
import com.osstem.kafkaadmin.schema.InvalidSchemaException;
import com.osstem.kafkaadmin.schema.SchemaRegistryNotConfiguredException;
import com.osstem.kafkaadmin.schema.SchemaRegistryUnavailableException;
import com.osstem.kafkaadmin.schema.SubjectNotFoundException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(KafkaUnavailableException.class)
    public ResponseEntity<Map<String, String>> kafkaUnavailable(KafkaUnavailableException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(TopicExistsException.class)
    public ResponseEntity<Map<String, String>> topicExists(TopicExistsException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", "이미 존재하는 토픽입니다"));
    }

    @ExceptionHandler(GroupExistsException.class)
    public ResponseEntity<Map<String, String>> groupExists(GroupExistsException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", "이미 존재하는 그룹입니다"));
    }

    @ExceptionHandler(UnknownTopicOrPartitionException.class)
    public ResponseEntity<Map<String, String>> unknownTopic(UnknownTopicOrPartitionException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "존재하지 않는 토픽입니다"));
    }

    // 서비스 검증 실패(파티션 감소 등)와 잘못된 클러스터 제약 요청을 400 으로
    @ExceptionHandler({IllegalArgumentException.class, InvalidPartitionsException.class,
            InvalidReplicationFactorException.class, InvalidConfigurationException.class,
            InvalidTopicException.class, PolicyViolationException.class})
    public ResponseEntity<Map<String, String>> badRequest(RuntimeException e) {
        return ResponseEntity.badRequest()
                .body(Map.of("error", e.getMessage() == null ? "잘못된 요청입니다" : e.getMessage()));
    }

    @ExceptionHandler(KafkaAppExistsException.class)
    public ResponseEntity<Map<String, String>> kafkaAppExists(KafkaAppExistsException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(KafkaAppNotFoundException.class)
    public ResponseEntity<Map<String, String>> kafkaAppNotFound(KafkaAppNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
    }

    // SCRAM 삭제 대상이 브로커에 없음 (토픽/그룹 ops 경로에서도 브로커 리소스 부재로 발생할 수 있어 문구를 일반화)
    @ExceptionHandler(ResourceNotFoundException.class)
    public ResponseEntity<Map<String, String>> resourceNotFound(ResourceNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(Map.of("error", "브로커에 없는 리소스입니다 (SCRAM 계정 등)"));
    }

    // 사이트의 kafka-admin SCRAM 계정에 Cluster ALTER 가 없으면 SCRAM/ACL 변경이 거부된다 (선결 작업: 배포 문서)
    // 토픽/그룹 ops 경로에서도 같은 예외가 날 수 있어 문구를 일반화.
    @ExceptionHandler(ClusterAuthorizationException.class)
    public ResponseEntity<Map<String, String>> clusterAuthorization(ClusterAuthorizationException e) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(Map.of("error", "kafka-admin 계정에 브로커 권한이 부족합니다 (Kafka 계정 관리에는 Cluster Alter 필요)"));
    }

    // --- Schema Registry ---
    @ExceptionHandler(SchemaRegistryNotConfiguredException.class)
    public ResponseEntity<Map<String, String>> schemaRegistryNotConfigured(SchemaRegistryNotConfiguredException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(SchemaRegistryUnavailableException.class)
    public ResponseEntity<Map<String, String>> schemaRegistryUnavailable(SchemaRegistryUnavailableException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(SubjectNotFoundException.class)
    public ResponseEntity<Map<String, String>> subjectNotFound(SubjectNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(IncompatibleSchemaException.class)
    public ResponseEntity<Map<String, Object>> incompatibleSchema(IncompatibleSchemaException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(Map.of("error", e.getMessage(), "details", e.getMessages()));
    }

    @ExceptionHandler(InvalidSchemaException.class)
    public ResponseEntity<Map<String, String>> invalidSchema(InvalidSchemaException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
