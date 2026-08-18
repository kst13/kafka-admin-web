package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
import com.osstem.kafkaadmin.ops.GroupExistsException;
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
}
