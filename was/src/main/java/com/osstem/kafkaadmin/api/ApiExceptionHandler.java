package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
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
}
