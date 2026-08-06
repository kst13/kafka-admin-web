package com.osstem.kafkaadmin.kafka;

public class KafkaUnavailableException extends RuntimeException {
    public KafkaUnavailableException(Throwable cause) {
        super("브로커 접속 불가: " + cause.getMessage(), cause);
    }
}
