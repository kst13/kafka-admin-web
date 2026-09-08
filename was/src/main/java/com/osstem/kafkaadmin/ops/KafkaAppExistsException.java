package com.osstem.kafkaadmin.ops;

// 이미 브로커에 있는 SCRAM 계정이거나 이미 등록된 메타데이터 (-> 409)
public class KafkaAppExistsException extends RuntimeException {
    public KafkaAppExistsException(String name) {
        super("이미 존재하는 Kafka 계정입니다: " + name);
    }
}
