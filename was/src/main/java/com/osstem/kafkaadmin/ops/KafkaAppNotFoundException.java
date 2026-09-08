package com.osstem.kafkaadmin.ops;

// 메타데이터가 없는(미등록) 앱에 대한 변경 요청 (-> 404)
public class KafkaAppNotFoundException extends RuntimeException {
    public KafkaAppNotFoundException(String name) {
        super("등록되지 않은 앱입니다: " + name);
    }
}
