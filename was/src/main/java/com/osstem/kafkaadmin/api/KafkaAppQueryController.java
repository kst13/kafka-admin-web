package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.KafkaAppQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppSummary;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

// Kafka 앱 계정 조회. /api/** 인증만 요구 — DEVELOPER 도 본다. 비밀번호는 어떤 응답에도 없다.
@RestController
@RequestMapping("/api/kafka-apps")
public class KafkaAppQueryController {

    private final KafkaAppQueryService queries;

    public KafkaAppQueryController(KafkaAppQueryService queries) {
        this.queries = queries;
    }

    @GetMapping
    public List<KafkaAppSummary> list() {
        return queries.listApps();
    }

    @GetMapping("/{name}")
    public KafkaAppDetail describe(@PathVariable String name) {
        return queries.describeApp(name);
    }
}
