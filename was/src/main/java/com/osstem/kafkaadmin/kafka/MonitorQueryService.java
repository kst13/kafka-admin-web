package com.osstem.kafkaadmin.kafka;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// 장애 감시용 조회: URP(복제 부족 파티션 수), 브로커별 디스크 사용률
@Service
public class MonitorQueryService {

    private final Admin admin;

    public MonitorQueryService(Admin admin) {
        this.admin = admin;
    }

    // 내부 토픽 포함 전체 기준. isr < replicas 인 파티션 수.
    public int countUnderReplicatedPartitions() {
        Set<String> names = KafkaFutures.await(
                admin.listTopics(new ListTopicsOptions().listInternal(true)).names());
        if (names.isEmpty()) {
            return 0;
        }
        return KafkaFutures.await(admin.describeTopics(names).allTopicNames()).values().stream()
                .flatMap(d -> d.partitions().stream())
                .mapToInt(p -> p.isr().size() < p.replicas().size() ? 1 : 0)
                .sum();
    }

    // 로그 디렉토리의 total/usable 바이트(KIP-827)로 사용률(%)을 계산한다.
    // totalBytes를 제공하지 않는 브로커(구버전/특수 FS)는 결과에서 제외한다.
    public Map<Integer, Double> diskUsedPercentByBroker() {
        List<Integer> brokerIds = KafkaFutures.await(admin.describeCluster().nodes()).stream()
                .map(n -> n.id())
                .toList();
        Map<Integer, Map<String, LogDirDescription>> all =
                KafkaFutures.await(admin.describeLogDirs(brokerIds).allDescriptions());
        Map<Integer, Double> result = new HashMap<>();
        all.forEach((brokerId, dirs) -> {
            long total = 0;
            long usable = 0;
            for (LogDirDescription d : dirs.values()) {
                if (d.totalBytes().isPresent() && d.usableBytes().isPresent()) {
                    total += d.totalBytes().getAsLong();
                    usable += d.usableBytes().getAsLong();
                }
            }
            if (total > 0) {
                result.put(brokerId, (total - usable) * 100.0 / total);
            }
        });
        return result;
    }
}
