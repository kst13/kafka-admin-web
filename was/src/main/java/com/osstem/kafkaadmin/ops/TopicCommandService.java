package com.osstem.kafkaadmin.ops;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

@Service
public class TopicCommandService {

    // Kafka 토픽명 규칙: 영숫자 . _ - 만, 249자 이하
    private static final Pattern TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,249}");

    // 생성 직후 메타데이터 전파 대기: KRaft 에서 createTopics 응답 뒤에도 브로커별 메타데이터 캐시가
    // 잠시 뒤처져 describeTopics 가 UnknownTopicOrPartition 을 돌려준다. 이 상태로 응답하면 화면의
    // 즉시 새로고침이 "브로커 접속 불가"로 보인다. 최대 약 3초(30 x 100ms) 까지만 기다린다.
    private static final int VISIBILITY_ATTEMPTS = 30;
    private static final long VISIBILITY_INTERVAL_MS = 100;

    private static final Logger log = LoggerFactory.getLogger(TopicCommandService.class);

    private final Admin admin;

    public TopicCommandService(Admin admin) {
        this.admin = admin;
    }

    public void createTopic(String name, int partitions, short replicationFactor,
                            Map<String, String> configs) {
        validateName(name);
        if (partitions < 1) throw new IllegalArgumentException("파티션 수는 1 이상이어야 합니다");
        if (replicationFactor < 1) throw new IllegalArgumentException("복제 팩터는 1 이상이어야 합니다");
        NewTopic topic = new NewTopic(name, partitions, replicationFactor);
        if (configs != null && !configs.isEmpty()) {
            topic.configs(configs);
        }
        OpsFutures.await(admin.createTopics(List.of(topic)).all());
        awaitVisible(name);
    }

    // 생성은 이미 성공했으므로 여기서 실패해도 예외를 올리지 않는다(요청 자체는 성공 응답).
    private void awaitVisible(String name) {
        for (int i = 0; i < VISIBILITY_ATTEMPTS; i++) {
            try {
                admin.describeTopics(List.of(name)).allTopicNames().get();
                return;
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    log.warn("토픽 {} 생성 후 조회 확인 실패 (계속 진행): {}", name, e.getCause().toString());
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                Thread.sleep(VISIBILITY_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        log.warn("토픽 {} 생성 후 {}ms 안에 메타데이터가 전파되지 않았다 (계속 진행)",
                name, VISIBILITY_ATTEMPTS * VISIBILITY_INTERVAL_MS);
    }

    public void updateTopic(String name, Integer partitions, Map<String, String> configs) {
        validateName(name);
        boolean hasConfigs = configs != null && !configs.isEmpty();
        if (partitions == null && !hasConfigs) {
            throw new IllegalArgumentException("변경할 파티션 수 또는 설정을 지정해야 합니다");
        }
        if (partitions != null) {
            int current = OpsFutures.await(
                    admin.describeTopics(List.of(name)).allTopicNames()).get(name)
                    .partitions().size();
            if (partitions <= current) {
                throw new IllegalArgumentException(
                        "파티션은 감소할 수 없습니다 (현재 " + current + ")");
            }
            OpsFutures.await(admin.createPartitions(
                    Map.of(name, NewPartitions.increaseTo(partitions))).all());
        }
        if (hasConfigs) {
            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
            List<AlterConfigOp> ops = configs.entrySet().stream()
                    .map(e -> new AlterConfigOp(
                            new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                    .toList();
            OpsFutures.await(admin.incrementalAlterConfigs(Map.of(resource, ops)).all());
        }
    }

    public void deleteTopic(String name) {
        validateName(name);
        OpsFutures.await(admin.deleteTopics(List.of(name)).all());
    }

    private void validateName(String name) {
        if (name == null || !TOPIC_NAME.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    "토픽명은 영문·숫자·'.', '_', '-' 만 사용해 249자 이하로 지정합니다");
        }
    }
}
