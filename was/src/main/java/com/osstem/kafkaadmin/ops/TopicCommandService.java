package com.osstem.kafkaadmin.ops;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Service
public class TopicCommandService {

    // Kafka 토픽명 규칙: 영숫자 . _ - 만, 249자 이하
    private static final Pattern TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,249}");

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
