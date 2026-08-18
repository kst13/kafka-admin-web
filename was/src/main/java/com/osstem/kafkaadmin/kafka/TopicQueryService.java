package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.PartitionInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicSummary;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Service
public class TopicQueryService {

    // 조회할 설정 항목들
    private static final Set<String> SHOWN_CONFIGS =
            Set.of("retention.ms", "min.insync.replicas", "cleanup.policy");

    private final Admin admin;

    public TopicQueryService(Admin admin) {
        this.admin = admin;
    }

    public List<TopicSummary> listTopics() {
        Set<String> names = KafkaFutures.await(
                admin.listTopics(new ListTopicsOptions().listInternal(false)).names());
        // 토픽별 future 로 받는다: 생성/삭제가 진행 중인 토픽은 목록에는 있지만 describe 가
        // UnknownTopicOrPartition 을 돌려줄 수 있는데, 그 하나 때문에 목록 전체를 실패시키지 않는다.
        Map<String, KafkaFuture<TopicDescription>> futures =
                admin.describeTopics(names).topicNameValues();
        List<TopicDescription> descriptions = new ArrayList<>();
        for (KafkaFuture<TopicDescription> f : futures.values()) {
            try {
                descriptions.add(f.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new KafkaUnavailableException(e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) continue;
                throw new KafkaUnavailableException(e.getCause());
            }
        }
        return descriptions.stream()
                .map(d -> new TopicSummary(d.name(), d.partitions().size(),
                        d.partitions().get(0).replicas().size()))
                .sorted(Comparator.comparing(TopicSummary::name))
                .toList();
    }

    public TopicDetail describeTopic(String name) {
        TopicDescription desc = KafkaFutures.await(
                admin.describeTopics(List.of(name)).allTopicNames()).get(name);
        List<PartitionInfo> partitions = desc.partitions().stream()
                .map(this::toPartitionInfo)
                .sorted(Comparator.comparingInt(PartitionInfo::partition))
                .toList();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Map<String, String> configs = KafkaFutures.await(
                        admin.describeConfigs(List.of(resource)).all())
                .get(resource).entries().stream()
                .filter(e -> SHOWN_CONFIGS.contains(e.name()))
                .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
        return new TopicDetail(name, partitions, configs);
    }

    private PartitionInfo toPartitionInfo(TopicPartitionInfo p) {
        return new PartitionInfo(p.partition(),
                p.leader() == null ? -1 : p.leader().id(),
                p.replicas().stream().map(Node::id).toList(),
                p.isr().stream().map(Node::id).toList());
    }
}
