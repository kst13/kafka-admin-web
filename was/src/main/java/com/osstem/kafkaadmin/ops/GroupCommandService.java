package com.osstem.kafkaadmin.ops;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// 컨슈머 그룹 사전 등록. Kafka 에는 "그룹 생성" API 가 없다 — 그룹은 컨슈머가 붙거나 오프셋이
// 커밋되는 순간 생긴다. 그래서 선택한 토픽의 전 파티션에 시작 위치(earliest/latest) 오프셋을
// 그 그룹 이름으로 커밋해 두면, 그룹이 Empty 상태로 목록에 나타나고 앱이 나중에 같은 group.id 로
// 붙었을 때 그 위치부터 읽는다.
@Service
public class GroupCommandService {

    public enum StartFrom { EARLIEST, LATEST }

    private final Admin admin;

    public GroupCommandService(Admin admin) {
        this.admin = admin;
    }

    public void registerGroup(String groupId, List<String> topics, StartFrom startFrom) {
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("그룹명을 지정해야 합니다");
        }
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("토픽을 1개 이상 지정해야 합니다");
        }
        if (startFrom == null) {
            throw new IllegalArgumentException("시작 위치(earliest/latest)를 지정해야 합니다");
        }
        boolean exists = OpsFutures.await(admin.listConsumerGroups().all()).stream()
                .anyMatch(g -> g.groupId().equals(groupId));
        if (exists) {
            throw new GroupExistsException(groupId);
        }
        // 없는 토픽이면 UnknownTopicOrPartitionException 이 그대로 올라간다 (→ 404)
        Map<String, TopicDescription> descriptions =
                OpsFutures.await(admin.describeTopics(topics).allTopicNames());
        OffsetSpec spec = startFrom == StartFrom.EARLIEST ? OffsetSpec.earliest() : OffsetSpec.latest();
        Map<TopicPartition, OffsetSpec> query = descriptions.values().stream()
                .flatMap(d -> d.partitions().stream()
                        .map(p -> new TopicPartition(d.name(), p.partition())))
                .collect(Collectors.toMap(tp -> tp, tp -> spec));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> offsets =
                OpsFutures.await(admin.listOffsets(query).all());
        Map<TopicPartition, OffsetAndMetadata> commit = offsets.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        e -> new OffsetAndMetadata(e.getValue().offset())));
        OpsFutures.await(admin.alterConsumerGroupOffsets(groupId, commit).all());
    }
}
