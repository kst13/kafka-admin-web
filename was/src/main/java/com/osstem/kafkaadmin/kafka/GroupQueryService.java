package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupMember;
import com.osstem.kafkaadmin.kafka.dto.Dtos.GroupSummary;
import com.osstem.kafkaadmin.kafka.dto.Dtos.PartitionLag;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class GroupQueryService {

    private final Admin admin;

    public GroupQueryService(Admin admin) {
        this.admin = admin;
    }

    public List<GroupSummary> listGroups() {
        List<String> ids = KafkaFutures.await(admin.listConsumerGroups().all()).stream()
                .map(l -> l.groupId())
                .toList();
        if (ids.isEmpty()) {
            return List.of();
        }
        Map<String, ConsumerGroupDescription> descs =
                KafkaFutures.await(admin.describeConsumerGroups(ids).all());
        return descs.values().stream()
                .map(d -> new GroupSummary(d.groupId(), d.state().toString(), d.members().size()))
                .sorted(Comparator.comparing(GroupSummary::groupId))
                .toList();
    }

    public GroupDetail describeGroup(String groupId) {
        // 컨슈머 그룹 설명 조회
        ConsumerGroupDescription desc =
                KafkaFutures.await(admin.describeConsumerGroups(List.of(groupId)).all()).get(groupId);
        // 커밋된 오프셋 조회
        Map<TopicPartition, OffsetAndMetadata> committed =
                KafkaFutures.await(admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata());

        // 각 파티션의 최신 오프셋 조회
        Map<TopicPartition, OffsetSpec> latestSpec = committed.keySet().stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ends =
                KafkaFutures.await(admin.listOffsets(latestSpec).all());

        // 파티션별 랙 계산
        List<PartitionLag> lags = committed.entrySet().stream()
                .map(e -> {
                    long committedOffset = e.getValue().offset();
                    long end = ends.get(e.getKey()).offset();
                    return new PartitionLag(e.getKey().topic(), e.getKey().partition(),
                            committedOffset, end, Math.max(0, end - committedOffset));
                })
                .sorted(Comparator.comparing(PartitionLag::topic)
                        .thenComparingInt(PartitionLag::partition))
                .toList();
        long totalLag = lags.stream().mapToLong(PartitionLag::lag).sum();

        // 활성 멤버(개별 컨슈머 인스턴스)와 담당 파티션 — 리밸런스·파티션 쏠림 추적용
        List<GroupMember> members = desc.members().stream()
                .map(m -> new GroupMember(m.consumerId(), m.clientId(), m.host(),
                        m.assignment().topicPartitions().stream()
                                .sorted(Comparator.comparing(TopicPartition::topic)
                                        .thenComparingInt(TopicPartition::partition))
                                .map(TopicPartition::toString)
                                .toList()))
                .sorted(Comparator.comparing(GroupMember::clientId)
                        .thenComparing(GroupMember::memberId))
                .toList();
        return new GroupDetail(groupId, desc.state().toString(), lags, totalLag, members);
    }
}
