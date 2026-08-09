package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.config.KafkaConnectionProperties;
import com.osstem.kafkaadmin.kafka.dto.Dtos.MessageRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// 메시지 열람(읽기 전용). 그룹 없이 assign+seek으로 읽으므로 오프셋 커밋이 없고
// 기존 컨슈머 그룹에 영향을 주지 않는다.
@Service
public class MessageQueryService {

    private static final int MAX_VALUE_CHARS = 1000;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(300);
    private static final Duration READ_DEADLINE = Duration.ofSeconds(5);

    private final KafkaConnectionProperties props;

    public MessageQueryService(KafkaConnectionProperties props) {
        this.props = props;
    }

    // 각 파티션의 끝에서 limit개씩 거슬러 읽어 합친 뒤 최신순으로 limit개만 돌려준다.
    public List<MessageRecord> readRecent(String topic, int limit) {
        Map<String, Object> p = new HashMap<>(props.toClientProps());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
            List<PartitionInfo> partitions = consumer.partitionsFor(topic);
            if (partitions == null || partitions.isEmpty()) {
                return List.of();
            }
            List<TopicPartition> tps = partitions.stream()
                    .map(pi -> new TopicPartition(topic, pi.partition()))
                    .toList();
            consumer.assign(tps);
            Map<TopicPartition, Long> end = consumer.endOffsets(tps);
            Map<TopicPartition, Long> begin = consumer.beginningOffsets(tps);

            long target = 0;
            for (TopicPartition tp : tps) {
                long from = Math.max(begin.get(tp), end.get(tp) - limit);
                consumer.seek(tp, from);
                target += end.get(tp) - from;
            }

            List<MessageRecord> out = new ArrayList<>();
            long deadline = System.nanoTime() + READ_DEADLINE.toNanos();
            while (out.size() < target && System.nanoTime() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(POLL_TIMEOUT)) {
                    // end 스냅숏 이후에 새로 들어온 메시지는 이번 조회에서 제외한다
                    if (r.offset() < end.get(new TopicPartition(topic, r.partition()))) {
                        out.add(new MessageRecord(r.partition(), r.offset(),
                                Instant.ofEpochMilli(r.timestamp()), r.key(), truncate(r.value())));
                    }
                }
            }
            out.sort(Comparator.comparing(MessageRecord::timestamp)
                    .thenComparingLong(MessageRecord::offset).reversed());
            return out.subList(0, Math.min(limit, out.size()));
        } catch (KafkaException e) {
            throw new KafkaUnavailableException(e);
        }
    }

    private static String truncate(String value) {
        if (value == null) {
            return null;
        }
        return value.length() > MAX_VALUE_CHARS ? value.substring(0, MAX_VALUE_CHARS) + "…" : value;
    }
}
