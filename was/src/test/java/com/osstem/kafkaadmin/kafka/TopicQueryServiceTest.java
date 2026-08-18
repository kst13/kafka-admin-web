package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicSummary;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

// 목록 조회 중 생성/삭제가 진행 중인 토픽은 listTopics 에는 있지만 describeTopics 가
// UnknownTopicOrPartition 을 돌려줄 수 있다. 그 토픽만 건너뛰고 나머지는 정상 반환한다.
class TopicQueryServiceTest {

    private final Admin admin = mock(Admin.class);
    private final TopicQueryService service = new TopicQueryService(admin);

    private static TopicDescription desc(String name) {
        Node n = new Node(1, "b", 9092);
        return new TopicDescription(name, false,
                List.of(new TopicPartitionInfo(0, n, List.of(n), List.of(n))));
    }

    private static <T> KafkaFuture<T> failed(Throwable t) {
        KafkaFutureImpl<T> f = new KafkaFutureImpl<>();
        f.completeExceptionally(t);
        return f;
    }

    private void stubList(Set<String> names) {
        ListTopicsResult list = mock(ListTopicsResult.class);
        when(list.names()).thenReturn(KafkaFuture.completedFuture(names));
        when(admin.listTopics(any())).thenReturn(list);
    }

    @Test
    void 전파_중인_토픽은_건너뛰고_나머지를_반환한다() {
        stubList(Set.of("ok", "pending"));
        DescribeTopicsResult r = mock(DescribeTopicsResult.class);
        when(r.topicNameValues()).thenReturn(Map.of(
                "ok", KafkaFuture.completedFuture(desc("ok")),
                "pending", failed(new UnknownTopicOrPartitionException("not yet"))));
        when(admin.describeTopics(anyCollection())).thenReturn(r);

        List<TopicSummary> topics = service.listTopics();
        assertThat(topics).extracting(TopicSummary::name).containsExactly("ok");
    }

    @Test
    void 그_외_실패는_여전히_KafkaUnavailable_로_올라간다() {
        stubList(Set.of("ok"));
        DescribeTopicsResult r = mock(DescribeTopicsResult.class);
        when(r.topicNameValues()).thenReturn(Map.of("ok", failed(new TimeoutException("slow"))));
        when(admin.describeTopics(anyCollection())).thenReturn(r);

        assertThatThrownBy(service::listTopics).isInstanceOf(KafkaUnavailableException.class);
    }
}
