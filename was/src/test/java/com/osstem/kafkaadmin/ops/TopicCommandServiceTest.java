package com.osstem.kafkaadmin.ops;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.*;

// 생성 직후 메타데이터 전파 지연: createTopics 는 성공했지만 describeTopics 가 잠시
// UnknownTopicOrPartition 을 돌려주는 구간이 있다. 서비스는 토픽이 조회될 때까지 기다린 뒤 응답한다.
class TopicCommandServiceTest {

    private final Admin admin = mock(Admin.class);
    private final TopicCommandService service = new TopicCommandService(admin);

    @Test
    void 생성_후_토픽이_조회될_때까지_기다린다() {
        CreateTopicsResult created = mock(CreateTopicsResult.class);
        when(created.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection())).thenReturn(created);

        AtomicInteger calls = new AtomicInteger();
        when(admin.describeTopics(anyCollection())).thenAnswer(inv -> {
            DescribeTopicsResult r = mock(DescribeTopicsResult.class);
            if (calls.incrementAndGet() < 3) {
                KafkaFutureImpl<Map<String, TopicDescription>> f = new KafkaFutureImpl<>();
                f.completeExceptionally(new UnknownTopicOrPartitionException("not yet"));
                when(r.allTopicNames()).thenReturn(f);
            } else {
                when(r.allTopicNames()).thenReturn(KafkaFuture.completedFuture(
                        Map.of("t", mock(TopicDescription.class))));
            }
            return r;
        });

        assertThatCode(() -> service.createTopic("t", 1, (short) 1, null)).doesNotThrowAnyException();
        verify(admin, times(3)).describeTopics(anyCollection());
    }

    @Test
    void 전파_확인이_계속_실패해도_생성_자체는_성공으로_처리한다() {
        CreateTopicsResult created = mock(CreateTopicsResult.class);
        when(created.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection())).thenReturn(created);

        DescribeTopicsResult r = mock(DescribeTopicsResult.class);
        KafkaFutureImpl<Map<String, TopicDescription>> f = new KafkaFutureImpl<>();
        f.completeExceptionally(new UnknownTopicOrPartitionException("never"));
        when(r.allTopicNames()).thenReturn(f);
        when(admin.describeTopics(anyCollection())).thenReturn(r);

        assertThatCode(() -> service.createTopic("t", 1, (short) 1, null)).doesNotThrowAnyException();
        verify(admin, atLeast(2)).describeTopics(anyCollection());
    }
}
