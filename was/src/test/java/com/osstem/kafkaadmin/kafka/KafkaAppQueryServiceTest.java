package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicPermission;
import com.osstem.kafkaadmin.ops.KafkaApp;
import com.osstem.kafkaadmin.ops.KafkaAppRepository;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// describeAppUntil: 자기 쓰기 읽기(read-your-write) 재조회. AdminClient 는 호출마다 다른 브로커로 갈 수
// 있어, 방금 반영된 브로커와 다른(뒤처진) 브로커가 응답하면 describeApp 한 번만으로는 옛 상태가 보일 수 있다.
class KafkaAppQueryServiceTest {

    private final Admin admin = mock(Admin.class);
    private final KafkaAppRepository repository = mock(KafkaAppRepository.class);
    private final KafkaAppQueryService service = new KafkaAppQueryService(admin, repository);

    private final KafkaApp app = new KafkaApp("order-api", "dev1", null, Instant.now());

    private void stubScram() {
        DescribeUserScramCredentialsResult r = mock(DescribeUserScramCredentialsResult.class);
        when(r.all()).thenReturn(KafkaFuture.completedFuture(
                Map.of("order-api", mock(UserScramCredentialsDescription.class))));
        when(admin.describeUserScramCredentials()).thenReturn(r);
    }

    @Test
    void 기대_상태가_보일_때까지_재조회하고_그_결과를_돌려준다() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        stubScram();

        List<AclBinding> oldBindings = AclMapping.topicBindings("order-api", "orders", PermissionMode.PRODUCE);
        List<AclBinding> newBindings = AclMapping.topicBindings("order-api", "orders", PermissionMode.CONSUME);
        AtomicInteger calls = new AtomicInteger();
        when(admin.describeAcls(any(AclBindingFilter.class))).thenAnswer(inv -> {
            DescribeAclsResult r = mock(DescribeAclsResult.class);
            if (calls.incrementAndGet() < 3) {
                when(r.values()).thenReturn(KafkaFuture.completedFuture(oldBindings));
            } else {
                when(r.values()).thenReturn(KafkaFuture.completedFuture(newBindings));
            }
            return r;
        });

        KafkaAppDetail detail = service.describeAppUntil("order-api",
                d -> d.permissions().stream().anyMatch(p -> p.topic().equals("orders") && p.mode().equals("consume")));

        assertThat(detail.permissions()).containsExactly(new TopicPermission("orders", "consume"));
        verify(admin, times(3)).describeAcls(any(AclBindingFilter.class));
    }

    @Test
    void 기대_상태에_끝내_도달하지_못하면_마지막_조회_결과를_돌려준다() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        stubScram();

        List<AclBinding> bindings = AclMapping.topicBindings("order-api", "orders", PermissionMode.PRODUCE);
        DescribeAclsResult r = mock(DescribeAclsResult.class);
        when(r.values()).thenReturn(KafkaFuture.completedFuture(bindings));
        when(admin.describeAcls(any(AclBindingFilter.class))).thenReturn(r);

        KafkaAppDetail detail = service.describeAppUntil("order-api",
                d -> d.permissions().stream().anyMatch(p -> p.mode().equals("consume")));

        assertThat(detail.permissions()).containsExactly(new TopicPermission("orders", "produce"));
    }

    @Test
    void 재조회_중_읽기가_실패해도_이전에_읽은_상태를_돌려준다() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        stubScram();

        List<AclBinding> bindings = AclMapping.topicBindings("order-api", "orders", PermissionMode.PRODUCE);
        AtomicInteger calls = new AtomicInteger();
        when(admin.describeAcls(any(AclBindingFilter.class))).thenAnswer(inv -> {
            DescribeAclsResult r = mock(DescribeAclsResult.class);
            if (calls.incrementAndGet() == 1) {
                when(r.values()).thenReturn(KafkaFuture.completedFuture(bindings));
            } else {
                KafkaFutureImpl<Collection<AclBinding>> f = new KafkaFutureImpl<>();
                f.completeExceptionally(new TimeoutException("slow"));
                when(r.values()).thenReturn(f);
            }
            return r;
        });

        // predicate 는 계속 false 라 두 번째 조회까지 가지만, 그 조회가 브로커 접속 실패로 죽는다.
        // 이미 성공한(첫 번째) 조회 결과가 있으므로 예외를 던지지 않고 그 결과를 돌려준다.
        KafkaAppDetail detail = service.describeAppUntil("order-api",
                d -> d.permissions().stream().anyMatch(p -> p.mode().equals("consume")));

        assertThat(detail.permissions()).containsExactly(new TopicPermission("orders", "produce"));
    }

    @Test
    void 첫_조회부터_실패하면_돌려줄_결과가_없으므로_그대로_던진다() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        stubScram();

        DescribeAclsResult failing = mock(DescribeAclsResult.class);
        KafkaFutureImpl<Collection<AclBinding>> f = new KafkaFutureImpl<>();
        f.completeExceptionally(new TimeoutException("slow"));
        when(failing.values()).thenReturn(f);
        when(admin.describeAcls(any(AclBindingFilter.class))).thenReturn(failing);

        assertThatThrownBy(() -> service.describeAppUntil("order-api", d -> true))
                .isInstanceOf(com.osstem.kafkaadmin.kafka.KafkaUnavailableException.class);
    }
}
