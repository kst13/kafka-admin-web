package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.AclMapping;
import com.osstem.kafkaadmin.kafka.PermissionMode;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

// 브로커 호출 순서와 그룹 ACL 보정 규칙을 mock Admin 으로 검증한다. 실브로커 왕복은 IT 에서.
class KafkaAppCommandServiceTest {

    private final Admin admin = mock(Admin.class);
    private final KafkaAppRepository repository = mock(KafkaAppRepository.class);
    private final KafkaAppCommandService service = new KafkaAppCommandService(admin, repository);

    private final KafkaApp app = new KafkaApp("order-api", "dev1", null, Instant.now());

    @BeforeEach
    void stubs() {
        AlterUserScramCredentialsResult alter = mock(AlterUserScramCredentialsResult.class);
        when(alter.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.alterUserScramCredentials(anyList())).thenReturn(alter);

        CreateAclsResult created = mock(CreateAclsResult.class);
        when(created.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createAcls(anyCollection())).thenReturn(created);

        DeleteAclsResult deleted = mock(DeleteAclsResult.class);
        when(deleted.all()).thenReturn(KafkaFuture.completedFuture(List.of()));
        when(admin.deleteAcls(anyCollection())).thenReturn(deleted);

        DescribeTopicsResult topics = mock(DescribeTopicsResult.class);
        when(topics.allTopicNames()).thenReturn(KafkaFuture.completedFuture(
                Map.of("orders", mock(TopicDescription.class))));
        when(admin.describeTopics(anyCollection())).thenReturn(topics);

        scramUsers(Map.of());
        acls(List.of());
    }

    private void scramUsers(Map<String, UserScramCredentialsDescription> users) {
        DescribeUserScramCredentialsResult r = mock(DescribeUserScramCredentialsResult.class);
        when(r.all()).thenReturn(KafkaFuture.completedFuture(users));
        when(admin.describeUserScramCredentials()).thenReturn(r);
    }

    private void acls(Collection<AclBinding> bindings) {
        DescribeAclsResult r = mock(DescribeAclsResult.class);
        when(r.values()).thenReturn(KafkaFuture.completedFuture(bindings));
        when(admin.describeAcls(any(AclBindingFilter.class))).thenReturn(r);
    }

    @Test
    void 생성은_SCRAM_upsert_후_메타데이터를_저장하고_비밀번호를_돌려준다() {
        when(repository.existsByName("order-api")).thenReturn(false);
        String pw = service.create("order-api", "dev1", "주문");
        assertThat(pw).matches("[A-Za-z0-9]{24}");
        ArgumentCaptor<List<org.apache.kafka.clients.admin.UserScramCredentialAlteration>> cap =
                ArgumentCaptor.forClass(List.class);
        verify(admin).alterUserScramCredentials(cap.capture());
        UserScramCredentialUpsertion up = (UserScramCredentialUpsertion) cap.getValue().get(0);
        assertThat(up.user()).isEqualTo("order-api");
        assertThat(new String(up.password())).isEqualTo(pw);
        verify(repository).save(argThat(a -> a.getName().equals("order-api") && "dev1".equals(a.getOwnerUsername())));
    }

    @Test
    void 브로커에_이미_있는_SCRAM_계정은_409() {
        scramUsers(Map.of("order-api", mock(UserScramCredentialsDescription.class)));
        assertThatThrownBy(() -> service.create("order-api", null, null))
                .isInstanceOf(KafkaAppExistsException.class);
        verify(admin, never()).alterUserScramCredentials(anyList());
    }

    @Test
    void 이름_패턴_위반은_400() {
        assertThatThrownBy(() -> service.create("bad name!", null, null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.create("a".repeat(65), null, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void 등록은_브로커에_있고_메타데이터가_없을_때만() {
        scramUsers(Map.of("order-api", mock(UserScramCredentialsDescription.class)));
        when(repository.existsByName("order-api")).thenReturn(false);
        service.register("order-api", "dev1", "기존 계정");
        verify(repository).save(any(KafkaApp.class));
        verify(admin, never()).alterUserScramCredentials(anyList());

        when(repository.existsByName("order-api")).thenReturn(true);
        assertThatThrownBy(() -> service.register("order-api", null, null))
                .isInstanceOf(KafkaAppExistsException.class);

        scramUsers(Map.of());
        when(repository.existsByName("ghost")).thenReturn(false);
        assertThatThrownBy(() -> service.register("ghost", null, null))
                .isInstanceOf(KafkaAppNotFoundException.class);
    }

    @Test
    void 미등록_앱의_재발급_삭제_권한변경은_404() {
        when(repository.findByName("ghost")).thenReturn(Optional.empty());
        assertThatThrownBy(() -> service.resetPassword("ghost")).isInstanceOf(KafkaAppNotFoundException.class);
        assertThatThrownBy(() -> service.delete("ghost")).isInstanceOf(KafkaAppNotFoundException.class);
        assertThatThrownBy(() -> service.setTopicPermission("ghost", "orders", PermissionMode.PRODUCE))
                .isInstanceOf(KafkaAppNotFoundException.class);
        assertThatThrownBy(() -> service.revokeTopicPermission("ghost", "orders"))
                .isInstanceOf(KafkaAppNotFoundException.class);
        verify(admin, never()).createAcls(anyCollection());
        verify(admin, never()).deleteAcls(anyCollection());
    }

    @Test
    void 삭제는_ACL_전부_제거_후_SCRAM_삭제_후_메타데이터_삭제() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        service.delete("order-api");
        var order = inOrder(admin, repository);
        order.verify(admin).deleteAcls(List.of(AclMapping.principalFilter("order-api")));
        order.verify(admin).alterUserScramCredentials(argThat(l ->
                l.get(0) instanceof UserScramCredentialDeletion d && d.user().equals("order-api")));
        order.verify(repository).deleteByName("order-api");
    }

    @Test
    void consume_부여는_토픽_ACL을_갈아끼우고_그룹_ACL을_만든다() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        service.setTopicPermission("order-api", "orders", PermissionMode.CONSUME);
        var order = inOrder(admin);
        order.verify(admin).deleteAcls(List.of(AclMapping.topicFilter("order-api", "orders")));
        order.verify(admin).createAcls(AclMapping.topicBindings("order-api", "orders", PermissionMode.CONSUME));
        order.verify(admin).createAcls(List.of(AclMapping.groupBinding("order-api")));
        // consume 을 새로 주는 경로는 방금 쓴 ACL 을 되짚어 읽는 describeAcls 에 기대지 않고 그룹 바인딩을 직접 보장한다
        // (전파 지연으로 그 읽기가 아직 반영 전이면 hasConsume() 이 false 로 보여 그룹 ACL 을 잘못 지울 수 있기 때문).
        verify(admin, never()).describeAcls(any());
        verify(admin, never()).deleteAcls(List.of(AclMapping.groupFilter("order-api")));
    }

    @Test
    void 마지막_consume을_produce로_바꾸면_그룹_ACL을_지운다() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        acls(AclMapping.topicBindings("order-api", "orders", PermissionMode.PRODUCE));
        service.setTopicPermission("order-api", "orders", PermissionMode.PRODUCE);
        verify(admin).deleteAcls(List.of(AclMapping.groupFilter("order-api")));
        verify(admin, never()).createAcls(List.of(AclMapping.groupBinding("order-api")));
    }

    @Test
    void 회수_후_다른_consume이_남아있으면_그룹_ACL을_유지한다() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        acls(AclMapping.topicBindings("order-api", "events", PermissionMode.CONSUME));
        service.revokeTopicPermission("order-api", "orders");
        verify(admin).deleteAcls(List.of(AclMapping.topicFilter("order-api", "orders")));
        verify(admin).createAcls(List.of(AclMapping.groupBinding("order-api")));
        verify(admin, never()).deleteAcls(List.of(AclMapping.groupFilter("order-api")));
    }

    @Test
    void 없는_토픽에_권한을_주면_UnknownTopic() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        DescribeTopicsResult topics = mock(DescribeTopicsResult.class);
        org.apache.kafka.common.internals.KafkaFutureImpl<Map<String, TopicDescription>> f =
                new org.apache.kafka.common.internals.KafkaFutureImpl<>();
        f.completeExceptionally(new org.apache.kafka.common.errors.UnknownTopicOrPartitionException("no"));
        when(topics.allTopicNames()).thenReturn(f);
        when(admin.describeTopics(anyCollection())).thenReturn(topics);
        assertThatThrownBy(() -> service.setTopicPermission("order-api", "ghost", PermissionMode.PRODUCE))
                .isInstanceOf(org.apache.kafka.common.errors.UnknownTopicOrPartitionException.class);
        verify(admin, never()).createAcls(anyCollection());
    }

    @Test
    void 토픽명이_비어있으면_전체_ACL_삭제로_번지지_않도록_거부한다() {
        when(repository.findByName("order-api")).thenReturn(Optional.of(app));
        assertThatThrownBy(() -> service.revokeTopicPermission("order-api", null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> service.setTopicPermission("order-api", " ", PermissionMode.PRODUCE))
                .isInstanceOf(IllegalArgumentException.class);
        verify(admin, never()).deleteAcls(anyCollection());
        verify(admin, never()).createAcls(anyCollection());
    }
}
