package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class OpsFuturesTest {

    private static <T> KafkaFutureImpl<T> failed(Throwable t) {
        KafkaFutureImpl<T> f = new KafkaFutureImpl<>();
        f.completeExceptionally(t);
        return f;
    }

    @Test
    void 클러스터_인가_거부는_그대로_던진다() {
        assertThatThrownBy(() -> OpsFutures.await(failed(new ClusterAuthorizationException("denied"))))
                .isInstanceOf(ClusterAuthorizationException.class);
    }

    @Test
    void 리소스_없음은_그대로_던진다() {
        assertThatThrownBy(() -> OpsFutures.await(failed(new ResourceNotFoundException("no user"))))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    @Test
    void 타임아웃은_접속불가로_감싼다() {
        assertThatThrownBy(() -> OpsFutures.await(failed(new TimeoutException("slow"))))
                .isInstanceOf(KafkaUnavailableException.class);
    }
}
