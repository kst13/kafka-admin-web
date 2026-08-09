package com.osstem.kafkaadmin.ops;

import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import java.util.concurrent.ExecutionException;

// 조치용 await: 409/404/400 으로 구분해야 하는 예외는 그대로 통과시키고,
// 나머지(타임아웃·접속 실패 등)는 기존과 같이 KafkaUnavailableException(→503)으로 감싼다.
// TimeoutException 도 ApiException 하위라 "ApiException 전부 통과"로 하면 안 된다.
public final class OpsFutures {
    private OpsFutures() {}

    public static <T> T await(KafkaFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUnavailableException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TopicExistsException
                    || cause instanceof UnknownTopicOrPartitionException
                    || cause instanceof InvalidPartitionsException
                    || cause instanceof InvalidReplicationFactorException
                    || cause instanceof InvalidConfigurationException
                    || cause instanceof InvalidTopicException
                    || cause instanceof PolicyViolationException) {
                throw (RuntimeException) cause;
            }
            throw new KafkaUnavailableException(cause);
        }
    }
}
