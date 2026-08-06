package com.osstem.kafkaadmin.kafka;

import org.apache.kafka.common.KafkaFuture;
import java.util.concurrent.ExecutionException;

public final class KafkaFutures {
    private KafkaFutures() {}

    public static <T> T await(KafkaFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaUnavailableException(e);
        } catch (ExecutionException e) {
            throw new KafkaUnavailableException(e.getCause());
        }
    }
}
