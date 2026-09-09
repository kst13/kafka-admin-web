package com.osstem.kafkaadmin.metrics;

public class BrokerNotFoundException extends RuntimeException {
    public BrokerNotFoundException(int id) { super("존재하지 않는 브로커입니다: " + id); }
}
