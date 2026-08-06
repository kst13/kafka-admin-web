package com.osstem.kafkaadmin.config;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaConnectionPropertiesTest {

    @Test
    void plaintext_구성은_보안설정을_포함하지_않는다() {
        var props = new KafkaConnectionProperties("localhost:9092", "PLAINTEXT", "", "", "");
        Map<String, Object> p = props.toClientProps();
        assertThat(p).containsEntry("bootstrap.servers", "localhost:9092")
                .containsEntry("request.timeout.ms", 5000)
                .containsEntry("default.api.timeout.ms", 5000)
                .doesNotContainKeys("sasl.jaas.config", "ssl.truststore.location");
    }

    @Test
    void sasl_ssl_구성은_인증과_truststore_설정을_포함한다() {
        var props = new KafkaConnectionProperties("10.0.0.11:9094", "SASL_SSL",
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"pw\";",
                "/secrets/truststore.jks", "tspw");
        Map<String, Object> p = props.toClientProps();
        assertThat(p).containsEntry("security.protocol", "SASL_SSL")
                .containsEntry("sasl.mechanism", "SCRAM-SHA-512")
                .containsEntry("ssl.truststore.location", "/secrets/truststore.jks")
                .containsEntry("ssl.truststore.password", "tspw")
                .containsKey("sasl.jaas.config");
    }
}
