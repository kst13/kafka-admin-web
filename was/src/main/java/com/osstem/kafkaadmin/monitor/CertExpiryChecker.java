package com.osstem.kafkaadmin.monitor;

import com.osstem.kafkaadmin.config.KafkaConnectionProperties;
import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

// 브로커 TLS 인증서 만료 감시. SASL_SSL 리스너는 TLS 핸드셰이크가 SASL 인증보다
// 먼저 완료되므로, 계정 없이도 서버 인증서를 읽을 수 있다.
@Service
public class CertExpiryChecker {

    private static final Logger log = LoggerFactory.getLogger(CertExpiryChecker.class);

    private final KafkaConnectionProperties kafkaProps;
    private final ClusterQueryService cluster;
    private final AlertEvaluator evaluator;
    private final MonitorProperties props;
    private final AtomicReference<List<CertStatus>> last = new AtomicReference<>(List.of());

    public record CertStatus(String broker, Instant notAfter, long daysRemaining) {}

    public CertExpiryChecker(KafkaConnectionProperties kafkaProps, ClusterQueryService cluster,
                             AlertEvaluator evaluator, MonitorProperties props) {
        this.kafkaProps = kafkaProps;
        this.cluster = cluster;
        this.evaluator = evaluator;
        this.props = props;
    }

    public void runDailyCheck() {
        if (!"SASL_SSL".equals(kafkaProps.securityProtocol())) {
            return; // PLAINTEXT 구성에는 인증서가 없다
        }
        List<CertStatus> statuses = new ArrayList<>();
        for (BrokerInfo b : cluster.getClusterInfo().brokers()) {
            String key = b.host() + ":" + b.port();
            try {
                X509Certificate cert = fetchServerCert(b.host(), b.port());
                long days = daysUntil(cert.getNotAfter().toInstant(), Instant.now());
                statuses.add(new CertStatus(key, cert.getNotAfter().toInstant(), days));
                if (days <= props.certWarnDays()) {
                    evaluator.raise("CERT_EXPIRY", key,
                            "브로커 %s 인증서 만료 D-%d".formatted(key, days),
                            days, props.certWarnDays());
                }
            } catch (Exception e) {
                log.warn("인증서 조회 실패 {}: {}", key, e.getMessage());
            }
        }
        last.set(List.copyOf(statuses));
    }

    public List<CertStatus> lastStatuses() { return last.get(); }

    public static long daysUntil(Instant notAfter, Instant now) {
        return ChronoUnit.DAYS.between(now, notAfter);
    }

    // 만료일 조회 전용이라 신뢰 검증을 생략한다(데이터 교환 없음, 인증서 메타데이터만 읽음).
    // 실제 Kafka 통신은 AdminClient가 truststore로 정상 검증한다.
    static X509Certificate fetchServerCert(String host, int port) throws Exception {
        TrustManager trustAll = new X509TrustManager() {
            public void checkClientTrusted(X509Certificate[] chain, String authType) {}
            public void checkServerTrusted(X509Certificate[] chain, String authType) {}
            public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        };
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, new TrustManager[]{trustAll}, null);
        // connect에도 5초 제한: createSocket(host, port)는 접속 타임아웃이 없어
        // 방화벽에 막힌 브로커에서 OS SYN 재시도 시간만큼(수십 초) 블록될 수 있다.
        try (SSLSocket socket = (SSLSocket) ctx.getSocketFactory().createSocket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            socket.setSoTimeout(5000);
            socket.startHandshake();
            return (X509Certificate) socket.getSession().getPeerCertificates()[0];
        }
    }
}
