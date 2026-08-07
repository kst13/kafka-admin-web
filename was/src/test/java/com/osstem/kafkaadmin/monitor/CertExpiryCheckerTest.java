package com.osstem.kafkaadmin.monitor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import static org.assertj.core.api.Assertions.assertThat;

class CertExpiryCheckerTest {

    @Test
    void 만료까지_남은_일수를_계산한다() {
        Instant now = Instant.parse("2026-08-07T00:00:00Z");
        assertThat(CertExpiryChecker.daysUntil(now.plus(30, ChronoUnit.DAYS), now)).isEqualTo(30);
        assertThat(CertExpiryChecker.daysUntil(now.plus(30, ChronoUnit.DAYS).plusSeconds(3600), now))
                .isEqualTo(30);
        assertThat(CertExpiryChecker.daysUntil(now.minus(1, ChronoUnit.DAYS), now)).isEqualTo(-1);
    }

    @Test
    void TLS_서버의_인증서_만료일을_읽는다(@TempDir Path dir) throws Exception {
        // keytool로 90일짜리 자가서명 키스토어 생성 (PATH가 아닌 실행 중 JDK의 keytool 사용)
        String keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
        Path ks = dir.resolve("server.jks");
        Process p = new ProcessBuilder(keytool, "-genkeypair", "-alias", "server",
                "-keyalg", "RSA", "-keysize", "2048", "-validity", "90",
                "-dname", "CN=localhost", "-keystore", ks.toString(),
                "-storepass", "changeit", "-keypass", "changeit").inheritIO().start();
        assertThat(p.waitFor()).isZero();

        KeyStore keyStore = KeyStore.getInstance("JKS");
        try (var in = new FileInputStream(ks.toFile())) {
            keyStore.load(in, "changeit".toCharArray());
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, "changeit".toCharArray());
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), null, null);

        try (SSLServerSocket server =
                     (SSLServerSocket) ctx.getServerSocketFactory().createServerSocket(0)) {
            Thread accepter = new Thread(() -> {
                try (var s = server.accept()) {
                    s.getInputStream().read(); // 핸드셰이크 완료까지 대기
                } catch (Exception ignored) {
                }
            });
            accepter.start();

            X509Certificate cert =
                    CertExpiryChecker.fetchServerCert("localhost", server.getLocalPort());
            long days = CertExpiryChecker.daysUntil(cert.getNotAfter().toInstant(), Instant.now());
            assertThat(days).isBetween(85L, 90L);
        }
    }
}
