/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.concurrent.pekko.ScalaFutureUtils;

import com.typesafe.config.Config;
import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link CustomSSLEngineProvider} verifying that configured TLS protocols are
 * actually applied to the produced {@link SSLEngine} instances.
 *
 * <p>Beyond checking {@link SSLEngine#getEnabledProtocols()}, the handshake tests perform a full
 * in-memory TLS handshake and assert the negotiated protocol via {@link
 * javax.net.ssl.SSLSession#getProtocol()}.
 */
class CustomSSLEngineProviderIT {

    private static final String KEYSTORE_PATH =
            CustomSSLEngineProviderIT.class.getResource("/local127.keystore").getPath();
    private static final String TRUSTSTORE_PATH =
            CustomSSLEngineProviderIT.class.getResource("/local127.truststore").getPath();
    private static final String STORE_PASSWORD = "password";
    private static final String KEY_PASSWORD = "password";

    /**
     * Include cipher suites for both TLSv1.2 and TLSv1.3 so the handshake tests work regardless of
     * which protocol version is being exercised. Flink's default only contains TLSv1.2 suites;
     * without TLSv1.3 suites, Java rejects TLSv1.3 at {@code beginHandshake()} with "No appropriate
     * protocol".
     */
    private static final String TEST_ALGORITHMS =
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,"
                    + "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,"
                    + "TLS_AES_128_GCM_SHA256,"
                    + "TLS_AES_256_GCM_SHA384";

    // -------------------------------------------------------------------------
    // Enabled-protocol assertions (configuration level)
    // -------------------------------------------------------------------------

    @Test
    void serverEngineHasConfiguredProtocolsEnabled()
            throws ExecutionException, InterruptedException {
        final ActorSystem actorSystem = createActorSystemWithProtocols("TLSv1.2,TLSv1.3");
        try {
            final CustomSSLEngineProvider provider = new CustomSSLEngineProvider(actorSystem);
            assertThat(Arrays.asList(provider.createServerSSLEngine().getEnabledProtocols()))
                    .containsExactlyInAnyOrder("TLSv1.2", "TLSv1.3");
        } finally {
            shutdownActorSystem(actorSystem);
        }
    }

    @Test
    void clientEngineHasConfiguredProtocolsEnabled()
            throws ExecutionException, InterruptedException {
        final ActorSystem actorSystem = createActorSystemWithProtocols("TLSv1.2,TLSv1.3");
        try {
            final CustomSSLEngineProvider provider = new CustomSSLEngineProvider(actorSystem);
            assertThat(Arrays.asList(provider.createClientSSLEngine().getEnabledProtocols()))
                    .containsExactlyInAnyOrder("TLSv1.2", "TLSv1.3");
        } finally {
            shutdownActorSystem(actorSystem);
        }
    }

    @Test
    void singleProtocolIsAppliedToEngine() throws ExecutionException, InterruptedException {
        final ActorSystem actorSystem = createActorSystemWithProtocols("TLSv1.3");
        try {
            final CustomSSLEngineProvider provider = new CustomSSLEngineProvider(actorSystem);
            assertThat(provider.createServerSSLEngine().getEnabledProtocols())
                    .containsExactly("TLSv1.3");
            assertThat(provider.createClientSSLEngine().getEnabledProtocols())
                    .containsExactly("TLSv1.3");
        } finally {
            shutdownActorSystem(actorSystem);
        }
    }

    // -------------------------------------------------------------------------
    // Handshake-level verification (socket level)
    // -------------------------------------------------------------------------

    /**
     * Performs a full in-memory TLS handshake between a server and a client engine both restricted
     * to the given protocol, then asserts that {@link javax.net.ssl.SSLSession#getProtocol()}
     * reports exactly that protocol. This is the definitive proof that the restriction works at the
     * TLS-record layer, not just in the engine's configuration.
     */
    @ParameterizedTest
    @ValueSource(strings = {"TLSv1.2", "TLSv1.3"})
    void handshakeNegotiatesExactlyTheConfiguredProtocol(String protocol) throws Exception {
        final ActorSystem actorSystem = createActorSystemWithProtocols(protocol);
        try {
            final CustomSSLEngineProvider provider = new CustomSSLEngineProvider(actorSystem);
            final SSLEngine server = provider.createServerSSLEngine();
            final SSLEngine client = provider.createClientSSLEngine();

            runHandshake(server, client);

            assertThat(server.getSession().getProtocol()).isEqualTo(protocol);
            assertThat(client.getSession().getProtocol()).isEqualTo(protocol);
        } finally {
            shutdownActorSystem(actorSystem);
        }
    }

    /**
     * When both TLSv1.2 and TLSv1.3 are enabled, the two peers should negotiate TLSv1.3 (the
     * highest mutually supported version), as mandated by the TLS specification.
     */
    @Test
    void handshakeNegotiatesHighestMutualProtocolWhenMultipleAreEnabled() throws Exception {
        final ActorSystem actorSystem = createActorSystemWithProtocols("TLSv1.2,TLSv1.3");
        try {
            final CustomSSLEngineProvider provider = new CustomSSLEngineProvider(actorSystem);
            final SSLEngine server = provider.createServerSSLEngine();
            final SSLEngine client = provider.createClientSSLEngine();

            runHandshake(server, client);

            assertThat(server.getSession().getProtocol()).isEqualTo("TLSv1.3");
            assertThat(client.getSession().getProtocol()).isEqualTo("TLSv1.3");
        } finally {
            shutdownActorSystem(actorSystem);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /**
     * Drives an in-memory TLS handshake between {@code server} and {@code client} using only {@link
     * ByteBuffer}s — no actual network socket is involved.
     *
     * <p>The loop advances the state machine one atomic step per iteration and uses {@code
     * compact}/{@code flip} so that multi-record TLS flights (ServerHello + Certificate +
     * ServerHelloDone, etc.) are handled correctly for both TLSv1.2 and TLSv1.3.
     */
    private static void runHandshake(SSLEngine server, SSLEngine client) throws Exception {
        // Generous buffers to hold a full TLS flight.
        final ByteBuffer cToS = ByteBuffer.allocate(32768); // client → server network data
        final ByteBuffer sToC = ByteBuffer.allocate(32768); // server → client network data
        final ByteBuffer app = ByteBuffer.allocate(32768);
        final ByteBuffer empty = ByteBuffer.allocate(0);

        // Start with empty network buffers in read position.
        cToS.limit(0);
        sToC.limit(0);

        client.setUseClientMode(true);
        server.setUseClientMode(false);
        client.beginHandshake();
        server.beginHandshake();

        for (int step = 0; step < 500; step++) {
            runAllTasks(client);
            runAllTasks(server);

            final HandshakeStatus cs = client.getHandshakeStatus();
            final HandshakeStatus ss = server.getHandshakeStatus();

            if (isDone(cs) && isDone(ss)) {
                return;
            }

            // Each branch does exactly one operation then loops back to re-read status.
            if (cs == HandshakeStatus.NEED_WRAP) {
                cToS.compact();
                client.wrap(empty, cToS);
                cToS.flip();
            } else if (ss == HandshakeStatus.NEED_UNWRAP && cToS.hasRemaining()) {
                app.clear();
                server.unwrap(cToS, app);
                runAllTasks(server);
            } else if (ss == HandshakeStatus.NEED_WRAP) {
                sToC.compact();
                server.wrap(empty, sToC);
                sToC.flip();
            } else if (cs == HandshakeStatus.NEED_UNWRAP && sToC.hasRemaining()) {
                app.clear();
                client.unwrap(sToC, app);
                runAllTasks(client);
            } else {
                break; // no progress possible
            }
        }

        throw new IllegalStateException(
                "TLS handshake did not complete. "
                        + "clientStatus="
                        + client.getHandshakeStatus()
                        + ", serverStatus="
                        + server.getHandshakeStatus());
    }

    private static boolean isDone(HandshakeStatus status) {
        return status == HandshakeStatus.FINISHED || status == HandshakeStatus.NOT_HANDSHAKING;
    }

    private static void runAllTasks(SSLEngine engine) {
        Runnable task;
        while ((task = engine.getDelegatedTask()) != null) {
            task.run();
        }
    }

    // -------------------------------------------------------------------------
    // openssl s_client verification (real TCP socket)
    // -------------------------------------------------------------------------

    /**
     * Starts a real {@link SSLServerSocket} that accepts both TLSv1.2 and TLSv1.3, then connects to
     * it with {@code openssl s_client} forcing each version in turn. Asserts the {@code Protocol}
     * line in openssl's output matches the forced version, proving the server honours both
     * protocols over an actual TCP connection.
     */
    @ParameterizedTest
    @ValueSource(strings = {"-tls1_2", "-tls1_3"})
    void opensslHandshakesWithEachConfiguredProtocol(String opensslVersionFlag) throws Exception {
        final File pemFile = exportServerCertToPem();
        final SSLContext ctx = buildTestSSLContext();

        try (SSLServerSocket serverSocket =
                (SSLServerSocket) ctx.getServerSocketFactory().createServerSocket(0)) {
            serverSocket.setEnabledProtocols(new String[] {"TLSv1.2", "TLSv1.3"});
            serverSocket.setNeedClientAuth(false);
            final int port = serverSocket.getLocalPort();

            final AtomicReference<String> serverProtocol = new AtomicReference<>();
            final CompletableFuture<Void> serverDone =
                    CompletableFuture.runAsync(
                            () -> {
                                try (SSLSocket client = (SSLSocket) serverSocket.accept()) {
                                    client.startHandshake();
                                    serverProtocol.set(client.getSession().getProtocol());
                                    Thread.sleep(
                                            300); // keep socket open until openssl has read the
                                    // response
                                } catch (Exception ignored) {
                                }
                            });

            final Process process =
                    new ProcessBuilder(
                                    "openssl",
                                    "s_client",
                                    "-connect",
                                    "127.0.0.1:" + port,
                                    opensslVersionFlag,
                                    "-CAfile",
                                    pemFile.getAbsolutePath())
                            .redirectErrorStream(true)
                            .start();
            process.getOutputStream().close(); // send EOF so openssl exits after handshake
            final String output = new String(process.getInputStream().readAllBytes());
            process.waitFor(10, TimeUnit.SECONDS);
            serverDone.get(10, TimeUnit.SECONDS);

            final String expectedProtocol =
                    "-tls1_2".equals(opensslVersionFlag) ? "TLSv1.2" : "TLSv1.3";
            // The SSL-Session block that contains "Protocol  :" is only printed when the server
            // sends a session ticket, which may not happen before the socket closes.  The summary
            // line "New, TLSv1.x, Cipher is ..." is unconditionally emitted after every successful
            // handshake and is the most reliable signal across all OpenSSL versions.
            assertThat(output)
                    .as("openssl output should report negotiated protocol")
                    .containsPattern("New, " + expectedProtocol + ", Cipher is");
            assertThat(serverProtocol.get())
                    .as("server-side SSLSession should report the same protocol")
                    .isEqualTo(expectedProtocol);
        }
    }

    /**
     * When the server is restricted to TLSv1.3 only, a client that forces TLSv1.2 must be rejected.
     * Verifies that the protocol restriction is actually enforced on the wire.
     */
    @Test
    void opensslTls12ClientIsRejectedWhenServerAllowsOnlyTls13() throws Exception {
        final File pemFile = exportServerCertToPem();
        final SSLContext ctx = buildTestSSLContext();

        try (SSLServerSocket serverSocket =
                (SSLServerSocket) ctx.getServerSocketFactory().createServerSocket(0)) {
            serverSocket.setEnabledProtocols(new String[] {"TLSv1.3"});
            serverSocket.setNeedClientAuth(false);
            final int port = serverSocket.getLocalPort();

            CompletableFuture.runAsync(
                    () -> {
                        try (SSLSocket client = (SSLSocket) serverSocket.accept()) {
                            client.startHandshake(); // expected to throw on the server side
                        } catch (Exception ignored) {
                        }
                    });

            final Process process =
                    new ProcessBuilder(
                                    "openssl",
                                    "s_client",
                                    "-connect",
                                    "127.0.0.1:" + port,
                                    "-tls1_2",
                                    "-CAfile",
                                    pemFile.getAbsolutePath())
                            .redirectErrorStream(true)
                            .start();
            process.getOutputStream().close();
            final String output = new String(process.getInputStream().readAllBytes());
            process.waitFor(10, TimeUnit.SECONDS);

            // The server sent a protocol_version alert (alert 70).
            // OpenSSL 3.x surfaces this as "tlsv1 alert protocol version" in the error line and
            // leaves "Cipher is (NONE)" in the summary — both indicate a failed handshake.
            assertThat(output)
                    .containsAnyOf(
                            "handshake failure",
                            "alert handshake failure",
                            "no protocols available",
                            "alert protocol version",
                            "Cipher is (NONE)");
        }
    }

    // -------------------------------------------------------------------------
    // openssl / real-socket helpers
    // -------------------------------------------------------------------------

    /** Exports the server certificate from the test keystore to a temporary PEM file. */
    private static File exportServerCertToPem() throws Exception {
        final KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = new FileInputStream(KEYSTORE_PATH)) {
            ks.load(in, STORE_PASSWORD.toCharArray());
        }
        final Certificate cert = ks.getCertificate(ks.aliases().nextElement());
        final String pem =
                "-----BEGIN CERTIFICATE-----\n"
                        + Base64.getMimeEncoder(64, new byte[] {'\n'})
                                .encodeToString(cert.getEncoded())
                        + "\n-----END CERTIFICATE-----\n";
        final File pemFile = File.createTempFile("flink-test-cert-", ".pem");
        pemFile.deleteOnExit();
        Files.writeString(pemFile.toPath(), pem);
        return pemFile;
    }

    /**
     * Builds an {@link SSLContext} backed by the same keystore and truststore that {@link
     * CustomSSLEngineProvider} uses at runtime, for use with real sockets.
     */
    private static SSLContext buildTestSSLContext() throws Exception {
        final KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = new FileInputStream(KEYSTORE_PATH)) {
            keyStore.load(in, STORE_PASSWORD.toCharArray());
        }
        final KeyManagerFactory kmf =
                KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, KEY_PASSWORD.toCharArray());

        final KeyStore trustStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = new FileInputStream(TRUSTSTORE_PATH)) {
            trustStore.load(in, STORE_PASSWORD.toCharArray());
        }
        final TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);

        final SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        return ctx;
    }

    private static ActorSystem createActorSystemWithProtocols(String protocols) {
        final Configuration configuration = new Configuration();
        configuration.set(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        configuration.set(SecurityOptions.SSL_PROTOCOL, protocols);
        configuration.set(SecurityOptions.SSL_ALGORITHMS, TEST_ALGORITHMS);
        configuration.set(SecurityOptions.SSL_INTERNAL_KEYSTORE, KEYSTORE_PATH);
        configuration.set(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, STORE_PASSWORD);
        configuration.set(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, KEY_PASSWORD);
        configuration.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE, TRUSTSTORE_PATH);
        configuration.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, STORE_PASSWORD);
        configuration.set(SecurityOptions.SSL_INTERNAL_KEYSTORE_TYPE, "PKCS12");
        configuration.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_TYPE, "PKCS12");

        final Config config = PekkoUtils.getConfig(configuration, new HostAndPort("localhost", 0));
        return PekkoUtils.createActorSystem(PekkoUtils.getFlinkActorSystemName(), config);
    }

    private static void shutdownActorSystem(ActorSystem actorSystem)
            throws ExecutionException, InterruptedException {
        actorSystem.terminate();
        ScalaFutureUtils.toJava(actorSystem.whenTerminated()).get();
    }
}
