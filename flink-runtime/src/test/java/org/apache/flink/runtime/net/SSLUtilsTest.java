/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.net;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.io.network.netty.SSLHandlerFactory;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.MultiThreadIoEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioIoHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.ClientAuth;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.JdkSslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.OpenSsl;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;

import java.io.File;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslProvider.JDK;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link SSLUtils}. */
public class SSLUtilsTest {

    private static final String TRUST_STORE_PATH =
            checkNotNull(SSLUtilsTest.class.getResource("/local127.truststore")).getFile();
    private static final String KEY_STORE_PATH =
            checkNotNull(SSLUtilsTest.class.getResource("/local127.keystore")).getFile();

    private static final String TRUST_STORE_PASSWORD = "password";
    private static final String KEY_STORE_PASSWORD = "password";
    private static final String KEY_PASSWORD = "password";

    public static final List<String> AVAILABLE_SSL_PROVIDERS;

    static {
        if (System.getProperty("flink.tests.with-openssl") != null) {
            assertThat(OpenSsl.isAvailable()).isTrue();
            AVAILABLE_SSL_PROVIDERS = Arrays.asList("JDK", "OPENSSL");
        } else {
            AVAILABLE_SSL_PROVIDERS = Collections.singletonList("JDK");
        }
    }

    private static List<String> parameters() {
        return AVAILABLE_SSL_PROVIDERS;
    }

    @Test
    void testSocketFactoriesWhenSslDisabled() {
        Configuration config = new Configuration();

        assertThatThrownBy(() -> SSLUtils.createSSLServerSocketFactory(config))
                .isInstanceOf(IllegalConfigurationException.class);

        assertThatThrownBy(() -> SSLUtils.createSSLClientSocketFactory(config))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    // ------------------------ REST client --------------------------

    /** Tests if REST Client SSL is created given a valid SSL configuration. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTClientSSL(String sslProvider) throws Exception {
        Configuration clientConfig = createRestSslConfigWithTrustStore(sslProvider);

        SSLHandlerFactory ssl = SSLUtils.createRestClientSSLEngineFactory(clientConfig);
        assertThat(ssl).isNotNull();
    }

    /** Tests that REST Client SSL Client is not created if SSL is not configured. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTClientSSLDisabled(String sslProvider) {
        Configuration clientConfig = createRestSslConfigWithTrustStore(sslProvider);
        clientConfig.set(SecurityOptions.SSL_REST_ENABLED, false);

        assertThatThrownBy(() -> SSLUtils.createRestClientSSLEngineFactory(clientConfig))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    /** Tests that REST Client SSL creation fails with bad SSL configuration. */
    @Test
    void testRESTClientSSLMissingTrustStore() {
        Configuration config = new Configuration();
        config.set(SecurityOptions.SSL_REST_ENABLED, true);
        config.set(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "some password");

        assertThatThrownBy(() -> SSLUtils.createRestClientSSLEngineFactory(config))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    /** Tests that REST Client SSL creation fails with bad SSL configuration. */
    @Test
    void testRESTClientSSLMissingPassword() {
        Configuration config = new Configuration();
        config.set(SecurityOptions.SSL_REST_ENABLED, true);
        config.set(SecurityOptions.SSL_REST_TRUSTSTORE, TRUST_STORE_PATH);

        assertThatThrownBy(() -> SSLUtils.createRestClientSSLEngineFactory(config))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    /** Tests that REST Client SSL creation fails with bad SSL configuration. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTClientSSLWrongPassword(String sslProvider) {
        Configuration clientConfig = createRestSslConfigWithTrustStore(sslProvider);
        clientConfig.set(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "badpassword");

        assertThatThrownBy(() -> SSLUtils.createRestClientSSLEngineFactory(clientConfig))
                .isInstanceOf(Exception.class);
    }

    /** Tests that REST Client SSL Engine creation fails with bad SSL configuration. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTClientSSLBadTruststoreType(String sslProvider) {
        Configuration clientConfig = createRestSslConfigWithTrustStore(sslProvider);
        clientConfig.set(SecurityOptions.SSL_REST_TRUSTSTORE_TYPE, "bad-truststore-type");

        assertThatThrownBy(() -> SSLUtils.createRestClientSSLEngineFactory(clientConfig))
                .isInstanceOf(KeyStoreException.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTSSLConfigCipherAlgorithms(String sslProvider) throws Exception {
        String testSSLAlgorithms = "test_algorithm1,test_algorithm2";
        Configuration config = createRestSslConfigWithTrustStore(sslProvider);
        config.set(SecurityOptions.SSL_REST_ENABLED, true);
        config.setString(SecurityOptions.SSL_ALGORITHMS.key(), testSSLAlgorithms);
        JdkSslContext nettySSLContext =
                (JdkSslContext)
                        SSLUtils.createRestNettySSLContext(config, true, ClientAuth.NONE, JDK);
        List<String> cipherSuites = checkNotNull(nettySSLContext).cipherSuites();
        assertThat(cipherSuites).hasSize(2);
        assertThat(cipherSuites).containsExactlyInAnyOrder(testSSLAlgorithms.split(","));
    }

    // ------------------------ server --------------------------

    /** Tests that REST Server SSL Engine is created given a valid SSL configuration. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTServerSSL(String sslProvider) throws Exception {
        Configuration serverConfig = createRestSslConfigWithKeyStore(sslProvider);

        SSLHandlerFactory ssl = SSLUtils.createRestServerSSLEngineFactory(serverConfig);
        assertThat(ssl).isNotNull();
    }

    /** Tests that REST Server SSL Engine is not created if SSL is disabled. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTServerSSLDisabled(String sslProvider) {
        Configuration serverConfig = createRestSslConfigWithKeyStore(sslProvider);
        serverConfig.set(SecurityOptions.SSL_REST_ENABLED, false);

        assertThatThrownBy(() -> SSLUtils.createRestServerSSLEngineFactory(serverConfig))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    /** Tests that REST Server SSL Engine creation fails with bad SSL configuration. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTServerSSLBadKeystorePassword(String sslProvider) {
        Configuration serverConfig = createRestSslConfigWithKeyStore(sslProvider);
        serverConfig.set(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, "badpassword");

        assertThatThrownBy(() -> SSLUtils.createRestServerSSLEngineFactory(serverConfig))
                .isInstanceOf(Exception.class);
    }

    /** Tests that REST Server SSL Engine creation fails with bad SSL configuration. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTServerSSLBadKeyPassword(String sslProvider) {
        Configuration serverConfig = createRestSslConfigWithKeyStore(sslProvider);
        serverConfig.set(SecurityOptions.SSL_REST_KEY_PASSWORD, "badpassword");

        assertThatThrownBy(() -> SSLUtils.createRestServerSSLEngineFactory(serverConfig))
                .isInstanceOf(Exception.class);
    }

    /** Tests that REST Server SSL Engine creation fails with bad SSL configuration. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTServerSSLBadKeystoreType(String sslProvider) {
        Configuration serverConfig = createRestSslConfigWithKeyStore(sslProvider);
        serverConfig.set(SecurityOptions.SSL_REST_KEYSTORE_TYPE, "bad-keystore-type");

        assertThatThrownBy(() -> SSLUtils.createRestServerSSLEngineFactory(serverConfig))
                .isInstanceOf(KeyStoreException.class);
    }

    // ----------------------- mutual auth contexts --------------------------

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSL(String sslProvider) throws Exception {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        assertThat(SSLUtils.createInternalServerSSLEngineFactory(config)).isNotNull();
        assertThat(SSLUtils.createInternalClientSSLEngineFactory(config)).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLWithSSLPinning(String sslProvider) throws Exception {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(
                SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT,
                getCertificateFingerprint(config, "flink.test"));

        assertThat(SSLUtils.createInternalServerSSLEngineFactory(config)).isNotNull();
        assertThat(SSLUtils.createInternalClientSSLEngineFactory(config)).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLDisables(String sslProvider) {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(SecurityOptions.SSL_INTERNAL_ENABLED, false);

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(Exception.class);

        assertThatThrownBy(() -> SSLUtils.createInternalClientSSLEngineFactory(config))
                .isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLKeyStoreOnly(String sslProvider) {
        final Configuration config = createInternalSslConfigWithKeyStore(sslProvider);

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(Exception.class);

        assertThatThrownBy(() -> SSLUtils.createInternalClientSSLEngineFactory(config))
                .isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLTrustStoreOnly(String sslProvider) {
        final Configuration config = createInternalSslConfigWithTrustStore(sslProvider);

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(Exception.class);

        assertThatThrownBy(() -> SSLUtils.createInternalClientSSLEngineFactory(config))
                .isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLWrongKeystorePassword(String sslProvider) {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, "badpw");

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(Exception.class);

        assertThatThrownBy(() -> SSLUtils.createInternalClientSSLEngineFactory(config))
                .isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLWrongTruststorePassword(String sslProvider) {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, "badpw");

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(Exception.class);

        assertThatThrownBy(() -> SSLUtils.createInternalClientSSLEngineFactory(config))
                .isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLWrongTruststoreType(String sslProvider) {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_TYPE, "bad-truststore-type");

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(KeyStoreException.class);

        assertThatThrownBy(() -> SSLUtils.createInternalClientSSLEngineFactory(config))
                .isInstanceOf(KeyStoreException.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLWrongKeyPassword(String sslProvider) {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, "badpw");

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(Exception.class);

        assertThatThrownBy(() -> SSLUtils.createInternalClientSSLEngineFactory(config))
                .isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLWrongKeystoreType(String sslProvider) {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(SecurityOptions.SSL_INTERNAL_KEYSTORE_TYPE, "bad-keystore-type");

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(KeyStoreException.class);

        assertThatThrownBy(() -> SSLUtils.createInternalClientSSLEngineFactory(config))
                .isInstanceOf(KeyStoreException.class);
    }

    // -------------------- protocols and cipher suites -----------------------

    /** Tests if SSLUtils set the right ssl version and cipher suites for SSLServerSocket. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testSetSSLVersionAndCipherSuitesForSSLServerSocket(String sslProvider) throws Exception {
        Configuration serverConfig = createInternalSslConfigWithKeyAndTrustStores(sslProvider);

        // set custom protocol and cipher suites
        serverConfig.set(SecurityOptions.SSL_PROTOCOL, "TLSv1.1");
        serverConfig.set(
                SecurityOptions.SSL_ALGORITHMS,
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");

        try (ServerSocket socket =
                SSLUtils.createSSLServerSocketFactory(serverConfig).createServerSocket(0)) {
            assertThat(socket).isInstanceOf(SSLServerSocket.class);
            final SSLServerSocket sslSocket = (SSLServerSocket) socket;

            String[] protocols = sslSocket.getEnabledProtocols();
            String[] algorithms = sslSocket.getEnabledCipherSuites();

            assertThat(protocols).hasSize(1);
            assertThat(protocols[0]).isEqualTo("TLSv1.1");
            assertThat(algorithms).hasSize(2);
            assertThat(algorithms)
                    .contains(
                            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
        }
    }

    /** Tests that {@link SSLHandlerFactory} is created correctly. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testCreateSSLEngineFactory(String sslProvider) throws Exception {
        Configuration serverConfig = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        final String[] sslAlgorithms;
        final String[] expectedSslProtocols;
        if (sslProvider.equalsIgnoreCase("OPENSSL")) {
            // openSSL does not support the same set of cipher algorithms!
            sslAlgorithms =
                    new String[] {
                        "TLS_RSA_WITH_AES_128_GCM_SHA256", "TLS_RSA_WITH_AES_256_GCM_SHA384"
                    };
            expectedSslProtocols = new String[] {"SSLv2Hello", "TLSv1"};
        } else {
            sslAlgorithms =
                    new String[] {
                        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256"
                    };
            expectedSslProtocols = new String[] {"TLSv1"};
        }

        // set custom protocol and cipher suites
        serverConfig.set(SecurityOptions.SSL_PROTOCOL, "TLSv1");
        serverConfig.set(SecurityOptions.SSL_ALGORITHMS, String.join(",", sslAlgorithms));

        final SSLHandlerFactory serverSSLHandlerFactory =
                SSLUtils.createInternalServerSSLEngineFactory(serverConfig);
        final SslHandler sslHandler =
                serverSSLHandlerFactory.createNettySSLHandler(UnpooledByteBufAllocator.DEFAULT);

        assertThat(sslHandler.engine().getEnabledProtocols()).hasSameSizeAs(expectedSslProtocols);
        assertThat(sslHandler.engine().getEnabledProtocols()).contains(expectedSslProtocols);

        assertThat(sslHandler.engine().getEnabledCipherSuites()).hasSameSizeAs(sslAlgorithms);
        assertThat(sslHandler.engine().getEnabledCipherSuites()).contains(sslAlgorithms);
    }

    // -------------------- hostname verification (shared certificate) -------------------
    //
    // Internal SSL uses one shared, mutually-trusted certificate across every node in the
    // cluster (see docs/content/docs/deployment/security/security-ssl.md: "Because internal
    // connections are mutually authenticated with shared certificates, Flink can skip hostname
    // verification. This makes container-based setups easier."), e.g. a Kubernetes deployment
    // with hundreds of dynamically-scheduled, differently-named pods sharing one cert. Simulate
    // that: the client connects to a real "localhost" socket but is told to verify a peer
    // identity the shared certificate was never meant to cover; negotiation must still succeed.

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLIgnoresPeerHostMismatch(String sslProvider) throws Exception {
        Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);

        SSLSession session =
                negotiate(
                        SSLUtils.createInternalServerSSLEngineFactory(config),
                        SSLUtils.createInternalClientSSLEngineFactory(config),
                        "some-taskmanager-42.flink-headless.flink.svc.cluster.local");

        assertThat(session).isNotNull();
    }

    // -------------------- multi-protocol negotiation -----------------------
    //
    // Regression coverage for SecurityOptions.SSL_PROTOCOL accepting a comma-separated protocol
    // list: every consumer below is expected to negotiate the highest protocol both sides support,
    // and to gracefully fall back rather than fail when the higher protocol has no usable cipher.

    private static final String TLS_13_CIPHERS = "TLS_AES_128_GCM_SHA256,TLS_AES_256_GCM_SHA384";

    private static String tls12Ciphers(String sslProvider) {
        // openSSL does not support the same set of cipher algorithms as the JDK provider, see
        // testCreateSSLEngineFactory above.
        return sslProvider.equalsIgnoreCase("OPENSSL")
                ? "TLS_RSA_WITH_AES_128_GCM_SHA256,TLS_RSA_WITH_AES_256_GCM_SHA384"
                : "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLNegotiatesTls13WhenBothSidesSupportIt(String sslProvider) throws Exception {
        Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(SecurityOptions.SSL_PROTOCOL, "TLSv1.2,TLSv1.3");
        config.set(
                SecurityOptions.SSL_ALGORITHMS, tls12Ciphers(sslProvider) + "," + TLS_13_CIPHERS);

        SSLSession session =
                negotiate(
                        SSLUtils.createInternalServerSSLEngineFactory(config),
                        SSLUtils.createInternalClientSSLEngineFactory(config));

        assertThat(session.getProtocol()).isEqualTo("TLSv1.3");
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLFallsBackToTls12WithoutTls13Cipher(String sslProvider) throws Exception {
        Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(SecurityOptions.SSL_PROTOCOL, "TLSv1.2,TLSv1.3");
        config.set(SecurityOptions.SSL_ALGORITHMS, tls12Ciphers(sslProvider));

        SSLSession session =
                negotiate(
                        SSLUtils.createInternalServerSSLEngineFactory(config),
                        SSLUtils.createInternalClientSSLEngineFactory(config));

        assertThat(session.getProtocol()).isEqualTo("TLSv1.2");
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLNonContiguousProtocolListWithoutCipherForHighest(String sslProvider)
            throws Exception {
        // TLSv1.3 has no usable cipher here (only TLSv1.2-style ones are configured), so Netty
        // drops it from the min/max range calculation entirely before any OpenSSL-specific
        // contiguous-range widening happens (ReferenceCountedOpenSslEngine's
        // explicitDisableTLSv13 handling). That collapses the enabled set to TLSv1.1 alone on
        // both providers, which also has no usable cipher (GCM suites are TLSv1.2+), so the
        // handshake fails on both providers instead of widening to TLSv1.2.
        Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(SecurityOptions.SSL_PROTOCOL, "TLSv1.1,TLSv1.3");
        config.set(SecurityOptions.SSL_ALGORITHMS, tls12Ciphers(sslProvider));

        final SSLHandlerFactory server = SSLUtils.createInternalServerSSLEngineFactory(config);
        final SSLHandlerFactory client = SSLUtils.createInternalClientSSLEngineFactory(config);

        assertThatThrownBy(() -> negotiate(server, client))
                .hasCauseInstanceOf(SSLHandshakeException.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testRestSSLNegotiatesTls13WhenBothSidesSupportIt(String sslProvider) throws Exception {
        Configuration config = createRestSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(SecurityOptions.SSL_PROTOCOL, "TLSv1.2,TLSv1.3");
        config.set(
                SecurityOptions.SSL_ALGORITHMS, tls12Ciphers(sslProvider) + "," + TLS_13_CIPHERS);

        SSLSession session =
                negotiate(
                        SSLUtils.createRestServerSSLEngineFactory(config),
                        SSLUtils.createRestClientSSLEngineFactory(config));

        assertThat(session.getProtocol()).isEqualTo("TLSv1.3");
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testRestSSLFallsBackToTls12WithoutTls13Cipher(String sslProvider) throws Exception {
        Configuration config = createRestSslConfigWithKeyAndTrustStores(sslProvider);
        config.set(SecurityOptions.SSL_PROTOCOL, "TLSv1.2,TLSv1.3");
        config.set(SecurityOptions.SSL_ALGORITHMS, tls12Ciphers(sslProvider));

        SSLSession session =
                negotiate(
                        SSLUtils.createRestServerSSLEngineFactory(config),
                        SSLUtils.createRestClientSSLEngineFactory(config));

        assertThat(session.getProtocol()).isEqualTo("TLSv1.2");
    }

    /**
     * The Blob server/client socket path ({@link SSLUtils#createSSLServerSocketFactory}) always
     * uses the JDK provider regardless of {@link SecurityOptions#SSL_PROVIDER}, so this is not
     * parameterized.
     */
    @Test
    void testBlobSocketSSLNegotiatesTls13WhenBothSidesSupportIt() throws Exception {
        Configuration config = createInternalSslConfigWithKeyAndTrustStores("JDK");
        config.set(SecurityOptions.SSL_PROTOCOL, "TLSv1.2,TLSv1.3");
        config.set(SecurityOptions.SSL_ALGORITHMS, tls12Ciphers("JDK") + "," + TLS_13_CIPHERS);

        assertThat(negotiateViaSockets(config)).isEqualTo("TLSv1.3");
    }

    @Test
    void testBlobSocketSSLFallsBackToTls12WithoutTls13Cipher() throws Exception {
        Configuration config = createInternalSslConfigWithKeyAndTrustStores("JDK");
        config.set(SecurityOptions.SSL_PROTOCOL, "TLSv1.2,TLSv1.3");
        config.set(SecurityOptions.SSL_ALGORITHMS, tls12Ciphers("JDK"));

        assertThat(negotiateViaSockets(config)).isEqualTo("TLSv1.2");
    }

    /**
     * Performs a real, socket-based TLS handshake between the given server and client {@link
     * SSLHandlerFactory} and returns the client's negotiated session.
     */
    private static SSLSession negotiate(
            SSLHandlerFactory serverFactory, SSLHandlerFactory clientFactory) throws Exception {
        return negotiate(serverFactory, clientFactory, "localhost");
    }

    /**
     * Like {@link #negotiate(SSLHandlerFactory, SSLHandlerFactory)}, but the client engine is told
     * to verify the given {@code clientPeerIdentity} instead of the real TCP destination
     * ("localhost"), so a mismatch between the two can be simulated without breaking the actual
     * connection.
     */
    private static SSLSession negotiate(
            SSLHandlerFactory serverFactory,
            SSLHandlerFactory clientFactory,
            String clientPeerIdentity)
            throws Exception {
        final EventLoopGroup group = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
        try {
            final CompletableFuture<SSLSession> serverSession = new CompletableFuture<>();
            final CompletableFuture<SSLSession> clientSession = new CompletableFuture<>();

            final ServerBootstrap serverBootstrap =
                    new ServerBootstrap()
                            .group(group)
                            .channel(NioServerSocketChannel.class)
                            .childHandler(
                                    new ChannelInitializer<Channel>() {
                                        @Override
                                        protected void initChannel(Channel ch) {
                                            SslHandler handler =
                                                    serverFactory.createNettySSLHandler(
                                                            UnpooledByteBufAllocator.DEFAULT);
                                            ch.pipeline().addLast(handler);
                                            completeWithSession(handler, serverSession);
                                        }
                                    });
            final Channel serverChannel = serverBootstrap.bind(0).sync().channel();
            final int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

            final Bootstrap clientBootstrap =
                    new Bootstrap()
                            .group(group)
                            .channel(NioSocketChannel.class)
                            .handler(
                                    new ChannelInitializer<Channel>() {
                                        @Override
                                        protected void initChannel(Channel ch) {
                                            // Mirrors NettyClient/RestClient: the client engine
                                            // must be created with the peer host/port, otherwise
                                            // TLSv1.3 mutual-auth handshakes fail with
                                            // "certificate_unknown: Hostname or IP address is
                                            // undefined" (TLSv1.2 tolerates the hostless engine).
                                            SslHandler handler =
                                                    clientFactory.createNettySSLHandler(
                                                            UnpooledByteBufAllocator.DEFAULT,
                                                            clientPeerIdentity,
                                                            port);
                                            ch.pipeline().addLast(handler);
                                            completeWithSession(handler, clientSession);
                                        }
                                    });
            clientBootstrap.connect("localhost", port).sync();

            final SSLSession client = clientSession.get(10, TimeUnit.SECONDS);
            final SSLSession server = serverSession.get(10, TimeUnit.SECONDS);
            assertThat(client.getProtocol()).isEqualTo(server.getProtocol());

            serverChannel.close().sync();
            return client;
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
        }
    }

    private static void completeWithSession(
            SslHandler handler, CompletableFuture<SSLSession> future) {
        handler.handshakeFuture()
                .addListener(
                        f -> {
                            if (f.isSuccess()) {
                                future.complete(handler.engine().getSession());
                            } else {
                                future.completeExceptionally(f.cause());
                            }
                        });
    }

    /**
     * Performs a real handshake through the raw-socket path used by the Blob server/client, and
     * returns the negotiated protocol.
     */
    private static String negotiateViaSockets(Configuration config) throws Exception {
        try (ServerSocket serverSocket =
                SSLUtils.createSSLServerSocketFactory(config).createServerSocket(0)) {
            final int port = serverSocket.getLocalPort();

            final CompletableFuture<String> serverProtocol = new CompletableFuture<>();
            final Thread serverThread =
                    new Thread(
                            () -> {
                                try (SSLSocket socket = (SSLSocket) serverSocket.accept()) {
                                    socket.startHandshake();
                                    serverProtocol.complete(socket.getSession().getProtocol());
                                } catch (Exception e) {
                                    serverProtocol.completeExceptionally(e);
                                }
                            });
            serverThread.start();

            final String clientProtocol;
            try (SSLSocket client =
                    (SSLSocket)
                            SSLUtils.createSSLClientSocketFactory(config)
                                    .createSocket("localhost", port)) {
                client.startHandshake();
                clientProtocol = client.getSession().getProtocol();
            }

            assertThat(serverProtocol.get(10, TimeUnit.SECONDS)).isEqualTo(clientProtocol);
            return clientProtocol;
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInvalidFingerprintParsing(String sslProvider) throws Exception {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        final String fingerprint = getCertificateFingerprint(config, "flink.test");

        config.set(
                SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT,
                fingerprint.substring(0, fingerprint.length() - 3));

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------- utils ----------------------------------

    private Configuration createRestSslConfigWithKeyStore(String sslProvider) {
        final Configuration config = new Configuration();
        config.set(SecurityOptions.SSL_REST_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addRestKeyStoreConfig(config);
        return config;
    }

    private Configuration createRestSslConfigWithTrustStore(String sslProvider) {
        final Configuration config = new Configuration();
        config.set(SecurityOptions.SSL_REST_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addRestTrustStoreConfig(config);
        return config;
    }

    public static Configuration createRestSslConfigWithKeyAndTrustStores(String sslProvider) {
        final Configuration config = new Configuration();
        config.set(SecurityOptions.SSL_REST_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addRestKeyStoreConfig(config);
        addRestTrustStoreConfig(config);
        return config;
    }

    private Configuration createInternalSslConfigWithKeyStore(String sslProvider) {
        final Configuration config = new Configuration();
        config.set(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addInternalKeyStoreConfig(config);
        return config;
    }

    private Configuration createInternalSslConfigWithTrustStore(String sslProvider) {
        final Configuration config = new Configuration();
        config.set(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addInternalTrustStoreConfig(config);
        return config;
    }

    public static Configuration createInternalSslConfigWithKeyAndTrustStores(String sslProvider) {
        final Configuration config = new Configuration();
        config.set(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        config.set(
                SecurityOptions.SSL_ALGORITHMS,
                "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
        addSslProviderConfig(config, sslProvider);
        addInternalKeyStoreConfig(config);
        addInternalTrustStoreConfig(config);
        return config;
    }

    public static String getCertificateFingerprint(Configuration config, String certificateAlias)
            throws Exception {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream keyStoreFile =
                Files.newInputStream(
                        new File(config.get(SecurityOptions.SSL_INTERNAL_KEYSTORE)).toPath())) {
            keyStore.load(
                    keyStoreFile,
                    config.get(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD).toCharArray());
        }
        return getSha1Fingerprint(keyStore.getCertificate(certificateAlias));
    }

    public static String getRestCertificateFingerprint(
            Configuration config, String certificateAlias) throws Exception {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream keyStoreFile =
                Files.newInputStream(
                        new File(config.get(SecurityOptions.SSL_REST_KEYSTORE)).toPath())) {
            keyStore.load(
                    keyStoreFile,
                    config.get(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD).toCharArray());
        }
        return getSha1Fingerprint(keyStore.getCertificate(certificateAlias));
    }

    private static void addSslProviderConfig(Configuration config, String sslProvider) {
        if (sslProvider.equalsIgnoreCase("OPENSSL")) {
            OpenSsl.ensureAvailability();
        }
        config.set(SecurityOptions.SSL_PROVIDER, sslProvider);
    }

    private static void addRestKeyStoreConfig(Configuration config) {
        config.set(SecurityOptions.SSL_REST_KEYSTORE, KEY_STORE_PATH);
        config.set(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, KEY_STORE_PASSWORD);
        config.set(SecurityOptions.SSL_REST_KEY_PASSWORD, KEY_PASSWORD);
    }

    private static void addRestTrustStoreConfig(Configuration config) {
        config.set(SecurityOptions.SSL_REST_TRUSTSTORE, TRUST_STORE_PATH);
        config.set(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, TRUST_STORE_PASSWORD);
    }

    private static void addInternalKeyStoreConfig(Configuration config) {
        config.set(SecurityOptions.SSL_INTERNAL_KEYSTORE, KEY_STORE_PATH);
        config.set(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, KEY_STORE_PASSWORD);
        config.set(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, KEY_PASSWORD);
    }

    private static void addInternalTrustStoreConfig(Configuration config) {
        config.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE, TRUST_STORE_PATH);
        config.set(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, TRUST_STORE_PASSWORD);
    }

    private static String getSha1Fingerprint(Certificate cert) {
        if (cert == null) {
            return null;
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA1");
            return toHexadecimalString(digest.digest(cert.getEncoded()));
        } catch (NoSuchAlgorithmException | CertificateEncodingException e) {
            // ignore
        }
        return null;
    }

    private static String toHexadecimalString(byte[] value) {
        StringBuilder sb = new StringBuilder();
        int len = value.length;
        for (int i = 0; i < len; i++) {
            int num = ((int) value[i]) & 0xff;
            if (num < 0x10) {
                sb.append('0');
            }
            sb.append(Integer.toHexString(num));
            if (i < len - 1) {
                sb.append(':');
            }
        }
        return sb.toString().toUpperCase(Locale.US);
    }
}
