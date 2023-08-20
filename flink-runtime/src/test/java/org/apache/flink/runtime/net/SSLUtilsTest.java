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

import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.ClientAuth;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.JdkSslContext;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.OpenSsl;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.net.ssl.SSLServerSocket;

import java.io.File;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

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
        clientConfig.setBoolean(SecurityOptions.SSL_REST_ENABLED, false);

        assertThatThrownBy(() -> SSLUtils.createRestClientSSLEngineFactory(clientConfig))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    /** Tests that REST Client SSL creation fails with bad SSL configuration. */
    @Test
    void testRESTClientSSLMissingTrustStore() {
        Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        config.setString(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "some password");

        assertThatThrownBy(() -> SSLUtils.createRestClientSSLEngineFactory(config))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    /** Tests that REST Client SSL creation fails with bad SSL configuration. */
    @Test
    void testRESTClientSSLMissingPassword() {
        Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        config.setString(SecurityOptions.SSL_REST_TRUSTSTORE, TRUST_STORE_PATH);

        assertThatThrownBy(() -> SSLUtils.createRestClientSSLEngineFactory(config))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    /** Tests that REST Client SSL creation fails with bad SSL configuration. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTClientSSLWrongPassword(String sslProvider) {
        Configuration clientConfig = createRestSslConfigWithTrustStore(sslProvider);
        clientConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "badpassword");

        assertThatThrownBy(() -> SSLUtils.createRestClientSSLEngineFactory(clientConfig))
                .isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTSSLConfigCipherAlgorithms(String sslProvider) throws Exception {
        String testSSLAlgorithms = "test_algorithm1,test_algorithm2";
        Configuration config = createRestSslConfigWithTrustStore(sslProvider);
        config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
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
        serverConfig.setBoolean(SecurityOptions.SSL_REST_ENABLED, false);

        assertThatThrownBy(() -> SSLUtils.createRestServerSSLEngineFactory(serverConfig))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    /** Tests that REST Server SSL Engine creation fails with bad SSL configuration. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTServerSSLBadKeystorePassword(String sslProvider) {
        Configuration serverConfig = createRestSslConfigWithKeyStore(sslProvider);
        serverConfig.setString(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, "badpassword");

        assertThatThrownBy(() -> SSLUtils.createRestServerSSLEngineFactory(serverConfig))
                .isInstanceOf(Exception.class);
    }

    /** Tests that REST Server SSL Engine creation fails with bad SSL configuration. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testRESTServerSSLBadKeyPassword(String sslProvider) {
        Configuration serverConfig = createRestSslConfigWithKeyStore(sslProvider);
        serverConfig.setString(SecurityOptions.SSL_REST_KEY_PASSWORD, "badpassword");

        assertThatThrownBy(() -> SSLUtils.createRestServerSSLEngineFactory(serverConfig))
                .isInstanceOf(Exception.class);
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
        config.setString(
                SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT,
                getCertificateFingerprint(config, "flink.test"));

        assertThat(SSLUtils.createInternalServerSSLEngineFactory(config)).isNotNull();
        assertThat(SSLUtils.createInternalClientSSLEngineFactory(config)).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLDisables(String sslProvider) {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, false);

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
        config.setString(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, "badpw");

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(Exception.class);

        assertThatThrownBy(() -> SSLUtils.createInternalClientSSLEngineFactory(config))
                .isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLWrongTruststorePassword(String sslProvider) {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.setString(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, "badpw");

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(Exception.class);

        assertThatThrownBy(() -> SSLUtils.createInternalClientSSLEngineFactory(config))
                .isInstanceOf(Exception.class);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInternalSSLWrongKeyPassword(String sslProvider) {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        config.setString(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, "badpw");

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(Exception.class);

        assertThatThrownBy(() -> SSLUtils.createInternalClientSSLEngineFactory(config))
                .isInstanceOf(Exception.class);
    }

    // -------------------- protocols and cipher suites -----------------------

    /** Tests if SSLUtils set the right ssl version and cipher suites for SSLServerSocket. */
    @ParameterizedTest
    @MethodSource("parameters")
    void testSetSSLVersionAndCipherSuitesForSSLServerSocket(String sslProvider) throws Exception {
        Configuration serverConfig = createInternalSslConfigWithKeyAndTrustStores(sslProvider);

        // set custom protocol and cipher suites
        serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1.1");
        serverConfig.setString(
                SecurityOptions.SSL_ALGORITHMS,
                "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA256");

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
                    .contains("TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA256");
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
        serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1");
        serverConfig.setString(SecurityOptions.SSL_ALGORITHMS, String.join(",", sslAlgorithms));

        final SSLHandlerFactory serverSSLHandlerFactory =
                SSLUtils.createInternalServerSSLEngineFactory(serverConfig);
        final SslHandler sslHandler =
                serverSSLHandlerFactory.createNettySSLHandler(UnpooledByteBufAllocator.DEFAULT);

        assertThat(sslHandler.engine().getEnabledProtocols()).hasSameSizeAs(expectedSslProtocols);
        assertThat(sslHandler.engine().getEnabledProtocols()).contains(expectedSslProtocols);

        assertThat(sslHandler.engine().getEnabledCipherSuites()).hasSameSizeAs(sslAlgorithms);
        assertThat(sslHandler.engine().getEnabledCipherSuites()).contains(sslAlgorithms);
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testInvalidFingerprintParsing(String sslProvider) throws Exception {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores(sslProvider);
        final String fingerprint = getCertificateFingerprint(config, "flink.test");

        config.setString(
                SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT,
                fingerprint.substring(0, fingerprint.length() - 3));

        assertThatThrownBy(() -> SSLUtils.createInternalServerSSLEngineFactory(config))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------- utils ----------------------------------

    private Configuration createRestSslConfigWithKeyStore(String sslProvider) {
        final Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addRestKeyStoreConfig(config);
        return config;
    }

    private Configuration createRestSslConfigWithTrustStore(String sslProvider) {
        final Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addRestTrustStoreConfig(config);
        return config;
    }

    public static Configuration createRestSslConfigWithKeyAndTrustStores(String sslProvider) {
        final Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addRestKeyStoreConfig(config);
        addRestTrustStoreConfig(config);
        return config;
    }

    private Configuration createInternalSslConfigWithKeyStore(String sslProvider) {
        final Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addInternalKeyStoreConfig(config);
        return config;
    }

    private Configuration createInternalSslConfigWithTrustStore(String sslProvider) {
        final Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addInternalTrustStoreConfig(config);
        return config;
    }

    public static Configuration createInternalSslConfigWithKeyAndTrustStores(String sslProvider) {
        final Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
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
                        new File(config.getString(SecurityOptions.SSL_INTERNAL_KEYSTORE))
                                .toPath())) {
            keyStore.load(
                    keyStoreFile,
                    config.getString(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD).toCharArray());
        }
        return getSha1Fingerprint(keyStore.getCertificate(certificateAlias));
    }

    public static String getRestCertificateFingerprint(
            Configuration config, String certificateAlias) throws Exception {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream keyStoreFile =
                Files.newInputStream(
                        new File(config.getString(SecurityOptions.SSL_REST_KEYSTORE)).toPath())) {
            keyStore.load(
                    keyStoreFile,
                    config.getString(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD).toCharArray());
        }
        return getSha1Fingerprint(keyStore.getCertificate(certificateAlias));
    }

    private static void addSslProviderConfig(Configuration config, String sslProvider) {
        if (sslProvider.equalsIgnoreCase("OPENSSL")) {
            OpenSsl.ensureAvailability();

            // Flink's default algorithm set is not available for openSSL - choose a different one:
            config.setString(
                    SecurityOptions.SSL_ALGORITHMS,
                    "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
        }
        config.setString(SecurityOptions.SSL_PROVIDER, sslProvider);
    }

    private static void addRestKeyStoreConfig(Configuration config) {
        config.setString(SecurityOptions.SSL_REST_KEYSTORE, KEY_STORE_PATH);
        config.setString(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, KEY_STORE_PASSWORD);
        config.setString(SecurityOptions.SSL_REST_KEY_PASSWORD, KEY_PASSWORD);
    }

    private static void addRestTrustStoreConfig(Configuration config) {
        config.setString(SecurityOptions.SSL_REST_TRUSTSTORE, TRUST_STORE_PATH);
        config.setString(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, TRUST_STORE_PASSWORD);
    }

    private static void addInternalKeyStoreConfig(Configuration config) {
        config.setString(SecurityOptions.SSL_INTERNAL_KEYSTORE, KEY_STORE_PATH);
        config.setString(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, KEY_STORE_PASSWORD);
        config.setString(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, KEY_PASSWORD);
    }

    private static void addInternalTrustStoreConfig(Configuration config) {
        config.setString(SecurityOptions.SSL_INTERNAL_TRUSTSTORE, TRUST_STORE_PATH);
        config.setString(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, TRUST_STORE_PASSWORD);
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
