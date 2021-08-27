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
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.netty4.io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.OpenSsl;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link SSLUtils}. */
@RunWith(Parameterized.class)
public class SSLUtilsTest extends TestLogger {

    private static final String TRUST_STORE_PATH =
            SSLUtilsTest.class.getResource("/local127.truststore").getFile();
    private static final String KEY_STORE_PATH =
            SSLUtilsTest.class.getResource("/local127.keystore").getFile();

    private static final String TRUST_STORE_PASSWORD = "password";
    private static final String KEY_STORE_PASSWORD = "password";
    private static final String KEY_PASSWORD = "password";

    public static final List<String> AVAILABLE_SSL_PROVIDERS;

    static {
        if (System.getProperty("flink.tests.with-openssl") != null) {
            assertTrue(
                    "openSSL not available but required (property 'flink.tests.with-openssl' is set)",
                    OpenSsl.isAvailable());
            AVAILABLE_SSL_PROVIDERS = Arrays.asList("JDK", "OPENSSL");
        } else {
            AVAILABLE_SSL_PROVIDERS = Collections.singletonList("JDK");
        }
    }

    @Parameterized.Parameter public String sslProvider;

    @Parameterized.Parameters(name = "SSL provider = {0}")
    public static List<String> parameters() {
        return AVAILABLE_SSL_PROVIDERS;
    }

    @Test
    public void testSocketFactoriesWhenSslDisabled() throws Exception {
        Configuration config = new Configuration();

        try {
            SSLUtils.createSSLServerSocketFactory(config);
            fail("exception expected");
        } catch (IllegalConfigurationException ignored) {
        }

        try {
            SSLUtils.createSSLClientSocketFactory(config);
            fail("exception expected");
        } catch (IllegalConfigurationException ignored) {
        }
    }

    // ------------------------ REST client --------------------------

    /** Tests if REST Client SSL is created given a valid SSL configuration. */
    @Test
    public void testRESTClientSSL() throws Exception {
        Configuration clientConfig = createRestSslConfigWithTrustStore();

        SSLHandlerFactory ssl = SSLUtils.createRestClientSSLEngineFactory(clientConfig);
        assertNotNull(ssl);
    }

    /** Tests that REST Client SSL Client is not created if SSL is not configured. */
    @Test
    public void testRESTClientSSLDisabled() throws Exception {
        Configuration clientConfig = createRestSslConfigWithTrustStore();
        clientConfig.setBoolean(SecurityOptions.SSL_REST_ENABLED, false);

        try {
            SSLUtils.createRestClientSSLEngineFactory(clientConfig);
            fail("exception expected");
        } catch (IllegalConfigurationException ignored) {
        }
    }

    /** Tests that REST Client SSL creation fails with bad SSL configuration. */
    @Test
    public void testRESTClientSSLMissingTrustStore() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        config.setString(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "some password");

        try {
            SSLUtils.createRestClientSSLEngineFactory(config);
            fail("exception expected");
        } catch (IllegalConfigurationException ignored) {
        }
    }

    /** Tests that REST Client SSL creation fails with bad SSL configuration. */
    @Test
    public void testRESTClientSSLMissingPassword() throws Exception {
        Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        config.setString(SecurityOptions.SSL_REST_TRUSTSTORE, TRUST_STORE_PATH);

        try {
            SSLUtils.createRestClientSSLEngineFactory(config);
            fail("exception expected");
        } catch (IllegalConfigurationException ignored) {
        }
    }

    /** Tests that REST Client SSL creation fails with bad SSL configuration. */
    @Test
    public void testRESTClientSSLWrongPassword() throws Exception {
        Configuration clientConfig = createRestSslConfigWithTrustStore();
        clientConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "badpassword");

        try {
            SSLUtils.createRestClientSSLEngineFactory(clientConfig);
            fail("exception expected");
        } catch (Exception ignored) {
        }
    }

    // ------------------------ server --------------------------

    /** Tests that REST Server SSL Engine is created given a valid SSL configuration. */
    @Test
    public void testRESTServerSSL() throws Exception {
        Configuration serverConfig = createRestSslConfigWithKeyStore();

        SSLHandlerFactory ssl = SSLUtils.createRestServerSSLEngineFactory(serverConfig);
        assertNotNull(ssl);
    }

    /** Tests that REST Server SSL Engine is not created if SSL is disabled. */
    @Test
    public void testRESTServerSSLDisabled() throws Exception {
        Configuration serverConfig = createRestSslConfigWithKeyStore();
        serverConfig.setBoolean(SecurityOptions.SSL_REST_ENABLED, false);

        try {
            SSLUtils.createRestServerSSLEngineFactory(serverConfig);
            fail("exception expected");
        } catch (IllegalConfigurationException ignored) {
        }
    }

    /** Tests that REST Server SSL Engine creation fails with bad SSL configuration. */
    @Test
    public void testRESTServerSSLBadKeystorePassword() {
        Configuration serverConfig = createRestSslConfigWithKeyStore();
        serverConfig.setString(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, "badpassword");

        try {
            SSLUtils.createRestServerSSLEngineFactory(serverConfig);
            fail("exception expected");
        } catch (Exception ignored) {
        }
    }

    /** Tests that REST Server SSL Engine creation fails with bad SSL configuration. */
    @Test
    public void testRESTServerSSLBadKeyPassword() {
        Configuration serverConfig = createRestSslConfigWithKeyStore();
        serverConfig.setString(SecurityOptions.SSL_REST_KEY_PASSWORD, "badpassword");

        try {
            SSLUtils.createRestServerSSLEngineFactory(serverConfig);
            fail("exception expected");
        } catch (Exception ignored) {
        }
    }

    // ----------------------- mutual auth contexts --------------------------

    @Test
    public void testInternalSSL() throws Exception {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores();
        assertNotNull(SSLUtils.createInternalServerSSLEngineFactory(config));
        assertNotNull(SSLUtils.createInternalClientSSLEngineFactory(config));
    }

    @Test
    public void testInternalSSLWithSSLPinning() throws Exception {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores();
        config.setString(
                SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT,
                getCertificateFingerprint(config, "flink.test"));

        assertNotNull(SSLUtils.createInternalServerSSLEngineFactory(config));
        assertNotNull(SSLUtils.createInternalClientSSLEngineFactory(config));
    }

    @Test
    public void testInternalSSLDisables() throws Exception {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores();
        config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, false);

        try {
            SSLUtils.createInternalServerSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }

        try {
            SSLUtils.createInternalClientSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testInternalSSLKeyStoreOnly() throws Exception {
        final Configuration config = createInternalSslConfigWithKeyStore();

        try {
            SSLUtils.createInternalServerSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }

        try {
            SSLUtils.createInternalClientSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testInternalSSLTrustStoreOnly() throws Exception {
        final Configuration config = createInternalSslConfigWithTrustStore();

        try {
            SSLUtils.createInternalServerSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }

        try {
            SSLUtils.createInternalClientSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testInternalSSLWrongKeystorePassword() throws Exception {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores();
        config.setString(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, "badpw");

        try {
            SSLUtils.createInternalServerSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }

        try {
            SSLUtils.createInternalClientSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testInternalSSLWrongTruststorePassword() throws Exception {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores();
        config.setString(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, "badpw");

        try {
            SSLUtils.createInternalServerSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }

        try {
            SSLUtils.createInternalClientSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void testInternalSSLWrongKeyPassword() throws Exception {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores();
        config.setString(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, "badpw");

        try {
            SSLUtils.createInternalServerSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }

        try {
            SSLUtils.createInternalClientSSLEngineFactory(config);
            fail("exception expected");
        } catch (Exception ignored) {
        }
    }

    // -------------------- protocols and cipher suites -----------------------

    /** Tests if SSLUtils set the right ssl version and cipher suites for SSLServerSocket. */
    @Test
    public void testSetSSLVersionAndCipherSuitesForSSLServerSocket() throws Exception {
        Configuration serverConfig = createInternalSslConfigWithKeyAndTrustStores();

        // set custom protocol and cipher suites
        serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1.1");
        serverConfig.setString(
                SecurityOptions.SSL_ALGORITHMS,
                "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA256");

        try (ServerSocket socket =
                SSLUtils.createSSLServerSocketFactory(serverConfig).createServerSocket(0)) {
            assertTrue(socket instanceof SSLServerSocket);
            final SSLServerSocket sslSocket = (SSLServerSocket) socket;

            String[] protocols = sslSocket.getEnabledProtocols();
            String[] algorithms = sslSocket.getEnabledCipherSuites();

            assertEquals(1, protocols.length);
            assertEquals("TLSv1.1", protocols[0]);
            assertEquals(2, algorithms.length);
            assertThat(
                    algorithms,
                    arrayContainingInAnyOrder(
                            "TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA256"));
        }
    }

    /** Tests that {@link SSLHandlerFactory} is created correctly. */
    @Test
    public void testCreateSSLEngineFactory() throws Exception {
        Configuration serverConfig = createInternalSslConfigWithKeyAndTrustStores();
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

        assertEquals(expectedSslProtocols.length, sslHandler.engine().getEnabledProtocols().length);
        assertThat(
                sslHandler.engine().getEnabledProtocols(),
                arrayContainingInAnyOrder(expectedSslProtocols));

        assertEquals(sslAlgorithms.length, sslHandler.engine().getEnabledCipherSuites().length);
        assertThat(
                sslHandler.engine().getEnabledCipherSuites(),
                arrayContainingInAnyOrder(sslAlgorithms));
    }

    @Test
    public void testInvalidFingerprintParsing() throws Exception {
        final Configuration config = createInternalSslConfigWithKeyAndTrustStores();
        final String fingerprint = getCertificateFingerprint(config, "flink.test");

        config.setString(
                SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT,
                fingerprint.substring(0, fingerprint.length() - 3));

        try {
            SSLUtils.createInternalServerSSLEngineFactory(config);
            fail("expected exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("malformed fingerprint"));
        }
    }

    // ------------------------------- utils ----------------------------------

    private Configuration createRestSslConfigWithKeyStore() {
        final Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addRestKeyStoreConfig(config);
        return config;
    }

    private Configuration createRestSslConfigWithTrustStore() {
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

    private Configuration createInternalSslConfigWithKeyStore() {
        final Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addInternalKeyStoreConfig(config);
        return config;
    }

    private Configuration createInternalSslConfigWithTrustStore() {
        final Configuration config = new Configuration();
        config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
        addSslProviderConfig(config, sslProvider);
        addInternalTrustStoreConfig(config);
        return config;
    }

    private Configuration createInternalSslConfigWithKeyAndTrustStores() {
        return createInternalSslConfigWithKeyAndTrustStores(sslProvider);
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
            assertTrue("openSSL not available", OpenSsl.isAvailable());

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
