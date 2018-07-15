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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;

import java.net.ServerSocket;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link SSLUtils}.
 */
public class SSLUtilsTest extends TestLogger {

	private static final String TRUST_STORE_PATH = SSLUtilsTest.class.getResource("/local127.truststore").getFile();
	private static final String KEY_STORE_PATH = SSLUtilsTest.class.getResource("/local127.keystore").getFile();

	private static final String TRUST_STORE_PASSWORD = "password";
	private static final String KEY_STORE_PASSWORD = "password";
	private static final String KEY_PASSWORD = "password";

	/**
	 * Tests whether activation of internal / REST SSL evaluates the config flags correctly.
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void checkEnableSSL() {
		// backwards compatibility
		Configuration oldConf = new Configuration();
		oldConf.setBoolean(SecurityOptions.SSL_ENABLED, true);
		assertTrue(SSLUtils.isInternalSSLEnabled(oldConf));
		assertTrue(SSLUtils.isRestSSLEnabled(oldConf));

		// new options take precedence
		Configuration newOptions = new Configuration();
		newOptions.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
		newOptions.setBoolean(SecurityOptions.SSL_REST_ENABLED, false);
		assertTrue(SSLUtils.isInternalSSLEnabled(newOptions));
		assertFalse(SSLUtils.isRestSSLEnabled(newOptions));

		// new options take precedence
		Configuration precedence = new Configuration();
		precedence.setBoolean(SecurityOptions.SSL_ENABLED, true);
		precedence.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, false);
		precedence.setBoolean(SecurityOptions.SSL_REST_ENABLED, false);
		assertFalse(SSLUtils.isInternalSSLEnabled(precedence));
		assertFalse(SSLUtils.isRestSSLEnabled(precedence));
	}

	@Test
	public void testSocketFactoriesWhenSslDisabled() throws Exception {
		Configuration config = new Configuration();

		try {
			SSLUtils.createSSLServerSocketFactory(config);
			fail("exception expected");
		} catch (IllegalConfigurationException ignored) {}

		try {
			SSLUtils.createSSLClientSocketFactory(config);
			fail("exception expected");
		} catch (IllegalConfigurationException ignored) {}
	}

	// ------------------------ REST client --------------------------

	/**
	 * Tests if REST Client SSL is created given a valid SSL configuration.
	 */
	@Test
	public void testRESTClientSSL() throws Exception {
		Configuration clientConfig = createRestSslConfigWithTrustStore();

		SSLEngineFactory ssl = SSLUtils.createRestClientSSLEngineFactory(clientConfig);
		assertNotNull(ssl);
	}

	/**
	 * Tests that REST Client SSL Client is not created if SSL is not configured.
	 */
	@Test
	public void testRESTClientSSLDisabled() throws Exception {
		Configuration clientConfig = createRestSslConfigWithTrustStore();
		clientConfig.setBoolean(SecurityOptions.SSL_REST_ENABLED, false);

		try {
			SSLUtils.createRestClientSSLEngineFactory(clientConfig);
			fail("exception expected");
		} catch (IllegalConfigurationException ignored) {}
	}

	/**
	 * Tests that REST Client SSL creation fails with bad SSL configuration.
	 */
	@Test
	public void testRESTClientSSLMissingTrustStore() throws Exception {
		Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
		config.setString(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "some password");

		try {
			SSLUtils.createRestClientSSLEngineFactory(config);
			fail("exception expected");
		} catch (IllegalConfigurationException ignored) {}
	}

	/**
	 * Tests that REST Client SSL creation fails with bad SSL configuration.
	 */
	@Test
	public void testRESTClientSSLMissingPassword() throws Exception {
		Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
		config.setString(SecurityOptions.SSL_REST_TRUSTSTORE, TRUST_STORE_PATH);

		try {
			SSLUtils.createRestClientSSLEngineFactory(config);
			fail("exception expected");
		} catch (IllegalConfigurationException ignored) {}
	}

	/**
	 * Tests that REST Client SSL creation fails with bad SSL configuration.
	 */
	@Test
	public void testRESTClientSSLWrongPassword() throws Exception {
		Configuration clientConfig = createRestSslConfigWithTrustStore();
		clientConfig.setString(SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD, "badpassword");

		try {
			SSLUtils.createRestClientSSLEngineFactory(clientConfig);
			fail("exception expected");
		} catch (Exception ignored) {}
	}

	// ------------------------ server --------------------------

	/**
	 * Tests that REST Server SSL Engine is created given a valid SSL configuration.
	 */
	@Test
	public void testRESTServerSSL() throws Exception {
		Configuration serverConfig = createRestSslConfigWithKeyStore();

		SSLEngineFactory ssl = SSLUtils.createRestServerSSLEngineFactory(serverConfig);
		assertNotNull(ssl);
	}

	/**
	 * Tests that REST Server SSL Engine is not created if SSL is disabled.
	 */
	@Test
	public void testRESTServerSSLDisabled() throws Exception {
		Configuration serverConfig = createRestSslConfigWithKeyStore();
		serverConfig.setBoolean(SecurityOptions.SSL_REST_ENABLED, false);

		try {
			SSLUtils.createRestServerSSLEngineFactory(serverConfig);
			fail("exception expected");
		} catch (IllegalConfigurationException ignored) {}
	}

	/**
	 * Tests that REST Server SSL Engine creation fails with bad SSL configuration.
	 */
	@Test
	public void testRESTServerSSLBadKeystorePassword() {
		Configuration serverConfig = createRestSslConfigWithKeyStore();
		serverConfig.setString(SecurityOptions.SSL_REST_KEYSTORE_PASSWORD, "badpassword");

		try {
			SSLUtils.createRestServerSSLEngineFactory(serverConfig);
			fail("exception expected");
		} catch (Exception ignored) {}
	}

	/**
	 * Tests that REST Server SSL Engine creation fails with bad SSL configuration.
	 */
	@Test
	public void testRESTServerSSLBadKeyPassword() {
		Configuration serverConfig = createRestSslConfigWithKeyStore();
		serverConfig.setString(SecurityOptions.SSL_REST_KEY_PASSWORD, "badpassword");

		try {
			SSLUtils.createRestServerSSLEngineFactory(serverConfig);
			fail("exception expected");
		} catch (Exception ignored) {}
	}

	// ----------------------- mutual auth contexts --------------------------

	@Test
	public void testInternalSSL() throws Exception {
		final Configuration config = createInternalSslConfigWithKeyAndTrustStores();
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
		} catch (Exception ignored) {}

		try {
			SSLUtils.createInternalClientSSLEngineFactory(config);
			fail("exception expected");
		} catch (Exception ignored) {}
	}

	@Test
	public void testInternalSSLKeyStoreOnly() throws Exception {
		final Configuration config = createInternalSslConfigWithKeyStore();

		try {
			SSLUtils.createInternalServerSSLEngineFactory(config);
			fail("exception expected");
		} catch (Exception ignored) {}

		try {
			SSLUtils.createInternalClientSSLEngineFactory(config);
			fail("exception expected");
		} catch (Exception ignored) {}
	}

	@Test
	public void testInternalSSLTrustStoreOnly() throws Exception {
		final Configuration config = createInternalSslConfigWithTrustStore();

		try {
			SSLUtils.createInternalServerSSLEngineFactory(config);
			fail("exception expected");
		} catch (Exception ignored) {}

		try {
			SSLUtils.createInternalClientSSLEngineFactory(config);
			fail("exception expected");
		} catch (Exception ignored) {}
	}

	@Test
	public void testInternalSSLWrongKeystorePassword() throws Exception {
		final Configuration config = createInternalSslConfigWithKeyAndTrustStores();
		config.setString(SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD, "badpw");

		try {
			SSLUtils.createInternalServerSSLEngineFactory(config);
			fail("exception expected");
		} catch (Exception ignored) {}

		try {
			SSLUtils.createInternalClientSSLEngineFactory(config);
			fail("exception expected");
		} catch (Exception ignored) {}
	}

	@Test
	public void testInternalSSLWrongTruststorePassword() throws Exception {
		final Configuration config = createInternalSslConfigWithKeyAndTrustStores();
		config.setString(SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD, "badpw");

		try {
			SSLUtils.createInternalServerSSLEngineFactory(config);
			fail("exception expected");
		} catch (Exception ignored) {}

		try {
			SSLUtils.createInternalClientSSLEngineFactory(config);
			fail("exception expected");
		} catch (Exception ignored) {}
	}

	@Test
	public void testInternalSSLWrongKeyPassword() throws Exception {
		final Configuration config = createInternalSslConfigWithKeyAndTrustStores();
		config.setString(SecurityOptions.SSL_INTERNAL_KEY_PASSWORD, "badpw");

		try {
			SSLUtils.createInternalServerSSLEngineFactory(config);
			fail("exception expected");
		} catch (Exception ignored) {}

		try {
			SSLUtils.createInternalClientSSLEngineFactory(config);
			fail("exception expected");
		} catch (Exception ignored) {}
	}

	// -------------------- protocols and cipher suites -----------------------

	/**
	 * Tests if SSLUtils set the right ssl version and cipher suites for SSLServerSocket.
	 */
	@Test
	public void testSetSSLVersionAndCipherSuitesForSSLServerSocket() throws Exception {
		Configuration serverConfig = createInternalSslConfigWithKeyAndTrustStores();

		// set custom protocol and cipher suites
		serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1.1");
		serverConfig.setString(SecurityOptions.SSL_ALGORITHMS, "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA256");

		try (ServerSocket socket = SSLUtils.createSSLServerSocketFactory(serverConfig).createServerSocket(0)) {
			assertTrue(socket instanceof SSLServerSocket);
			final SSLServerSocket sslSocket = (SSLServerSocket) socket;

			String[] protocols = sslSocket.getEnabledProtocols();
			String[] algorithms = sslSocket.getEnabledCipherSuites();

			assertEquals(1, protocols.length);
			assertEquals("TLSv1.1", protocols[0]);
			assertEquals(2, algorithms.length);
			assertThat(algorithms, arrayContainingInAnyOrder(
					"TLS_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_CBC_SHA256"));
		}
	}

	/**
	 * Tests that {@link SSLEngineFactory} is created correctly.
	 */
	@Test
	public void testCreateSSLEngineFactory() throws Exception {
		Configuration serverConfig = createInternalSslConfigWithKeyAndTrustStores();

		// set custom protocol and cipher suites
		serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1");
		serverConfig.setString(SecurityOptions.SSL_ALGORITHMS, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA,TLS_DHE_RSA_WITH_AES_128_CBC_SHA256");

		final SSLEngineFactory serverSSLEngineFactory = SSLUtils.createInternalServerSSLEngineFactory(serverConfig);
		final SSLEngine sslEngine = serverSSLEngineFactory.createSSLEngine();

		assertEquals(1, sslEngine.getEnabledProtocols().length);
		assertEquals("TLSv1", sslEngine.getEnabledProtocols()[0]);

		assertEquals(2, sslEngine.getEnabledCipherSuites().length);
		assertThat(sslEngine.getEnabledCipherSuites(), arrayContainingInAnyOrder(
				"TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256"));
	}

	// ------------------------------- utils ----------------------------------

	public static Configuration createRestSslConfigWithKeyStore() {
		final Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
		addRestKeyStoreConfig(config);
		return config;
	}

	public static Configuration createRestSslConfigWithTrustStore() {
		final Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
		addRestTrustStoreConfig(config);
		return config;
	}

	public static Configuration createRestSslConfigWithKeyAndTrustStores() {
		final Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_REST_ENABLED, true);
		addRestKeyStoreConfig(config);
		addRestTrustStoreConfig(config);
		return config;
	}

	public static Configuration createInternalSslConfigWithKeyStore() {
		final Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
		addInternalKeyStoreConfig(config);
		return config;
	}

	public static Configuration createInternalSslConfigWithTrustStore() {
		final Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
		addInternalTrustStoreConfig(config);
		return config;
	}

	public static Configuration createInternalSslConfigWithKeyAndTrustStores() {
		final Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_INTERNAL_ENABLED, true);
		addInternalKeyStoreConfig(config);
		addInternalTrustStoreConfig(config);
		return config;
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
}
