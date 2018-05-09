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

import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;

import java.net.ServerSocket;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link SSLUtils}.
 */
public class SSLUtilsTest {

	private static final String TRUST_STORE_PATH = "src/test/resources/local127.truststore";
	private static final String TRUST_STORE_PASSWORD = "password";
	private static final String KEY_STORE_PATH = "src/test/resources/local127.keystore";
	private static final String KEY_STORE_PASSWORD = "password";
	private static final String KEY_PASSWORD = "password";

	// ------------------------ client --------------------------

	/**
	 * Tests if SSL Client Context is created given a valid SSL configuration.
	 */
	@Test
	public void testCreateSSLClientContext() throws Exception {
		Configuration clientConfig = createSslConfigWithTrustStore();

		SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
		assertNotNull(clientContext);
	}

	/**
	 * Tests if SSL Client Context is not created if SSL is not configured.
	 */
	@Test
	public void testCreateSSLClientContextWithSSLDisabled() throws Exception {
		Configuration clientConfig = createSslConfigWithTrustStore();
		clientConfig.setBoolean(SecurityOptions.SSL_ENABLED, false);

		SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
		assertNull(clientContext);
	}

	/**
	 * Tests if SSL Client Context creation fails with bad SSL configuration.
	 */
	@Test
	public void testCreateSSLClientContextMissingTrustStore() throws Exception {
		Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_ENABLED, true);
		config.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "some password");

		try {
			SSLContext clientContext = SSLUtils.createSSLClientContext(config);
			fail("exception expected");
		}
		catch (IllegalConfigurationException e) {
			// expected
		}
	}

	/**
	 * Tests if SSL Client Context creation fails with bad SSL configuration.
	 */
	@Test
	public void testCreateSSLClientContextMissingPassword() throws Exception {
		Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_ENABLED, true);
		config.setString(SecurityOptions.SSL_TRUSTSTORE, TRUST_STORE_PATH);

		try {
			SSLContext clientContext = SSLUtils.createSSLClientContext(config);
			fail("exception expected");
		}
		catch (IllegalConfigurationException e) {
			// expected
		}
	}

	/**
	 * Tests if SSL Client Context creation fails with bad SSL configuration.
	 */
	@Test
	public void testCreateSSLClientContextWrongPassword() throws Exception {
		Configuration clientConfig = createSslConfigWithTrustStore();
		clientConfig.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "badpassword");

		try {
			SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
			fail("SSL client context created even with bad SSL configuration ");
		} catch (Exception e) {
			// Exception here is valid
		}
	}

	// ------------------------ server --------------------------

	/**
	 * Tests if SSL Server Context is created given a valid SSL configuration.
	 */
	@Test
	public void testCreateSSLServerContext() throws Exception {
		Configuration serverConfig = createSslConfigWithKeyStore();

		SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
		assertNotNull(serverContext);
	}

	/**
	 * Tests if SSL Server Context is not created if SSL is disabled.
	 */
	@Test
	public void testCreateSSLServerContextWithSSLDisabled() throws Exception {
		Configuration serverConfig = createSslConfigWithKeyStore();
		serverConfig.setBoolean(SecurityOptions.SSL_ENABLED, false);

		SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
		assertNull(serverContext);
	}

	/**
	 * Tests if SSL Server Context creation fails with bad SSL configuration.
	 */
	@Test
	public void testCreateSSLServerContextBadKeystorePassword() {
		Configuration serverConfig = createSslConfigWithKeyStore();
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "badpassword");

		try {
			SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
			fail("SSL server context created even with bad SSL configuration ");
		} catch (Exception e) {
			// Exception here is valid
		}
	}

	/**
	 * Tests if SSL Server Context creation fails with bad SSL configuration.
	 */
	@Test
	public void testCreateSSLServerContextBadKeyPassword() {
		Configuration serverConfig = createSslConfigWithKeyStore();
		serverConfig.setString(SecurityOptions.SSL_KEY_PASSWORD, "badpassword");

		try {
			SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
			fail("SSL server context created even with bad SSL configuration ");
		} catch (Exception e) {
			// Exception here is valid
		}
	}

	// ----------------------- mutual auth contexts --------------------------

	@Test
	public void testCreateMutualAuthServerContext() throws Exception {
		final Configuration config = createSslConfigWithKeyAndTrustStores();
		assertNotNull(SSLUtils.createSSLServerContextIntraCluster(config));
	}

	@Test
	public void testCreateMutualAuthClientContext() throws Exception {
		final Configuration config = createSslConfigWithKeyAndTrustStores();
		assertNotNull(SSLUtils.createSSLClientContextIntraCluster(config));
	}

	@Test
	public void testCreateMutualAuthServerContextKeyStoreOnly() throws Exception {
		final Configuration config = createSslConfigWithKeyStore();

		try {
			SSLContext serverContext = SSLUtils.createSSLServerContextIntraCluster(config);
			fail("exception expected");
		} catch (IllegalConfigurationException e) {
			// expected
		}
	}

	@Test
	public void testCreateMutualAuthServerContextTrustStoreOnly() throws Exception {
		final Configuration config = createSslConfigWithTrustStore();

		try {
			SSLContext serverContext = SSLUtils.createSSLServerContextIntraCluster(config);
			fail("exception expected");
		} catch (IllegalConfigurationException e) {
			// expected
		}
	}

	@Test
	public void testCreateMutualAuthClientContextKeyStoreOnly() throws Exception {
		final Configuration config = createSslConfigWithKeyStore();

		try {
			SSLContext serverContext = SSLUtils.createSSLClientContextIntraCluster(config);
			fail("exception expected");
		} catch (IllegalConfigurationException e) {
			// expected
		}
	}

	@Test
	public void testCreateMutualAuthClientContextTrustStoreOnly() throws Exception {
		final Configuration config = createSslConfigWithTrustStore();

		try {
			SSLContext serverContext = SSLUtils.createSSLClientContextIntraCluster(config);
			fail("exception expected");
		} catch (IllegalConfigurationException e) {
			// expected
		}
	}

	// -------------------- protocols and cipher suites -----------------------

	/**
	 * Tests if SSL Server Context creation fails with bad SSL configuration.
	 */
	@Test
	public void testCreateSSLServerContextWithMultiProtocols() {
		Configuration serverConfig = createSslConfigWithKeyStore();
		serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1,TLSv1.2");

		try {
			SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
			fail("SSL server context should not be created with multiple protocols");
		} catch (Exception e) {
			// Exception here is valid
		}
	}

	/**
	 * Tests if SSLUtils set the right ssl version and cipher suites for SSLServerSocket.
	 */
	@Test
	public void testSetSSLVersionAndCipherSuitesForSSLServerSocket() throws Exception {
		Configuration serverConfig = createSslConfigWithKeyStore();
		serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1.1");
		serverConfig.setString(SecurityOptions.SSL_ALGORITHMS, "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA256");

		SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);

		try (ServerSocket socket = serverContext.getServerSocketFactory().createServerSocket(0)) {

			String[] protocols = ((SSLServerSocket) socket).getEnabledProtocols();
			String[] algorithms = ((SSLServerSocket) socket).getEnabledCipherSuites();

			Assert.assertNotEquals(1, protocols.length);
			Assert.assertNotEquals(2, algorithms.length);

			SSLUtils.setSSLVerAndCipherSuites(socket, serverConfig);
			protocols = ((SSLServerSocket) socket).getEnabledProtocols();
			algorithms = ((SSLServerSocket) socket).getEnabledCipherSuites();

			Assert.assertEquals(1, protocols.length);
			Assert.assertEquals("TLSv1.1", protocols[0]);
			Assert.assertEquals(2, algorithms.length);
			Assert.assertTrue(algorithms[0].equals("TLS_RSA_WITH_AES_128_CBC_SHA") || algorithms[0].equals("TLS_RSA_WITH_AES_128_CBC_SHA256"));
			Assert.assertTrue(algorithms[1].equals("TLS_RSA_WITH_AES_128_CBC_SHA") || algorithms[1].equals("TLS_RSA_WITH_AES_128_CBC_SHA256"));
		}
	}

	/**
	 * Tests if SSLUtils set the right ssl version and cipher suites for SSLEngine.
	 */
	@Test
	public void testSetSSLVersionAndCipherSuitesForSSLEngine() throws Exception {
		Configuration serverConfig = createSslConfigWithKeyStore();
		serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1");
		serverConfig.setString(SecurityOptions.SSL_ALGORITHMS, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA,TLS_DHE_RSA_WITH_AES_128_CBC_SHA256");

		SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
		SSLEngine engine = serverContext.createSSLEngine();

		String[] protocols = engine.getEnabledProtocols();
		String[] algorithms = engine.getEnabledCipherSuites();

		Assert.assertNotEquals(1, protocols.length);
		Assert.assertNotEquals(2, algorithms.length);

		SSLUtils.setSSLVerAndCipherSuites(engine, serverConfig);
		protocols = engine.getEnabledProtocols();
		algorithms = engine.getEnabledCipherSuites();

		Assert.assertEquals(1, protocols.length);
		Assert.assertEquals("TLSv1", protocols[0]);
		Assert.assertEquals(2, algorithms.length);
		Assert.assertTrue(algorithms[0].equals("TLS_DHE_RSA_WITH_AES_128_CBC_SHA") || algorithms[0].equals("TLS_DHE_RSA_WITH_AES_128_CBC_SHA256"));
		Assert.assertTrue(algorithms[1].equals("TLS_DHE_RSA_WITH_AES_128_CBC_SHA") || algorithms[1].equals("TLS_DHE_RSA_WITH_AES_128_CBC_SHA256"));
	}

	// ------------------------------- utils ----------------------------------

	public static Configuration createSslConfigWithKeyStore() {
		final Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_ENABLED, true);
		addKeyStoreConfig(config);
		return config;
	}

	public static Configuration createSslConfigWithTrustStore() {
		final Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_ENABLED, true);
		addTrustStoreConfig(config);
		return config;
	}

	public static Configuration createSslConfigWithKeyAndTrustStores() {
		final Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_ENABLED, true);
		addKeyStoreConfig(config);
		addTrustStoreConfig(config);
		return config;
	}

	private static void addKeyStoreConfig(Configuration config) {
		config.setString(SecurityOptions.SSL_KEYSTORE, KEY_STORE_PATH);
		config.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, KEY_STORE_PASSWORD);
		config.setString(SecurityOptions.SSL_KEY_PASSWORD, KEY_PASSWORD);
	}

	private static void addTrustStoreConfig(Configuration config) {
		config.setString(SecurityOptions.SSL_TRUSTSTORE, TRUST_STORE_PATH);
		config.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, TRUST_STORE_PASSWORD);
	}
}
