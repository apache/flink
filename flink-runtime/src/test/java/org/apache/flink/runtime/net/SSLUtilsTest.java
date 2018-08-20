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
import org.apache.flink.configuration.SecurityOptions;

import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;

import java.net.ServerSocket;
import java.util.Arrays;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link SSLUtils}.
 */
public class SSLUtilsTest {

	private static final String TRUST_STORE_PATH = SSLUtilsTest.class.getResource("/local127.truststore").getFile();
	private static final String KEY_STORE_PATH = SSLUtilsTest.class.getResource("/local127.keystore").getFile();

	private static final String TRUST_STORE_PASSWORD = "password";
	private static final String KEY_STORE_PASSWORD = "password";
	private static final String KEY_PASSWORD = "password";

	/**
	 * Tests if SSL Client Context is created given a valid SSL configuration.
	 */
	@Test
	public void testCreateSSLClientContext() throws Exception {

		Configuration clientConfig = new Configuration();
		clientConfig.setBoolean(SecurityOptions.SSL_ENABLED, true);
		clientConfig.setString(SecurityOptions.SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		clientConfig.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "password");

		SSLUtils.SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
		Assert.assertNotNull(clientContext);
	}

	/**
	 * Tests if SSL Client Context is not created if SSL is not configured.
	 */
	@Test
	public void testCreateSSLClientContextWithSSLDisabled() throws Exception {

		Configuration clientConfig = new Configuration();
		clientConfig.setBoolean(SecurityOptions.SSL_ENABLED, false);

		SSLUtils.SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
		Assert.assertNull(clientContext);
	}

	/**
	 * Tests if SSL Client Context creation fails with bad SSL configuration.
	 */
	@Test
	public void testCreateSSLClientContextMisconfiguration() {

		Configuration clientConfig = new Configuration();
		clientConfig.setBoolean(SecurityOptions.SSL_ENABLED, true);
		clientConfig.setString(SecurityOptions.SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		clientConfig.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "badpassword");

		try {
			SSLUtils.SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
			Assert.fail("SSL client context created even with bad SSL configuration ");
		} catch (Exception e) {
			// Exception here is valid
		}
	}

	/**
	 * Tests if SSL Server Context is created given a valid SSL configuration.
	 */
	@Test
	public void testCreateSSLServerContext() throws Exception {

		Configuration serverConfig = new Configuration();
		serverConfig.setBoolean(SecurityOptions.SSL_ENABLED, true);
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE, "src/test/resources/local127.keystore");
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password");
		serverConfig.setString(SecurityOptions.SSL_KEY_PASSWORD, "password");

		SSLUtils.SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
		Assert.assertNotNull(serverContext);
	}

	/**
	 * Tests if SSL Server Context is not created if SSL is disabled.
	 */
	@Test
	public void testCreateSSLServerContextWithSSLDisabled() throws Exception {

		Configuration serverConfig = new Configuration();
		serverConfig.setBoolean(SecurityOptions.SSL_ENABLED, false);

		SSLUtils.SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
		Assert.assertNull(serverContext);
	}

	/**
	 * Tests if SSL Server Context creation fails with bad SSL configuration.
	 */
	@Test
	public void testCreateSSLServerContextMisconfiguration() {

		Configuration serverConfig = new Configuration();
		serverConfig.setBoolean(SecurityOptions.SSL_ENABLED, true);
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE, "src/test/resources/local127.keystore");
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "badpassword");
		serverConfig.setString(SecurityOptions.SSL_KEY_PASSWORD, "badpassword");

		try {
			SSLUtils.SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
			Assert.fail("SSL server context created even with bad SSL configuration ");
		} catch (Exception e) {
			// Exception here is valid
		}
	}

	/**
	 * Tests if SSL Server Context creation fails with bad SSL configuration.
	 */
	@Test
	public void testCreateSSLServerContextWithMultiProtocols() {

		Configuration serverConfig = new Configuration();
		serverConfig.setBoolean(SecurityOptions.SSL_ENABLED, true);
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE, "src/test/resources/local127.keystore");
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password");
		serverConfig.setString(SecurityOptions.SSL_KEY_PASSWORD, "password");
		serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1,TLSv1.2");

		try {
			SSLUtils.SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
			Assert.fail("SSL server context created even with multiple protocols set ");
		} catch (Exception e) {
			// Exception here is valid
		}
	}

	/**
	 * Tests if SSLUtils set the right ssl version and cipher suites for SSLServerSocket.
	 */
	@Test
	public void testSetSSLVersionAndCipherSuitesForSSLServerSocket() throws Exception {

		Configuration serverConfig = new Configuration();
		serverConfig.setBoolean(SecurityOptions.SSL_ENABLED, true);
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE, "src/test/resources/local127.keystore");
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password");
		serverConfig.setString(SecurityOptions.SSL_KEY_PASSWORD, "password");
		serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1.1");
		serverConfig.setString(SecurityOptions.SSL_ALGORITHMS, "TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_CBC_SHA256");

		SSLUtils.SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
		assertNotNull(serverContext);
		try (ServerSocket socket = serverContext.getSslContext().getServerSocketFactory().createServerSocket(0)) {

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

		Configuration serverConfig = new Configuration();
		serverConfig.setBoolean(SecurityOptions.SSL_ENABLED, true);
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE, "src/test/resources/local127.keystore");
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password");
		serverConfig.setString(SecurityOptions.SSL_KEY_PASSWORD, "password");
		serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1");
		serverConfig.setString(SecurityOptions.SSL_ALGORITHMS, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA,TLS_DHE_RSA_WITH_AES_128_CBC_SHA256");

		SSLUtils.SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
		assertNotNull(serverContext);
		SSLEngine engine = serverContext.getSslContext().createSSLEngine();

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

	/**
	 * Tests that {@link SSLEngineFactory} is created correctly.
	 */
	@Test
	public void testCreateSSLEngineFactory() throws Exception {
		Configuration serverConfig = new Configuration();
		serverConfig.setBoolean(SecurityOptions.SSL_ENABLED, true);
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE, "src/test/resources/local127.keystore");
		serverConfig.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, "password");
		serverConfig.setString(SecurityOptions.SSL_KEY_PASSWORD, "password");
		serverConfig.setString(SecurityOptions.SSL_PROTOCOL, "TLSv1");
		serverConfig.setString(SecurityOptions.SSL_ALGORITHMS, "TLS_DHE_RSA_WITH_AES_128_CBC_SHA,TLS_DHE_RSA_WITH_AES_128_CBC_SHA256");

		final SSLEngineFactory serverSSLEngineFactory = SSLUtils.createServerSSLEngineFactory(serverConfig);
		final SSLEngine sslEngine = serverSSLEngineFactory.createSSLEngine();

		assertThat(
			Arrays.asList(sslEngine.getEnabledProtocols()),
			contains("TLSv1"));
		assertThat(
			Arrays.asList(sslEngine.getEnabledCipherSuites()),
			containsInAnyOrder("TLS_DHE_RSA_WITH_AES_128_CBC_SHA", "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256"));
	}

	public static Configuration createInternalSslConfigWithKeyAndTrustStores() {
		final Configuration config = new Configuration();
		config.setBoolean(SecurityOptions.SSL_ENABLED, true);
		addInternalKeyStoreConfig(config);
		addInternalTrustStoreConfig(config);
		return config;
	}

	private static void addInternalKeyStoreConfig(Configuration config) {
		config.setString(SecurityOptions.SSL_KEYSTORE, KEY_STORE_PATH);
		config.setString(SecurityOptions.SSL_KEYSTORE_PASSWORD, KEY_STORE_PASSWORD);
		config.setString(SecurityOptions.SSL_KEY_PASSWORD, KEY_PASSWORD);
	}

	private static void addInternalTrustStoreConfig(Configuration config) {
		config.setString(SecurityOptions.SSL_TRUSTSTORE, TRUST_STORE_PATH);
		config.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, TRUST_STORE_PASSWORD);
	}
}
