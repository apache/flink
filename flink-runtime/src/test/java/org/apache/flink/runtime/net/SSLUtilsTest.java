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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLServerSocket;

import java.net.ServerSocket;

/**
 * Tests for the {@link SSLUtils}.
 */
public class SSLUtilsTest {

	/**
	 * Tests if SSL Client Context is created given a valid SSL configuration.
	 */
	@Test
	public void testCreateSSLClientContext() throws Exception {

		Configuration clientConfig = new Configuration();
		clientConfig.setBoolean(SecurityOptions.SSL_ENABLED, true);
		clientConfig.setString(SecurityOptions.SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		clientConfig.setString(SecurityOptions.SSL_TRUSTSTORE_PASSWORD, "password");

		SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
		Assert.assertNotNull(clientContext);
	}

	/**
	 * Tests if SSL Client Context is not created if SSL is not configured.
	 */
	@Test
	public void testCreateSSLClientContextWithSSLDisabled() throws Exception {

		Configuration clientConfig = new Configuration();
		clientConfig.setBoolean(SecurityOptions.SSL_ENABLED, false);

		SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
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
			SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
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

		SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
		Assert.assertNotNull(serverContext);
	}

	/**
	 * Tests if SSL Server Context is not created if SSL is disabled.
	 */
	@Test
	public void testCreateSSLServerContextWithSSLDisabled() throws Exception {

		Configuration serverConfig = new Configuration();
		serverConfig.setBoolean(SecurityOptions.SSL_ENABLED, false);

		SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
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
			SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
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
			SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
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

		SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
		ServerSocket socket = null;
		try {
			socket = serverContext.getServerSocketFactory().createServerSocket(0);

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
		} finally {
			if (socket != null) {
				socket.close();
			}
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

}
