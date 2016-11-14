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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLContext;

/*
 * Tests for the SSL utilities
 */
public class SSLUtilsTest {

	/**
	 * Tests if SSL Client Context is created given a valid SSL configuration
	 */
	@Test
	public void testCreateSSLClientContext() throws Exception {

		Configuration clientConfig = new Configuration();
		clientConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password");

		SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
		Assert.assertNotNull(clientContext);
	}

	/**
	 * Tests if SSL Client Context is not created if SSL is not configured
	 */
	@Test
	public void testCreateSSLClientContextWithSSLDisabled() throws Exception {

		Configuration clientConfig = new Configuration();
		clientConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, false);

		SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
		Assert.assertNull(clientContext);
	}

	/**
	 * Tests if SSL Client Context creation fails with bad SSL configuration
	 */
	@Test
	public void testCreateSSLClientContextMisconfiguration() {

		Configuration clientConfig = new Configuration();
		clientConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "badpassword");

		try {
			SSLContext clientContext = SSLUtils.createSSLClientContext(clientConfig);
			Assert.fail("SSL client context created even with bad SSL configuration ");
		} catch (Exception e) {
			// Exception here is valid
		}
	}

	/**
	 * Tests if SSL Server Context is created given a valid SSL configuration
	 */
	@Test
	public void testCreateSSLServerContext() throws Exception {

		Configuration serverConfig = new Configuration();
		serverConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		serverConfig.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/local127.keystore");
		serverConfig.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "password");
		serverConfig.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "password");

		SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
		Assert.assertNotNull(serverContext);
	}

	/**
	 * Tests if SSL Server Context is not created if SSL is disabled
	 */
	@Test
	public void testCreateSSLServerContextWithSSLDisabled() throws Exception {

		Configuration serverConfig = new Configuration();
		serverConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, false);

		SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
		Assert.assertNull(serverContext);
	}

	/**
	 * Tests if SSL Server Context creation fails with bad SSL configuration
	 */
	@Test
	public void testCreateSSLServerContextMisconfiguration() {

		Configuration serverConfig = new Configuration();
		serverConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		serverConfig.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/local127.keystore");
		serverConfig.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "badpassword");
		serverConfig.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "badpassword");

		try {
			SSLContext serverContext = SSLUtils.createSSLServerContext(serverConfig);
			Assert.fail("SSL server context created even with bad SSL configuration ");
		} catch (Exception e) {
			// Exception here is valid
		}
	}

}
