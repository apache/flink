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

package org.apache.flink.runtime.blob;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.MessageDigest;

import static org.junit.Assert.fail;

public class BlobClientCookieTest extends BlobClientTest {

	/**
	 * Starts the BLOB server with secure cookie enabled configuration
	 */
	@BeforeClass
	public static void startServer() {
		try {
			config.setBoolean(ConfigConstants.SECURITY_ENABLED, true);
			config.setString(ConfigConstants.SECURITY_COOKIE, "foo");

			config.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
			config.setBoolean(ConfigConstants.BLOB_SERVICE_SSL_ENABLED, false);
			config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/local127.keystore");
			config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "password");
			config.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "password");

			BLOB_SERVER = new BlobServer(config);


			clientConfig.setString(ConfigConstants.SECURITY_COOKIE, "foo");
			clientConfig.setBoolean(ConfigConstants.SECURITY_ENABLED, true);

			clientConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
			clientConfig.setBoolean(ConfigConstants.BLOB_SERVICE_SSL_ENABLED, false);
			clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
			clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password");
		}
		catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test(expected = IOException.class)
	public void testInvalidSecurityCookie() throws IOException {

		BlobClient client = null;

		try {
			byte[] testBuffer = createTestBuffer();
			MessageDigest md = BlobUtils.createMessageDigest();
			md.update(testBuffer);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", BLOB_SERVER.getPort());

			Configuration clientConfig = new Configuration();
			clientConfig.setString(ConfigConstants.SECURITY_COOKIE, "bar");
			clientConfig.setBoolean(ConfigConstants.SECURITY_ENABLED, true);

			clientConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
			clientConfig.setBoolean(ConfigConstants.BLOB_SERVICE_SSL_ENABLED, false);
			clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
			clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password");

			client = new BlobClient(serverAddress, clientConfig);

			// Store some data
			client.put(testBuffer);
		}
		finally {
			if (client != null) {
				try {
					client.close();
				} catch (Throwable t) {}
			}
		}
	}

}