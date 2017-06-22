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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;

/**
 * This class contains unit tests for the {@link BlobClient} with ssl enabled.
 */
public class BlobClientSslTest extends BlobClientTest {

	/** The instance of the SSL BLOB server used during the tests. */
	private static BlobServer BLOB_SSL_SERVER;

	/** The SSL blob service client configuration */
	private static Configuration sslClientConfig;

	/**
	 * Starts the SSL enabled BLOB server.
	 */
	@BeforeClass
	public static void startSSLServer() throws IOException {
		Configuration config = new Configuration();
		config.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/local127.keystore");
		config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "password");
		config.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "password");
		BLOB_SSL_SERVER = new BlobServer(config, new VoidBlobStore());


		sslClientConfig = new Configuration();
		sslClientConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		sslClientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		sslClientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password");
	}

	/**
	 * Shuts the BLOB server down.
	 */
	@AfterClass
	public static void stopServers() throws IOException {
		if (BLOB_SSL_SERVER != null) {
			BLOB_SSL_SERVER.close();
		}
	}

	protected Configuration getBlobClientConfig() {
		return sslClientConfig;
	}

	protected BlobServer getBlobServer() {
		return BLOB_SSL_SERVER;
	}

	/**
	 * Verify ssl client to ssl server upload
	 */
	@Test
	public void testUploadJarFilesHelper() throws Exception {
		uploadJarFile(BLOB_SSL_SERVER, sslClientConfig);
	}

	/**
	 * Verify ssl client to non-ssl server failure
	 */
	@Test
	public void testSSLClientFailure() throws Exception {
		try {
			uploadJarFile(BLOB_SERVER, sslClientConfig);
			fail("SSL client connected to non-ssl server");
		} catch (Exception e) {
			// Exception expected
		}
	}

	/**
	 * Verify non-ssl client to ssl server failure
	 */
	@Test
	public void testSSLServerFailure() throws Exception {
		try {
			uploadJarFile(BLOB_SSL_SERVER, clientConfig);
			fail("Non-SSL client connected to ssl server");
		} catch (Exception e) {
			// Exception expected
		}
	}

	/**
	 * Verify non-ssl connection sanity
	 */
	@Test
	public void testNonSSLConnection() throws Exception {
		uploadJarFile(BLOB_SERVER, clientConfig);
	}
}
