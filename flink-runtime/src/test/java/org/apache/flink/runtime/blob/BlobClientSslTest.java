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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class contains unit tests for the {@link BlobClient} with ssl enabled.
 */
public class BlobClientSslTest {

	/** The buffer size used during the tests in bytes. */
	private static final int TEST_BUFFER_SIZE = 17 * 1000;

	/** The instance of the SSL BLOB server used during the tests. */
	private static BlobServer BLOB_SSL_SERVER;

	/** The SSL blob service client configuration */
	private static Configuration sslClientConfig;

	/** The instance of the non-SSL BLOB server used during the tests. */
	private static BlobServer BLOB_SERVER;

	/** The non-ssl blob service client configuration */
	private static Configuration clientConfig;

	/**
	 * Starts the SSL enabled BLOB server.
	 */
	@BeforeClass
	public static void startSSLServer() {
		try {
			Configuration config = new Configuration();
			config.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
			config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/local127.keystore");
			config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "password");
			config.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "password");
			BLOB_SSL_SERVER = new BlobServer(config);
		}
		catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		sslClientConfig = new Configuration();
		sslClientConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		sslClientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		sslClientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password");
	}

	/**
	 * Starts the SSL disabled BLOB server.
	 */
	@BeforeClass
	public static void startNonSSLServer() {
		try {
			Configuration config = new Configuration();
			config.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
			config.setBoolean(ConfigConstants.BLOB_SERVICE_SSL_ENABLED, false);
			config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE, "src/test/resources/local127.keystore");
			config.setString(ConfigConstants.SECURITY_SSL_KEYSTORE_PASSWORD, "password");
			config.setString(ConfigConstants.SECURITY_SSL_KEY_PASSWORD, "password");
			BLOB_SERVER = new BlobServer(config);
		}
		catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		clientConfig = new Configuration();
		clientConfig.setBoolean(ConfigConstants.SECURITY_SSL_ENABLED, true);
		clientConfig.setBoolean(ConfigConstants.BLOB_SERVICE_SSL_ENABLED, false);
		clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE, "src/test/resources/local127.truststore");
		clientConfig.setString(ConfigConstants.SECURITY_SSL_TRUSTSTORE_PASSWORD, "password");
	}

	/**
	 * Shuts the BLOB server down.
	 */
	@AfterClass
	public static void stopServers() {
		if (BLOB_SSL_SERVER != null) {
			BLOB_SSL_SERVER.shutdown();
		}

		if (BLOB_SERVER != null) {
			BLOB_SERVER.shutdown();
		}
	}

	/**
	 * Prepares a test file for the unit tests, i.e. the methods fills the file with a particular byte patterns and
	 * computes the file's BLOB key.
	 *
	 * @param file
	 *        the file to prepare for the unit tests
	 * @return the BLOB key of the prepared file
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing to the test file
	 */
	private static BlobKey prepareTestFile(File file) throws IOException {

		MessageDigest md = BlobUtils.createMessageDigest();

		final byte[] buf = new byte[TEST_BUFFER_SIZE];
		for (int i = 0; i < buf.length; ++i) {
			buf[i] = (byte) (i % 128);
		}

		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(file);

			for (int i = 0; i < 20; ++i) {
				fos.write(buf);
				md.update(buf);
			}

		} finally {
			if (fos != null) {
				fos.close();
			}
		}

		return new BlobKey(md.digest());
	}

	/**
	 * Validates the result of a GET operation by comparing the data from the retrieved input stream to the content of
	 * the specified file.
	 *
	 * @param inputStream
	 *        the input stream returned from the GET operation
	 * @param file
	 *        the file to compare the input stream's data to
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the input stream or the file
	 */
	private static void validateGet(final InputStream inputStream, final File file) throws IOException {

		InputStream inputStream2 = null;
		try {

			inputStream2 = new FileInputStream(file);

			while (true) {

				final int r1 = inputStream.read();
				final int r2 = inputStream2.read();

				assertEquals(r2, r1);

				if (r1 < 0) {
					break;
				}
			}

		} finally {
			if (inputStream2 != null) {
				inputStream2.close();
			}
		}

	}

	/**
	 * Tests the PUT/GET operations for content-addressable streams.
	 */
	@Test
	public void testContentAddressableStream() {

		BlobClient client = null;
		InputStream is = null;

		try {
			File testFile = File.createTempFile("testfile", ".dat");
			testFile.deleteOnExit();

			BlobKey origKey = prepareTestFile(testFile);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", BLOB_SSL_SERVER.getPort());
			client = new BlobClient(serverAddress, sslClientConfig);

			// Store the data
			is = new FileInputStream(testFile);
			BlobKey receivedKey = client.put(is);
			assertEquals(origKey, receivedKey);

			is.close();
			is = null;

			// Retrieve the data
			is = client.get(receivedKey);
			validateGet(is, testFile);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (is != null) {
				try {
					is.close();
				} catch (Throwable t) {}
			}
			if (client != null) {
				try {
					client.close();
				} catch (Throwable t) {}
			}
		}
	}

	/**
	 * Tests the PUT/GET operations for regular (non-content-addressable) streams.
	 */
	@Test
	public void testRegularStream() {

		final JobID jobID = JobID.generate();
		final String key = "testkey3";

		try {
			final File testFile = File.createTempFile("testfile", ".dat");
			testFile.deleteOnExit();
			prepareTestFile(testFile);

			BlobClient client = null;
			InputStream is = null;
			try {

				final InetSocketAddress serverAddress = new InetSocketAddress("localhost", BLOB_SSL_SERVER.getPort());
				client = new BlobClient(serverAddress, sslClientConfig);

				// Store the data
				is = new FileInputStream(testFile);
				client.put(jobID, key, is);

				is.close();
				is = null;

				// Retrieve the data
				is = client.get(jobID, key);
				validateGet(is, testFile);

			}
			finally {
				if (is != null) {
					is.close();
				}
				if (client != null) {
					client.close();
				}
			}

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests the static {@link BlobClient#uploadJarFiles(InetSocketAddress, Configuration, List)} helper.
	 */
	private void uploadJarFile(BlobServer blobServer, Configuration blobClientConfig) throws Exception {
		final File testFile = File.createTempFile("testfile", ".dat");
		testFile.deleteOnExit();
		prepareTestFile(testFile);

		InetSocketAddress serverAddress = new InetSocketAddress("localhost", blobServer.getPort());

		List<BlobKey> blobKeys = BlobClient.uploadJarFiles(serverAddress, blobClientConfig,
			Collections.singletonList(new Path(testFile.toURI())));

		assertEquals(1, blobKeys.size());

		try (BlobClient blobClient = new BlobClient(serverAddress, blobClientConfig)) {
			InputStream is = blobClient.get(blobKeys.get(0));
			validateGet(is, testFile);
		}
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
