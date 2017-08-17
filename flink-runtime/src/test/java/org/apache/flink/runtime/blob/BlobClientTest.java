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

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.TestLogger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This class contains unit tests for the {@link BlobClient}.
 */
public class BlobClientTest extends TestLogger {

	/** The buffer size used during the tests in bytes. */
	private static final int TEST_BUFFER_SIZE = 17 * 1000;

	/** The instance of the (non-ssl) BLOB server used during the tests. */
	static BlobServer BLOB_SERVER;

	/** The blob service (non-ssl) client configuration */
	static Configuration clientConfig;

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Starts the BLOB server.
	 */
	@BeforeClass
	public static void startServer() throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		BLOB_SERVER = new BlobServer(config, new VoidBlobStore());

		clientConfig = new Configuration();
	}

	/**
	 * Shuts the BLOB server down.
	 */
	@AfterClass
	public static void stopServer() throws IOException {
		if (BLOB_SERVER != null) {
			BLOB_SERVER.close();
		}
	}

	/**
	 * Creates a test buffer and fills it with a specific byte pattern.
	 * 
	 * @return a test buffer filled with a specific byte pattern
	 */
	private static byte[] createTestBuffer() {
		final byte[] buf = new byte[TEST_BUFFER_SIZE];
		for (int i = 0; i < buf.length; ++i) {
			buf[i] = (byte) (i % 128);
		}
		return buf;
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
	 * the specified buffer.
	 * 
	 * @param inputStream
	 *        the input stream returned from the GET operation (will be closed by this method)
	 * @param buf
	 *        the buffer to compare the input stream's data to
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the input stream
	 */
	static void validateGetAndClose(final InputStream inputStream, final byte[] buf) throws IOException {
		try {
			byte[] receivedBuffer = new byte[buf.length];

			int bytesReceived = 0;

			while (true) {

				final int read = inputStream
					.read(receivedBuffer, bytesReceived, receivedBuffer.length - bytesReceived);
				if (read < 0) {
					throw new EOFException();
				}
				bytesReceived += read;

				if (bytesReceived == receivedBuffer.length) {
					assertEquals(-1, inputStream.read());
					assertArrayEquals(buf, receivedBuffer);
					return;
				}
			}
		} finally {
			inputStream.close();
		}
	}

	/**
	 * Validates the result of a GET operation by comparing the data from the retrieved input stream to the content of
	 * the specified file.
	 * 
	 * @param inputStream
	 *        the input stream returned from the GET operation (will be closed by this method)
	 * @param file
	 *        the file to compare the input stream's data to
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the input stream or the file
	 */
	private static void validateGetAndClose(final InputStream inputStream, final File file) throws IOException {

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
			inputStream.close();
		}

	}

	/**
	 * Tests the PUT/GET operations for content-addressable buffers.
	 */
	@Test
	public void testContentAddressableBuffer() throws IOException {

		BlobClient client = null;

		try {
			byte[] testBuffer = createTestBuffer();
			MessageDigest md = BlobUtils.createMessageDigest();
			md.update(testBuffer);
			BlobKey origKey = new BlobKey(md.digest());

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", getBlobServer().getPort());
			client = new BlobClient(serverAddress, getBlobClientConfig());

			JobID jobId = new JobID();

			// Store the data
			BlobKey receivedKey = client.put(null, testBuffer);
			assertEquals(origKey, receivedKey);
			// try again with a job-related BLOB:
			receivedKey = client.put(jobId, testBuffer);
			assertEquals(origKey, receivedKey);

			// Retrieve the data
			validateGetAndClose(client.get(receivedKey), testBuffer);
			validateGetAndClose(client.get(jobId, receivedKey), testBuffer);

			// Check reaction to invalid keys
			try (InputStream ignored = client.get(new BlobKey())) {
				fail("Expected IOException did not occur");
			}
			catch (IOException fnfe) {
				// expected
			}
			// new client needed (closed from failure above)
			client = new BlobClient(serverAddress, getBlobClientConfig());
			try (InputStream ignored = client.get(jobId, new BlobKey())) {
				fail("Expected IOException did not occur");
			}
			catch (IOException fnfe) {
				// expected
			}
		}
		finally {
			if (client != null) {
				try {
					client.close();
				} catch (Throwable ignored) {}
			}
		}
	}

	protected Configuration getBlobClientConfig() {
		return clientConfig;
	}

	protected BlobServer getBlobServer() {
		return BLOB_SERVER;
	}

	/**
	 * Tests the PUT/GET operations for content-addressable streams.
	 */
	@Test
	public void testContentAddressableStream() throws IOException {

		BlobClient client = null;
		InputStream is = null;

		try {
			File testFile = File.createTempFile("testfile", ".dat");
			testFile.deleteOnExit();

			BlobKey origKey = prepareTestFile(testFile);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", getBlobServer().getPort());
			client = new BlobClient(serverAddress, getBlobClientConfig());

			JobID jobId = new JobID();

			// Store the data
			is = new FileInputStream(testFile);
			BlobKey receivedKey = client.put(is);
			assertEquals(origKey, receivedKey);
			// try again with a job-related BLOB:
			is = new FileInputStream(testFile);
			receivedKey = client.put(jobId, is);
			assertEquals(origKey, receivedKey);

			is.close();
			is = null;

			// Retrieve the data
			validateGetAndClose(client.get(receivedKey), testFile);
			validateGetAndClose(client.get(jobId, receivedKey), testFile);
		}
		finally {
			if (is != null) {
				try {
					is.close();
				} catch (Throwable ignored) {}
			}
			if (client != null) {
				try {
					client.close();
				} catch (Throwable ignored) {}
			}
		}
	}

	/**
	 * Tests the static {@link BlobClient#uploadJarFiles(InetSocketAddress, Configuration, JobID, List)} helper.
	 */
	@Test
	public void testUploadJarFilesHelper() throws Exception {
		uploadJarFile(getBlobServer(), getBlobClientConfig());
	}

	/**
	 * Tests the static {@link BlobClient#uploadJarFiles(InetSocketAddress, Configuration, JobID, List)}} helper.
	 */
	static void uploadJarFile(BlobServer blobServer, Configuration blobClientConfig) throws Exception {
		final File testFile = File.createTempFile("testfile", ".dat");
		testFile.deleteOnExit();
		prepareTestFile(testFile);

		InetSocketAddress serverAddress = new InetSocketAddress("localhost", blobServer.getPort());

		uploadJarFile(serverAddress, blobClientConfig, testFile);
		uploadJarFile(serverAddress, blobClientConfig, testFile);
	}

	private static void uploadJarFile(
			final InetSocketAddress serverAddress, final Configuration blobClientConfig,
			final File testFile) throws IOException {
		JobID jobId = new JobID();
		List<BlobKey> blobKeys = BlobClient.uploadJarFiles(serverAddress, blobClientConfig,
			jobId, Collections.singletonList(new Path(testFile.toURI())));

		assertEquals(1, blobKeys.size());

		try (BlobClient blobClient = new BlobClient(serverAddress, blobClientConfig)) {
			validateGetAndClose(blobClient.get(jobId, blobKeys.get(0)), testFile);
		}
	}
}
