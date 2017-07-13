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
import java.util.Random;

import org.apache.flink.api.common.JobID;

import javax.annotation.Nullable;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * This class contains unit tests for the {@link BlobClient}.
 */
public class BlobClientTest {

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
	 * @param actualInputStream
	 *        the input stream returned from the GET operation
	 * @param expectedBuf
	 *        the buffer to compare the input stream's data to
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the input stream
	 */
	static void validateGet(final InputStream actualInputStream, final byte[] expectedBuf)
			throws IOException {

		byte[] receivedBuffer = new byte[expectedBuf.length];

		int bytesReceived = 0;

		while (true) {

			final int read = actualInputStream.read(receivedBuffer, bytesReceived, receivedBuffer.length - bytesReceived);
			if (read < 0) {
				throw new EOFException();
			}
			bytesReceived += read;

			if (bytesReceived == receivedBuffer.length) {
				assertEquals(-1, actualInputStream.read());
				assertArrayEquals(expectedBuf, receivedBuffer);
				return;
			}
		}
	}

	/**
	 * Validates the result of a GET operation by comparing the data from the retrieved input stream to the content of
	 * the expected input stream.
	 *
	 * @param actualInputStream
	 *        the input stream returned from the GET operation
	 * @param expectedInputStream
	 *        the input stream to compare the input stream's data to
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading any input stream
	 */
	static void validateGet(InputStream actualInputStream, InputStream expectedInputStream)
			throws IOException {

		while (true) {
			final int r1 = actualInputStream.read();
			final int r2 = expectedInputStream.read();

			assertEquals(r2, r1);

			if (r1 < 0) {
				break;
			}
		}
	}

	/**
	 * Validates the result of a GET operation by comparing the data from the retrieved input stream to the content of
	 * the specified file.
	 * 
	 * @param actualInputStream
	 *        the input stream returned from the GET operation
	 * @param expectedFile
	 *        the file to compare the input stream's data to
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the input stream or the file
	 */
	@SuppressWarnings("WeakerAccess")
	static void validateGet(final InputStream actualInputStream, final File expectedFile) throws IOException {
		try (InputStream inputStream2 = new FileInputStream(expectedFile)) {
			validateGet(actualInputStream, inputStream2);
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

			// Store the data (job-unrelated)
			BlobKey receivedKey = client.putBuffer(null, testBuffer, 0, testBuffer.length);
			assertEquals(origKey, receivedKey);

			// try again with a job-related BLOB:
			receivedKey = client.putBuffer(jobId, testBuffer, 0, testBuffer.length);
			assertEquals(origKey, receivedKey);

			// Retrieve the data (job-unrelated)
			InputStream is = client.getInternal(null, receivedKey);
			validateGet(is, testBuffer);
			// job-related
			is = client.getInternal(jobId, receivedKey);
			validateGet(is, testBuffer);

			// Check reaction to invalid keys for job-unrelated blobs
			try {
				client.getInternal(null, new BlobKey());
				fail("Expected IOException did not occur");
			}
			catch (IOException fnfe) {
				// expected
			}

			// Check reaction to invalid keys for job-related blobs
			// new client needed (closed from failure above)
			client = new BlobClient(serverAddress, getBlobClientConfig());
			try {
				client.getInternal(jobId, new BlobKey());
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

		File testFile = temporaryFolder.newFile();
		BlobKey origKey = prepareTestFile(testFile);

		InputStream is = null;

		try (BlobClient client = new BlobClient(new InetSocketAddress("localhost", getBlobServer().getPort()), getBlobClientConfig())) {

			JobID jobId = new JobID();

			// Store the data (job-unrelated)
			is = new FileInputStream(testFile);
			BlobKey receivedKey = client.putInputStream(null, is);
			assertEquals(origKey, receivedKey);

			// try again with a job-related BLOB:
			is = new FileInputStream(testFile);
			receivedKey = client.putInputStream(jobId, is);
			assertEquals(origKey, receivedKey);

			is.close();
			is = null;

			// Retrieve the data (job-unrelated)
			is = client.getInternal(null, receivedKey);
			validateGet(is, testFile);
			// job-related
			is = client.getInternal(jobId, receivedKey);
			validateGet(is, testFile);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (Throwable ignored) {}
			}
		}
	}

	@Test
	public void testGetFailsDuringStreamingNoJob() throws IOException {
		testGetFailsDuringStreaming(null);
	}

	@Test
	public void testGetFailsDuringStreamingForJob() throws IOException {
		testGetFailsDuringStreaming(new JobID());
	}

	/**
	 * Checks the correct result if a GET operation fails during the file download.
	 *
	 * @param jobId job ID or <tt>null</tt> if job-unrelated
	 */
	private void testGetFailsDuringStreaming(@Nullable final JobID jobId) throws IOException {

		try (BlobClient client = new BlobClient(
			new InetSocketAddress("localhost", getBlobServer().getPort()), getBlobClientConfig())) {

			byte[] data = new byte[5000000];
			Random rnd = new Random();
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = client.putBuffer(jobId, data, 0, data.length);
			assertNotNull(key);

			// issue a GET request that succeeds
			InputStream is = client.getInternal(jobId, key);

			byte[] receiveBuffer = new byte[data.length];
			int firstChunkLen = 50000;
			BlobUtils.readFully(is, receiveBuffer, 0, firstChunkLen, null);
			BlobUtils.readFully(is, receiveBuffer, firstChunkLen, firstChunkLen, null);

			// shut down the server
			for (BlobServerConnection conn : getBlobServer().getCurrentActiveConnections()) {
				conn.close();
			}

			try {
				BlobUtils.readFully(is, receiveBuffer, 2 * firstChunkLen, data.length - 2 * firstChunkLen, null);
				// we tolerate that this succeeds, as the receiver socket may have buffered
				// everything already, but in this case, also verify the contents
				assertArrayEquals(data, receiveBuffer);
			}
			catch (IOException e) {
				// expected
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
			InputStream is = blobClient.getInternal(jobId, blobKeys.get(0));
			validateGet(is, testFile);
		}
	}
}
