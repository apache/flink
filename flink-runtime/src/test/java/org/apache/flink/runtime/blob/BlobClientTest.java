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

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.MessageDigest;

import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This class contains unit tests for the {@link BlobClient}.
 */
public class BlobClientTest {

	/**
	 * The buffer size used during the tests in bytes.
	 */
	private static final int TEST_BUFFER_SIZE = 17 * 1000;

	/**
	 * The instance of the BLOB server used during the tests.
	 */
	private static BlobServer BLOB_SERVER;

	/**
	 * Starts the BLOB server.
	 */
	@BeforeClass
	public static void startServer() {

		try {
			BLOB_SERVER = new BlobServer();
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

	}

	/**
	 * Shuts the BLOB server down.
	 */
	@AfterClass
	public static void stopServer() {

		if (BLOB_SERVER != null) {
			try {
				BLOB_SERVER.shutdown();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
	private static BlobKey prepareTestFile(final File file) throws IOException {

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
	 *        the input stream returned from the GET operation
	 * @param buf
	 *        the buffer to compare the input stream's data to
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the input stream
	 */
	private static void validateGet(final InputStream inputStream, final byte[] buf) throws IOException {

		int bytesReceived = 0;

		while (true) {

			final int read = inputStream.read(buf, bytesReceived, buf.length - bytesReceived);
			if (read < 0) {
				throw new EOFException();
			}
			bytesReceived += read;

			if (bytesReceived == buf.length) {
				assertEquals(-1, inputStream.read());
				return;
			}
		}
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
	 * Tests the PUT/GET operations for content-addressable buffers.
	 */
	@Test
	public void testContentAddressableBuffer() {

		final byte[] testBuffer = createTestBuffer();
		final MessageDigest md = BlobUtils.createMessageDigest();
		md.update(testBuffer);
		final BlobKey origKey = new BlobKey(md.digest());

		try {

			BlobClient client = null;
			try {

				final InetSocketAddress serverAddress = new InetSocketAddress("localhost", BLOB_SERVER.getServerPort());
				client = new BlobClient(serverAddress);

				// Store the data
				final BlobKey receivedKey = client.put(testBuffer);
				assertEquals(origKey, receivedKey);

				// Retrieve the data
				final InputStream is = client.get(receivedKey);
				validateGet(is, testBuffer);

				// Check reaction to invalid keys
				try {
					client.get(new BlobKey());
				} catch (FileNotFoundException fnfe) {
					return;
				}

				fail("Expected FileNotFoundException did not occur");

			} finally {
				if (client != null) {
					client.close();
				}
			}

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}
	}

	/**
	 * Tests the PUT/GET operations for content-addressable streams.
	 */
	@Test
	public void testContentAddressableStream() {

		try {

			final File testFile = File.createTempFile("testfile", ".dat");
			testFile.deleteOnExit();
			final BlobKey origKey = prepareTestFile(testFile);

			BlobClient client = null;
			InputStream is = null;
			try {

				final InetSocketAddress serverAddress = new InetSocketAddress("localhost", BLOB_SERVER.getServerPort());
				client = new BlobClient(serverAddress);

				// Store the data
				is = new FileInputStream(testFile);
				final BlobKey receivedKey = client.put(is);
				assertEquals(origKey, receivedKey);

				is.close();
				is = null;

				// Retrieve the data
				is = client.get(receivedKey);
				validateGet(is, testFile);

			} finally {
				if (is != null) {
					is.close();
				}
				if (client != null) {
					client.close();
				}
			}

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}
	}

	/**
	 * Tests the PUT/GET operations for regular (non-content-addressable) buffers.
	 */
	@Test
	public void testRegularBuffer() {

		final byte[] testBuffer = createTestBuffer();
		final JobID jobID = JobID.generate();
		final String key = "testkey";

		try {

			BlobClient client = null;
			try {

				final InetSocketAddress serverAddress = new InetSocketAddress("localhost", BLOB_SERVER.getServerPort());
				client = new BlobClient(serverAddress);

				// Store the data
				client.put(jobID, key, testBuffer);

				// Retrieve the data
				final InputStream is = client.get(jobID, key);
				validateGet(is, testBuffer);

				// Delete the data
				client.delete(jobID, key);
				
				// Check if the BLOB is still available
				try {
					client.get(jobID, key);
				} catch (FileNotFoundException fnfe) {
					return;
				}

				fail("Expected FileNotFoundException did not occur");

			} finally {
				if (client != null) {
					client.close();
				}
			}

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
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

				final InetSocketAddress serverAddress = new InetSocketAddress("localhost", BLOB_SERVER.getServerPort());
				client = new BlobClient(serverAddress);

				// Store the data
				is = new FileInputStream(testFile);
				client.put(jobID, key, is);

				is.close();
				is = null;

				// Retrieve the data
				is = client.get(jobID, key);
				validateGet(is, testFile);

			} finally {
				if (is != null) {
					is.close();
				}
				if (client != null) {
					client.close();
				}
			}

		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}
	}
}
