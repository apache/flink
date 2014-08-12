package org.apache.flink.runtime.blob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.MessageDigest;

import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BlobClientTest {

	private static final int TEST_BUFFER_SIZE = 17 * 1000;

	private static BlobServer BLOB_SERVER;

	@BeforeClass
	public static void startServer() {

		try {
			BLOB_SERVER = new BlobServer();
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

	}

	@AfterClass
	public static void stopServer() {

		if (BLOB_SERVER != null) {
			BLOB_SERVER.shutDown();
		}
	}

	private static byte[] createTestBuffer() {

		final byte[] buf = new byte[TEST_BUFFER_SIZE];

		for (int i = 0; i < buf.length; ++i) {
			buf[i] = (byte) (i % 128);
		}

		return buf;
	}

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

	@Test
	public void testContentAddressableBuffer() {

		final byte[] testBuffer = createTestBuffer();
		final MessageDigest md = BlobServer.createMessageDigest();
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

	@Test
	public void testRegularBuffer() {

		final byte[] testBuffer = createTestBuffer();
		final JobID jobID = JobID.generate();
		final String key = "testkey";

		System.out.println("Job ID is " + jobID);

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

				// Check reaction to invalid keys
				try {
					client.get(jobID, "testkey2");
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
}
