package org.apache.flink.runtime.blob;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobID;

public final class BlobServer extends Thread {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(BlobServer.class);

	static final int BUFFER_SIZE = 4096;

	static final int MAX_KEY_LENGTH = 64;

	static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");

	static final byte PUT_OPERATION = 0;

	static final byte GET_OPERATION = 1;

	/**
	 * Algorithm to be used for calculating the BLOB keys.
	 */
	private static final String HASHING_ALGORITHM = "SHA-1";

	private static final AtomicInteger TEMP_FILE_COUNTER = new AtomicInteger(0);

	private static File STORAGE_DIRECTORY = null;

	private static File INCOMING_DIRECTORY = null;

	private static File CACHE_DIRECTORY = null;

	private final ServerSocket serverSocket;

	public BlobServer() throws IOException {

		this.serverSocket = new ServerSocket(0);
		start();

		if (LOG.isInfoEnabled()) {
			LOG.info(String.format("Started BLOB server on port %d",
				this.serverSocket.getLocalPort()));
		}
	}

	public int getServerPort() {

		return this.serverSocket.getLocalPort();
	}

	/**
	 * Creates a new instance of the message digest to use for the BLOB key computation.
	 * 
	 * @return a new instance of the message digest to use for the BLOB key computation
	 */
	static MessageDigest createMessageDigest() {

		try {
			return MessageDigest.getInstance(HASHING_ALGORITHM);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
	}

	private static File getStorageDirectory() {

		if (STORAGE_DIRECTORY != null) {
			return STORAGE_DIRECTORY;
		}

		File storageDirectory;
		String sd = GlobalConfiguration.getString(
			ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY, null);
		if (sd != null) {
			storageDirectory = new File(sd);
		} else {
			storageDirectory = new File(System.getProperty("java.io.tmpdir"));
		}

		storageDirectory = new File(storageDirectory, String.format(
			"blobStore-%s", System.getProperty("user.name")));

		// Create the storage directory
		storageDirectory.mkdirs();
		STORAGE_DIRECTORY = storageDirectory;

		return storageDirectory;
	}

	static File getTemporaryFilename() {

		return new File(getIncomingDirectory(), String.format("temp-%08d", TEMP_FILE_COUNTER.getAndIncrement()));
	}

	private static File getIncomingDirectory() {

		if (INCOMING_DIRECTORY != null) {
			return INCOMING_DIRECTORY;
		}

		final File storageDirectory = getStorageDirectory();
		final File incomingDirectory = new File(storageDirectory, "incoming");
		incomingDirectory.mkdir();
		INCOMING_DIRECTORY = incomingDirectory;

		return incomingDirectory;
	}

	private static File getCacheDirectory() {

		if (CACHE_DIRECTORY != null) {
			return CACHE_DIRECTORY;
		}

		final File storageDirectory = getStorageDirectory();
		final File cacheDirectory = new File(storageDirectory, "cache");
		cacheDirectory.mkdir();
		CACHE_DIRECTORY = cacheDirectory;

		return CACHE_DIRECTORY;
	}

	private static File getJobDirectory(final JobID jobID) {

		final File jobDirectory = new File(getStorageDirectory(), "job_" + jobID.toString());
		jobDirectory.mkdirs();

		return jobDirectory;
	}

	public static File getStorageLocation(final BlobKey key) {

		final File storageDirectory = getCacheDirectory();

		return new File(storageDirectory, key.toString());
	}

	public static File getStorageLocation(final JobID jobID, final String key) {

		// TODO: Reencode key

		return new File(getJobDirectory(jobID), key);
	}

	@Override
	public void run() {

		try {

			while (true) {
				new BlobConnection(this.serverSocket.accept()).start();
			}

		} catch (IOException ioe) {
			if (LOG.isErrorEnabled()) {
				LOG.error(ioe);
			}
		}
	}

	/**
	 * Auxiliary method to write the length of an upcoming data chunk to an
	 * output stream.
	 * 
	 * @param length
	 *        the length of the upcoming data chunk in bytes
	 * @param buf
	 *        the byte buffer to use for the integer serialization
	 * @param outputStream
	 *        the output stream to write the length to
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing to the output
	 *         stream
	 */
	static void writeLength(final int length, final byte[] buf,
			final OutputStream outputStream) throws IOException {

		buf[0] = (byte) (length & 0xff);
		buf[1] = (byte) ((length >> 8) & 0xff);
		buf[2] = (byte) ((length >> 16) & 0xff);
		buf[3] = (byte) ((length >> 24) & 0xff);

		outputStream.write(buf, 0, 4);
	}

	/**
	 * Auxiliary method to read the length of an upcoming data chunk from an
	 * input stream.
	 * 
	 * @param buf
	 *        the byte buffer to use for the integer deserialization
	 * @param inputStream
	 *        the input stream to read the length from
	 * @return the length of the upcoming data chunk in bytes
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading from the input
	 *         stream
	 */
	static int readLength(final byte[] buf, final InputStream inputStream)
			throws IOException {

		int bytesRead = 0;
		while (bytesRead < 4) {
			final int read = inputStream.read(buf, bytesRead, 4 - bytesRead);
			if (read < 0) {
				throw new EOFException();
			}
			bytesRead += read;
		}

		bytesRead = buf[0] & 0xff;
		bytesRead |= (buf[1] & 0xff) << 8;
		bytesRead |= (buf[2] & 0xff) << 16;
		bytesRead |= (buf[3] & 0xff) << 24;

		return bytesRead;
	}

	static void readFully(final InputStream inputStream,
			final byte[] buf, final int off, final int len) throws IOException {

		int bytesRead = 0;
		while (bytesRead < len) {

			final int read = inputStream.read(buf, off + bytesRead, len
				- bytesRead);
			if (read < 0) {
				throw new EOFException();
			}
			bytesRead += read;
		}
	}

	public void shutDown() {

		// TODO: Implement me
	}
}
