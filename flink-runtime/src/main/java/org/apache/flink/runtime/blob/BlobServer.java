package org.apache.flink.runtime.blob;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.jobgraph.JobID;

import com.google.common.io.BaseEncoding;

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

	private final ServerSocket serverSocket;

	private final Path storageDirectory;

	private static final class Connection extends Thread {

		private final Socket clientSocket;

		private final Path storageDirectory;

		private Connection(final Socket clientSocket,
				final Path storageDirectory) {
			super("BLOB connection for "
					+ clientSocket.getRemoteSocketAddress().toString());

			this.clientSocket = clientSocket;
			this.storageDirectory = storageDirectory;
		}

		@Override
		public void run() {

			try {

				final InputStream inputStream = this.clientSocket
						.getInputStream();

				while (true) {

					// Read the requested operation
					final int operation = inputStream.read();
					if (operation < 0) {
						return;
					}

					// Read the job ID
					final byte[] buf = new byte[JobID.SIZE];
					readFully(inputStream, buf, 0, buf.length);
					final JobID jobID = JobID.fromByteArray(buf);

					switch (operation) {
					case BlobServer.PUT_OPERATION:
						put(jobID, inputStream, this.storageDirectory);
						break;
					case BlobServer.GET_OPERATION:
						break;
					default:
						throw new IOException("Unknown operation " + operation);
					}
				}

			} catch (IOException ioe) {
				if (LOG.isErrorEnabled()) {
					LOG.error(ioe);
				}
			} finally {
				closeSilently(this.clientSocket);
			}
		}
	}

	public BlobServer() throws IOException {

		this.serverSocket = new ServerSocket(0);
		this.storageDirectory = createStorageDirectory();
		start();

		if (LOG.isInfoEnabled()) {
			LOG.info(String.format("Started BLOB server on port %d",
					this.serverSocket.getLocalPort()));
		}
	}

	private static final Path createStorageDirectory() throws IOException {

		Path storageDirectory;
		String sd = GlobalConfiguration.getString(
				ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY, null);
		if (sd != null) {
			storageDirectory = new Path(sd);
		} else {
			storageDirectory = new Path(LocalFileSystem.getLocalFileSystem()
					.getUri());
			storageDirectory = new Path(storageDirectory,
					System.getProperty("java.io.tmpdir"));
		}

		storageDirectory = new Path(storageDirectory, String.format(
				"blobStore-%s", System.getProperty("user.name")));

		// Create the storage directory
		final FileSystem fs = storageDirectory.getFileSystem();
		fs.mkdirs(storageDirectory);

		return storageDirectory;
	}

	public int getServerPort() {

		return this.serverSocket.getLocalPort();
	}

	private static void put(final JobID jobID, final InputStream inputStream,
			final Path storageDirectory) throws IOException {

		byte[] buf = new byte[BUFFER_SIZE];

		// Read the key
		final String key = readKey(buf, inputStream);

		final int valueLength = readLength(buf, inputStream);

		final Path jobPath = new Path(storageDirectory, jobID.toString());
		final Path blobPath = new Path(jobPath, key);

		final FileSystem fs = blobPath.getFileSystem();
		FSDataOutputStream fos = null;

		try {

			fos = fs.create(blobPath, true);

			int bytesReceived = 0;
			while (bytesReceived < valueLength) {
				final int read = inputStream.read(buf, 0,
						Math.min(BUFFER_SIZE, valueLength - bytesReceived));
				if (read < 0) {
					throw new EOFException();
				}
				bytesReceived += read;
				fos.write(buf, 0, read);
			}

		} finally {
			if (fos != null) {
				fos.close();
			}
		}

	}

	private static void get() throws IOException {

	}

	static void closeSilently(final Closeable closeable) {

		try {
			if (closeable != null) {
				closeable.close();
			}
		} catch (IOException ioe) {
		}
	}

	@Override
	public void run() {

		try {

			while (true) {
				new Connection(this.serverSocket.accept(),
						this.storageDirectory).start();
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
	 *            the length of the upcoming data chunk in bytes
	 * @param buf
	 *            the byte buffer to use for the integer serialization
	 * @param outputStream
	 *            the output stream to write the length to
	 * @throws IOException
	 *             thrown if an I/O error occurs while writing to the output
	 *             stream
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
	 *            the byte buffer to use for the integer deserialization
	 * @param inputStream
	 *            the input stream to read the length from
	 * @return the length of the upcoming data chunk in bytes
	 * @throws IOException
	 *             thrown if an I/O error occurs while reading from the input
	 *             stream
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

	private static void readFully(final InputStream inputStream,
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

	private static String readKey(final byte[] buf,
			final InputStream inputStream) throws IOException {

		final int keyLength = BlobServer.readLength(buf, inputStream);
		if (keyLength > MAX_KEY_LENGTH) {
			throw new IOException("Unexpected key length " + keyLength);
		}

		readFully(inputStream, buf, 0, keyLength);

		return new String(buf, 0, keyLength, BlobServer.DEFAULT_CHARSET);
	}

	public static void main(final String[] args) throws Exception {

		final BlobServer bs = new BlobServer();

		final byte[] buf = new byte[4096 * 4096];
		for (int i = 0; i < buf.length; ++i) {
			buf[i] = (byte) 'a';
		}

		final JobID jobID = JobID.generate();

		final BlobClient bc = new BlobClient(bs.getServerPort());
		bc.put(jobID, "daniel", buf);
		bc.put(jobID, "daniel2", buf);
		bc.put(jobID, "daniel3", buf);
		bc.close();

		System.out.println("Got here");
	}
}
