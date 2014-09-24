/**
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

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobID;

import com.google.common.io.BaseEncoding;

/**
 * This class implements the BLOB server. The BLOB server is responsible for listening for incoming requests and
 * spawning threads to handle these requests. Furthermore, it takes care of creating the directory structure to store
 * the BLOBs or temporarily cache them.
 * <p>
 * This class is thread-safe.
 */
public final class BlobServer extends Thread {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(BlobServer.class);

	/**
	 * The prefix of all BLOB files stored by the BLOB server.
	 */
	private static final String BLOB_FILE_PREFIX = "blob_";

	/**
	 * The prefix of all job-specific directories created by the BLOB server.
	 */
	private static final String JOB_DIR_PREFIX = "job_";

	/**
	 * A file filter to list all BLOB files in a directory.
	 */
	private static final FileFilter BLOB_FILE_FILTER = new FileFilter() {

		@Override
		public boolean accept(final File pathname) {

			return (pathname.isFile() && pathname.getName().startsWith(BLOB_FILE_PREFIX));
		}
	};

	/**
	 * The buffer size in bytes for network transfers.
	 */
	static final int BUFFER_SIZE = 4096;

	/**
	 * The maximum key length allowed for storing BLOBs.
	 */
	static final int MAX_KEY_LENGTH = 64;

	/**
	 * The default character set to translate between characters and bytes.
	 */
	static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");

	/**
	 * Internal code to identify a PUT operation.
	 */
	static final byte PUT_OPERATION = 0;

	/**
	 * Internal code to identify a GET operation.
	 */
	static final byte GET_OPERATION = 1;

	/**
	 * Internal code to identify a DELETE operation.
	 */
	static final byte DELETE_OPERATION = 2;

	/**
	 * Algorithm to be used for calculating the BLOB keys.
	 */
	private static final String HASHING_ALGORITHM = "SHA-1";

	/**
	 * Counter to generate unique names for temporary files.
	 */
	private static final AtomicInteger TEMP_FILE_COUNTER = new AtomicInteger(0);

	/**
	 * Cache for the base storage directory.
	 */
	private static File STORAGE_DIRECTORY = null;

	/**
	 * Cache for the storage directory for incoming files.
	 */
	private static File INCOMING_DIRECTORY = null;

	/**
	 * Cache for the storage directory for cached files.
	 */
	private static File CACHE_DIRECTORY = null;

	/**
	 * The server socket listening for incoming connections.
	 */
	private final ServerSocket serverSocket;

	/**
	 * Set of job-specific directories created by this BLOB server.
	 */
	private final Set<JobID> createdJobDirectories = Collections.newSetFromMap(new ConcurrentHashMap<JobID, Boolean>());

	/**
	 * Indicates whether a shutdown of server component has been requested.
	 */
	private volatile boolean shutdownRequested = false;

	/**
	 * Instantiates a new BLOB server and binds it to a free network port.
	 * 
	 * @throws IOException
	 *         thrown if the BLOB server cannot bind to a free network port
	 */
	public BlobServer() throws IOException {

		this.serverSocket = new ServerSocket(0);
		start();

		if (LOG.isInfoEnabled()) {
			LOG.info(String.format("Started BLOB server on port %d",
				this.serverSocket.getLocalPort()));
		}
	}

	/**
	 * Returns the network port the BLOB server is bound to. The return value of this method is undefined after the BLOB
	 * server has been shut down.
	 * 
	 * @return the network port the BLOB server is bound to
	 */
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

	/**
	 * Returns the storage directory used by the BLOB server. The directory is created if it did not exist so far.
	 * 
	 * @return the storage directory used by the BLOB server
	 */
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

	/**
	 * Returns a temporary file inside the BLOB server's incoming directory.
	 * 
	 * @return a temporary file inside the BLOB server's incoming directory
	 */
	static File getTemporaryFilename() {

		return new File(getIncomingDirectory(), String.format("temp-%08d", TEMP_FILE_COUNTER.getAndIncrement()));
	}

	/**
	 * Returns the BLOB server's directory for incoming files. The directory is created if it did not exist so far.
	 * 
	 * @return the BLOB server's directory for incoming files
	 */
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

	/**
	 * Returns the BLOB server's directory for cached files. The directory is created if it did not exist so far.
	 * 
	 * @return the BLOB server's directory for cached files
	 */
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

	/**
	 * Returns the BLOB server's storage directory for BLOBs belonging to the job with the given ID.
	 * 
	 * @param jobID
	 *        the ID of the job to return the storage directory for
	 * @param create
	 *        <code>true</code> to create the directory if it did not exist so far, <code>false</code> otherwise
	 * @return the storage directory for BLOBs belonging to the job with the given ID
	 */
	private File getJobDirectory(final JobID jobID, final boolean create) {

		final File jobDirectory = new File(getStorageDirectory(), JOB_DIR_PREFIX + jobID.toString());
		if (create) {
			if (jobDirectory.mkdirs()) {
				this.createdJobDirectories.add(jobID);
			}
		}

		return jobDirectory;
	}

	/**
	 * Returns the (designated) physical storage location of the BLOB with the given key.
	 * 
	 * @param key
	 *        the key identifying the BLOB
	 * @return the (designated) physical storage location of the BLOB
	 */
	static File getStorageLocation(final BlobKey key) {

		final File storageDirectory = getCacheDirectory();

		return new File(storageDirectory, BLOB_FILE_PREFIX + key.toString());
	}

	/**
	 * Returns the (designated) physical storage location of the BLOB with the given job ID and key.
	 * 
	 * @param jobID
	 *        the ID of the job the BLOB belongs to
	 * @param key
	 *        the key of the BLOB
	 * @return the (designated) physical storage location of the BLOB with the given job ID and key
	 */
	File getStorageLocation(final JobID jobID, final String key) {

		return new File(getJobDirectory(jobID, true), BLOB_FILE_PREFIX + encodeKey(key));
	}

	/**
	 * Translates the user's key for a BLOB into the internal name used by the BLOB server
	 * 
	 * @param key
	 *        the user's key for a BLOB
	 * @return the internal name for the BLOB as used by the BLOB server
	 */
	private static String encodeKey(final String key) {

		return BaseEncoding.base64().encode(key.getBytes(DEFAULT_CHARSET));
	}

	@Override
	public void run() {

		try {

			while (!this.shutdownRequested) {
				new BlobConnection(this.serverSocket.accept(), this).start();
			}

		} catch (IOException ioe) {
			if (!this.shutdownRequested && LOG.isErrorEnabled()) {
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

	/**
	 * Auxiliary method to read a particular number of bytes from an input stream. This method blocks until the
	 * requested number of bytes have been read from the stream. If the stream cannot offer enough data, an
	 * {@link EOFException} is thrown.
	 * 
	 * @param inputStream
	 *        the input stream to read the data from
	 * @param buf
	 *        the buffer to store the read data
	 * @param off
	 *        the offset inside the buffer
	 * @param len
	 *        the number of bytes to read from the stream
	 * @throws IOException
	 *         thrown if I/O error occurs while reading from the stream or the stream cannot offer enough data
	 */
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

	/**
	 * Shuts down the BLOB server.
	 */
	public void shutDown() {

		this.shutdownRequested = true;
		try {
			this.serverSocket.close();
		} catch (IOException ioe) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(ioe);
			}
		}
		try {
			join();
		} catch (InterruptedException ie) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(ie);
			}
		}

		// Clean up the storage directory
		deleteAllJobDirectories();

		// TODO: Find/implement strategy to handle content-addressable BLOBs
	}

	/**
	 * Deletes all job-specific storage directories.
	 */
	private void deleteAllJobDirectories() {

		if (STORAGE_DIRECTORY == null) {
			return;
		}

		for (final Iterator<JobID> it = this.createdJobDirectories.iterator(); it.hasNext();) {
			deleteJobDirectory(it.next());
		}
	}

	/**
	 * Deletes the storage directory for the job with the given ID.
	 * 
	 * @param jobId
	 *        the job ID identifying the directory to delete
	 */
	void deleteJobDirectory(final JobID jobId) {

		final File jobDirectory = getJobDirectory(jobId, false);

		if (!jobDirectory.exists()) {
			return;
		}

		final File[] blobFiles = jobDirectory.listFiles(BLOB_FILE_FILTER);

		for (final File blobFile : blobFiles) {
			blobFile.delete();
		}

		jobDirectory.delete();
		this.createdJobDirectories.remove(jobId);
	}
}
