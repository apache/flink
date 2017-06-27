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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/**
 * Utility class to work with blob data.
 */
public class BlobUtils {

	/**
	 * Algorithm to be used for calculating the BLOB keys.
	 */
	private static final String HASHING_ALGORITHM = "SHA-1";

	/**
	 * The prefix of all BLOB files stored by the BLOB server.
	 */
	private static final String BLOB_FILE_PREFIX = "blob_";

	/**
	 * The prefix of all job-specific directories created by the BLOB server.
	 */
	private static final String JOB_DIR_PREFIX = "job_";

	/**
	 * The prefix of all job-unrelated directories created by the BLOB server.
	 */
	private static final String NO_JOB_DIR_PREFIX = "no_job";

	/**
	 * Creates a BlobStore based on the parameters set in the configuration.
	 *
	 * @param config
	 * 		configuration to use
	 *
	 * @return a (distributed) blob store for high availability
	 *
	 * @throws IOException
	 * 		thrown if the (distributed) file storage cannot be created
	 */
	public static BlobStoreService createBlobStoreFromConfig(Configuration config) throws IOException {
		HighAvailabilityMode highAvailabilityMode = HighAvailabilityMode.fromConfig(config);

		if (highAvailabilityMode == HighAvailabilityMode.NONE) {
			return new VoidBlobStore();
		} else if (highAvailabilityMode == HighAvailabilityMode.ZOOKEEPER) {
			return createFileSystemBlobStore(config);
		} else {
			throw new IllegalConfigurationException("Unexpected high availability mode '" + highAvailabilityMode + "'.");
		}
	}

	private static BlobStoreService createFileSystemBlobStore(Configuration configuration) throws IOException {
		String storagePath = configuration.getValue(
			HighAvailabilityOptions.HA_STORAGE_PATH);
		if (isNullOrWhitespaceOnly(storagePath)) {
			throw new IllegalConfigurationException("Configuration is missing the mandatory parameter: " +
				HighAvailabilityOptions.HA_STORAGE_PATH);
		}

		final Path path;
		try {
			path = new Path(storagePath);
		} catch (Exception e) {
			throw new IOException("Invalid path for highly available storage (" +
				HighAvailabilityOptions.HA_STORAGE_PATH.key() + ')', e);
		}

		final FileSystem fileSystem;
		try {
			fileSystem = path.getFileSystem();
		} catch (Exception e) {
			throw new IOException("Could not create FileSystem for highly available storage (" +
				HighAvailabilityOptions.HA_STORAGE_PATH.key() + ')', e);
		}

		final String clusterId =
			configuration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);
		storagePath += "/" + clusterId;

		return new FileSystemBlobStore(fileSystem, storagePath);
	}

	/**
	 * Creates a local storage directory for a blob service under the given parent directory.
	 *
	 * @param basePath
	 * 		base path, i.e. parent directory, of the storage directory to use (if <tt>null</tt> or
	 * 		empty, the path in <tt>java.io.tmpdir</tt> will be used)
	 *
	 * @return a new local storage directory
	 *
	 * @throws IOException
	 * 		thrown if the local file storage cannot be created or is not usable
	 */
	static File initLocalStorageDirectory(String basePath) throws IOException {
		File baseDir;
		if (StringUtils.isNullOrWhitespaceOnly(basePath)) {
			baseDir = new File(System.getProperty("java.io.tmpdir"));
		}
		else {
			baseDir = new File(basePath);
		}

		File storageDir;

		// NOTE: although we will be using UUIDs, there may be collisions
		final int MAX_ATTEMPTS = 10;
		for(int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
			storageDir = new File(baseDir, String.format(
					"blobStore-%s", UUID.randomUUID().toString()));

			// Create the storage dir if it doesn't exist. Only return it when the operation was
			// successful.
			if (storageDir.mkdirs()) {
				return storageDir;
			}
		}

		// max attempts exceeded to find a storage directory
		throw new IOException("Could not create storage directory for BLOB store in '" + baseDir + "'.");
	}

	/**
	 * Returns the BLOB service's directory for incoming (job-unrelated) files. The directory is
	 * created if it does not exist yet.
	 *
	 * @param storageDir
	 * 		storage directory used be the BLOB service
	 *
	 * @return the BLOB service's directory for incoming files
	 */
	static File getIncomingDirectory(File storageDir) {
		final File incomingDir = new File(storageDir, "incoming");

		mkdirTolerateExisting(incomingDir);

		return incomingDir;
	}

	/**
	 * Makes sure a given directory exists by creating it if necessary.
	 *
	 * @param dir
	 * 		directory to create
	 */
	private static void mkdirTolerateExisting(final File dir) {
		// note: thread-safe create should try to mkdir first and then ignore the case that the
		//       directory already existed
		if (!dir.mkdirs() && !dir.exists()) {
			throw new RuntimeException(
				"Cannot create directory '" + dir.getAbsolutePath() + "'.");
		}
	}

	/**
	 * Returns the (designated) physical storage location of the BLOB with the given key.
	 *
	 * @param storageDir
	 * 		storage directory used be the BLOB service
	 * @param key
	 * 		the key identifying the BLOB
	 * @param jobId
	 * 		ID of the job for the incoming files (or <tt>null</tt> if job-unrelated)
	 *
	 * @return the (designated) physical storage location of the BLOB
	 */
	static File getStorageLocation(
			File storageDir, @Nullable JobID jobId, BlobKey key) {
		File file = new File(getStorageLocationPath(storageDir.getAbsolutePath(), jobId, key));

		mkdirTolerateExisting(file.getParentFile());

		return file;
	}

	/**
	 * Returns the BLOB server's storage directory for BLOBs belonging to the job with the given ID
	 * <em>without</em> creating the directory.
	 *
	 * @param storageDir
	 * 		storage directory used be the BLOB service
	 * @param jobId
	 * 		the ID of the job to return the storage directory for
	 *
	 * @return the storage directory for BLOBs belonging to the job with the given ID
	 */
	static String getStorageLocationPath(String storageDir, @Nullable JobID jobId) {
		if (jobId == null) {
			// format: $base/no_job
			return String.format("%s/%s", storageDir, NO_JOB_DIR_PREFIX);
		} else {
			// format: $base/job_$jobId
			return String.format("%s/%s%s", storageDir, JOB_DIR_PREFIX, jobId.toString());
		}
	}

	/**
	 * Returns the path for the given blob key.
	 * <p>
	 * The returned path can be used with the (local or HA) BLOB store file system back-end for
	 * recovery purposes and follows the same scheme as {@link #getStorageLocation(File, JobID,
	 * BlobKey)}.
	 *
	 * @param storageDir
	 * 		storage directory used be the BLOB service
	 * @param key
	 * 		the key identifying the BLOB
	 * @param jobId
	 * 		ID of the job for the incoming files
	 *
	 * @return the path to the given BLOB
	 */
	static String getStorageLocationPath(
			String storageDir, @Nullable JobID jobId, BlobKey key) {
		if (jobId == null) {
			// format: $base/no_job/blob_$key
			return String.format("%s/%s/%s%s",
				storageDir, NO_JOB_DIR_PREFIX, BLOB_FILE_PREFIX, key.toString());
		} else {
			// format: $base/job_$jobId/blob_$key
			return String.format("%s/%s%s/%s%s",
				storageDir, JOB_DIR_PREFIX, jobId.toString(), BLOB_FILE_PREFIX, key.toString());
		}
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
			throw new RuntimeException("Cannot instantiate the message digest algorithm " + HASHING_ALGORITHM, e);
		}
	}

	/**
	 * Adds a shutdown hook to the JVM and returns the Thread, which has been registered.
	 */
	static Thread addShutdownHook(final Closeable service, final Logger logger) {
		checkNotNull(service);
		checkNotNull(logger);

		final Thread shutdownHook = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					service.close();
				}
				catch (Throwable t) {
					logger.error("Error during shutdown of blob service via JVM shutdown hook.", t);
				}
			}
		});

		try {
			// Add JVM shutdown hook to call shutdown of service
			Runtime.getRuntime().addShutdownHook(shutdownHook);
			return shutdownHook;
		}
		catch (IllegalStateException e) {
			// JVM is already shutting down. no need to do our work
			return null;
		}
		catch (Throwable t) {
			logger.error("Cannot register shutdown hook that cleanly terminates the BLOB service.");
			return null;
		}
	}

	/**
	 * Auxiliary method to write the length of an upcoming data chunk to an
	 * output stream.
	 *
	 * @param length
	 *        the length of the upcoming data chunk in bytes
	 * @param outputStream
	 *        the output stream to write the length to
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing to the output
	 *         stream
	 */
	static void writeLength(int length, OutputStream outputStream) throws IOException {
		byte[] buf = new byte[4];
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
	 * @param inputStream
	 *        the input stream to read the length from
	 * @return the length of the upcoming data chunk in bytes
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading from the input
	 *         stream
	 */
	static int readLength(InputStream inputStream) throws IOException {
		byte[] buf = new byte[4];
		int bytesRead = 0;
		while (bytesRead < 4) {
			final int read = inputStream.read(buf, bytesRead, 4 - bytesRead);
			if (read < 0) {
				throw new EOFException("Read an incomplete length");
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
	 * @param inputStream The input stream to read the data from.
	 * @param buf The buffer to store the read data.
	 * @param off The offset inside the buffer.
	 * @param len The number of bytes to read from the stream.
	 * @param type The name of the type, to throw a good error message in case of not enough data.
	 * @throws IOException
	 *         Thrown if I/O error occurs while reading from the stream or the stream cannot offer enough data.
	 */
	static void readFully(InputStream inputStream, byte[] buf, int off, int len, String type) throws IOException {

		int bytesRead = 0;
		while (bytesRead < len) {

			final int read = inputStream.read(buf, off + bytesRead, len
					- bytesRead);
			if (read < 0) {
				throw new EOFException("Received an incomplete " + type);
			}
			bytesRead += read;
		}
	}

	static void closeSilently(Socket socket, Logger LOG) {
		if (socket != null) {
			try {
				socket.close();
			} catch (Throwable t) {
				LOG.debug("Exception while closing BLOB server connection socket.", t);
			}
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private BlobUtils() {
		throw new RuntimeException();
	}
}
