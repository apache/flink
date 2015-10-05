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

import com.google.common.io.BaseEncoding;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.util.IOUtils;
import org.slf4j.Logger;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

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
	static final String BLOB_FILE_PREFIX = "blob_";

	/**
	 * The prefix of all job-specific directories created by the BLOB server.
	 */
	static final String JOB_DIR_PREFIX = "job_";

	/**
	 * The default character set to translate between characters and bytes.
	 */
	static final Charset DEFAULT_CHARSET = Charset.forName("utf-8");

	/**
	 * Creates a storage directory for a blob service.
	 *
	 * @return the storage directory used by a BLOB service
	 */
	static File initStorageDirectory(String storageDirectory) {
		File baseDir;
		if (storageDirectory == null || storageDirectory.trim().isEmpty()) {
			baseDir = new File(System.getProperty("java.io.tmpdir"));
		}
		else {
			baseDir = new File(storageDirectory);
		}

		File storageDir;
		final int MAX_ATTEMPTS = 10;
		int attempt;

		for(attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
			storageDir = new File(baseDir, String.format(
					"blobStore-%s", UUID.randomUUID().toString()));

			// Create the storage dir if it doesn't exist. Only return it when the operation was
			// successful.
			if (!storageDir.exists() && storageDir.mkdirs()) {
				return storageDir;
			}
		}

		// max attempts exceeded to find a storage directory
		throw new RuntimeException("Could not create storage directory for BLOB store in '" + baseDir + "'.");
	}

	/**
	 * Returns the BLOB service's directory for incoming files. The directory is created if it did
	 * not exist so far.
	 *
	 * @return the BLOB server's directory for incoming files
	 */
	static File getIncomingDirectory(File storageDir) {
		final File incomingDir = new File(storageDir, "incoming");

		if (!incomingDir.exists() && !incomingDir.mkdirs()) {
			throw new RuntimeException("Cannot create directory for incoming files " + incomingDir.getAbsolutePath());
		}

		return incomingDir;
	}

	/**
	 * Returns the BLOB service's directory for cached files. The directory is created if it did
	 * not exist so far.
	 *
	 * @return the BLOB server's directory for cached files
	 */
	private static File getCacheDirectory(File storageDir) {
		final File cacheDirectory = new File(storageDir, "cache");

		if (!cacheDirectory.exists() && !cacheDirectory.mkdirs()) {
			throw new RuntimeException("Could not create cache directory '" + cacheDirectory.getAbsolutePath() + "'.");
		}

		return cacheDirectory;
	}

	/**
	 * Returns the (designated) physical storage location of the BLOB with the given key.
	 *
	 * @param key
	 *        the key identifying the BLOB
	 * @return the (designated) physical storage location of the BLOB
	 */
	static File getStorageLocation(File storageDir, BlobKey key) {
		return new File(getCacheDirectory(storageDir), BLOB_FILE_PREFIX + key.toString());
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
	static File getStorageLocation(File storageDir, JobID jobID, String key) {
		return new File(getJobDirectory(storageDir, jobID), BLOB_FILE_PREFIX + encodeKey(key));
	}

	/**
	 * Returns the BLOB server's storage directory for BLOBs belonging to the job with the given ID.
	 *
	 * @param jobID
	 *        the ID of the job to return the storage directory for
	 * @return the storage directory for BLOBs belonging to the job with the given ID
	 */
	private static File getJobDirectory(File storageDir, JobID jobID) {
		final File jobDirectory = new File(storageDir, JOB_DIR_PREFIX + jobID.toString());

		if (!jobDirectory.exists() && !jobDirectory.mkdirs()) {
			throw new RuntimeException("Could not create jobId directory '" + jobDirectory.getAbsolutePath() + "'.");
		}

		return jobDirectory;
	}

	/**
	 * Translates the user's key for a BLOB into the internal name used by the BLOB server
	 *
	 * @param key
	 *        the user's key for a BLOB
	 * @return the internal name for the BLOB as used by the BLOB server
	 */
	static String encodeKey(String key) {
		return BaseEncoding.base64().encode(key.getBytes(DEFAULT_CHARSET));
	}

	/**
	 * Deletes the storage directory for the job with the given ID.
	 *
	 * @param jobID
	 *			jobID whose directory shall be deleted
	 */
	static void deleteJobDirectory(File storageDir, JobID jobID) throws IOException {
		File directory = getJobDirectory(storageDir, jobID);
		FileUtils.deleteDirectory(directory);
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
	static Thread addShutdownHook(final BlobService service, final Logger logger) {
		checkNotNull(service);
		checkNotNull(logger);

		final Thread shutdownHook = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					service.shutdown();
				}
				catch (Throwable t) {
					logger.error("Error during shutdown of blob service via JVM shutdown hook: " + t.getMessage(), t);
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
				if (LOG.isDebugEnabled()) {
					LOG.debug("Error while closing resource after BLOB transfer.", t);
				}
			}
		}
	}

	/**
	 * Returns the path for the given blob key.
	 *
	 * <p>The returned path can be used with the state backend for recovery purposes.
	 *
	 * <p>This follows the same scheme as {@link #getStorageLocation(File, BlobKey)}.
	 */
	static String getRecoveryPath(String basePath, BlobKey blobKey) {
		// format: $base/cache/blob_$key
		return String.format("%s/cache/%s", basePath, BLOB_FILE_PREFIX + blobKey.toString());
	}

	/**
	 * Returns the path for the given job ID and key.
	 *
	 * <p>The returned path can be used with the state backend for recovery purposes.
	 *
	 * <p>This follows the same scheme as {@link #getStorageLocation(File, JobID, String)}.
	 */
	static String getRecoveryPath(String basePath, JobID jobId, String key) {
		// format: $base/job_$id/blob_$key
		return String.format("%s/%s/%s", basePath, JOB_DIR_PREFIX + jobId.toString(),
				BLOB_FILE_PREFIX + encodeKey(key));
	}

	/**
	 * Returns the path for the given job ID.
	 *
	 * <p>The returned path can be used with the state backend for recovery purposes.
	 */
	static String getRecoveryPath(String basePath, JobID jobId) {
		return String.format("%s/%s", basePath, JOB_DIR_PREFIX + jobId.toString());
	}

	/**
	 * Copies the file from the recovery path to the local file.
	 */
	static void copyFromRecoveryPath(String recoveryPath, File localBlobFile) throws Exception {
		if (recoveryPath == null) {
			throw new IllegalStateException("Failed to determine recovery path.");
		}

		if (!localBlobFile.createNewFile()) {
			throw new IllegalStateException("Failed to create new local file to copy to");
		}

		URI uri = new URI(recoveryPath);
		Path path = new Path(recoveryPath);

		if (FileSystem.get(uri).exists(path)) {
			try (InputStream is = FileSystem.get(uri).open(path)) {
				FileOutputStream fos = new FileOutputStream(localBlobFile);
				IOUtils.copyBytes(is, fos); // closes the streams
			}
		}
		else {
			throw new IOException("Cannot find required BLOB at '" + recoveryPath + "' for recovery.");
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private BlobUtils() {
		throw new RuntimeException();
	}
}
