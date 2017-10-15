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
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract base class for permanent and transient BLOB files.
 */
public abstract class AbstractBlobCache implements Closeable {

	/**
	 * The log object used for debugging.
	 */
	protected final Logger log;

	/**
	 * Counter to generate unique names for temporary files.
	 */
	protected final AtomicLong tempFileCounter = new AtomicLong(0);

	protected final InetSocketAddress serverAddress;

	/**
	 * Root directory for local file storage.
	 */
	protected final File storageDir;

	/**
	 * Blob store for distributed file storage, e.g. in HA.
	 */
	protected final BlobView blobView;

	protected final AtomicBoolean shutdownRequested = new AtomicBoolean();

	/**
	 * Shutdown hook thread to ensure deletion of the local storage directory.
	 */
	protected final Thread shutdownHook;

	/**
	 * The number of retries when the transfer fails.
	 */
	protected final int numFetchRetries;

	/**
	 * Configuration for the blob client like ssl parameters required to connect to the blob
	 * server.
	 */
	protected final Configuration blobClientConfig;

	/**
	 * Lock guarding concurrent file accesses.
	 */
	protected final ReadWriteLock readWriteLock;

	public AbstractBlobCache(
			final InetSocketAddress serverAddress,
			final Configuration blobClientConfig,
			final BlobView blobView,
			final Logger logger) throws IOException {

		this.log = checkNotNull(logger);

		this.serverAddress = checkNotNull(serverAddress);
		this.blobClientConfig = checkNotNull(blobClientConfig);
		this.blobView = checkNotNull(blobView);
		this.readWriteLock = new ReentrantReadWriteLock();

		// configure and create the storage directory
		String storageDirectory = blobClientConfig.getString(BlobServerOptions.STORAGE_DIRECTORY);
		this.storageDir = BlobUtils.initLocalStorageDirectory(storageDirectory);
		log.info("Created BLOB cache storage directory " + storageDir);

		// configure the number of fetch retries
		final int fetchRetries = blobClientConfig.getInteger(BlobServerOptions.FETCH_RETRIES);
		if (fetchRetries >= 0) {
			this.numFetchRetries = fetchRetries;
		} else {
			log.warn("Invalid value for {}. System will attempt no retries on failed fetch operations of BLOBs.",
				BlobServerOptions.FETCH_RETRIES.key());
			this.numFetchRetries = 0;
		}

		// Add shutdown hook to delete storage directory
		shutdownHook = BlobUtils.addShutdownHook(this, log);
	}

	/**
	 * Returns local copy of the file for the BLOB with the given key.
	 *
	 * <p>The method will first attempt to serve the BLOB from its local cache. If the BLOB is not
	 * in the cache, the method will try to download it from this cache's BLOB server via a
	 * distributed BLOB store (if available) or direct end-to-end download.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param blobKey
	 * 		The key of the desired BLOB.
	 *
	 * @return file referring to the local storage location of the BLOB.
	 *
	 * @throws IOException
	 * 		Thrown if an I/O error occurs while downloading the BLOBs from the BLOB server.
	 */
	protected File getFileInternal(@Nullable JobID jobId, BlobKey blobKey) throws IOException {
		checkArgument(blobKey != null, "BLOB key cannot be null.");

		final File localFile = BlobUtils.getStorageLocation(storageDir, jobId, blobKey);
		readWriteLock.readLock().lock();

		try {
			if (localFile.exists()) {
				return localFile;
			}
		} finally {
			readWriteLock.readLock().unlock();
		}

		// first try the distributed blob store (if available)
		// use a temporary file (thread-safe without locking)
		File incomingFile = createTemporaryFilename();
		try {
			try {
				if (blobView.get(jobId, blobKey, incomingFile)) {
					// now move the temp file to our local cache atomically
					readWriteLock.writeLock().lock();
					try {
						BlobUtils.moveTempFileToStore(
							incomingFile, jobId, blobKey, localFile, log, null);
					} finally {
						readWriteLock.writeLock().unlock();
					}

					return localFile;
				}
			} catch (Exception e) {
				log.info("Failed to copy from blob store. Downloading from BLOB server instead.", e);
			}

			// fallback: download from the BlobServer
			BlobClient.downloadFromBlobServer(
				jobId, blobKey, incomingFile, serverAddress, blobClientConfig, numFetchRetries);

			readWriteLock.writeLock().lock();
			try {
				BlobUtils.moveTempFileToStore(
					incomingFile, jobId, blobKey, localFile, log, null);
			} finally {
				readWriteLock.writeLock().unlock();
			}

			return localFile;
		} finally {
			// delete incomingFile from a failed download
			if (!incomingFile.delete() && incomingFile.exists()) {
				log.warn("Could not delete the staging file {} for blob key {} and job {}.",
					incomingFile, blobKey, jobId);
			}
		}
	}

	/**
	 * Returns the port the BLOB server is listening on.
	 *
	 * @return BLOB server port
	 */
	public int getPort() {
		return serverAddress.getPort();
	}

	/**
	 * Returns a temporary file inside the BLOB server's incoming directory.
	 *
	 * @return a temporary file inside the BLOB server's incoming directory
	 *
	 * @throws IOException
	 * 		if creating the directory fails
	 */
	File createTemporaryFilename() throws IOException {
		return new File(BlobUtils.getIncomingDirectory(storageDir),
			String.format("temp-%08d", tempFileCounter.getAndIncrement()));
	}

	@Override
	public void close() throws IOException {
		cancelCleanupTask();

		if (shutdownRequested.compareAndSet(false, true)) {
			log.info("Shutting down BLOB cache");

			// Clean up the storage directory
			try {
				FileUtils.deleteDirectory(storageDir);
			} finally {
				// Remove shutdown hook to prevent resource leaks, unless this is invoked by the shutdown hook itself
				if (shutdownHook != null && shutdownHook != Thread.currentThread()) {
					try {
						Runtime.getRuntime().removeShutdownHook(shutdownHook);
					} catch (IllegalStateException e) {
						// race, JVM is in shutdown already, we can safely ignore this
					} catch (Throwable t) {
						log.warn("Exception while unregistering BLOB cache's cleanup shutdown hook.");
					}
				}
			}
		}
	}

	/**
	 * Cancels any cleanup task that subclasses may be executing.
	 *
	 * <p>This is called during {@link #close()}.
	 */
	protected abstract void cancelCleanupTask();
}
