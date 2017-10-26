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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides a cache for permanent BLOB files including a per-job ref-counting and a staged cleanup.
 * <p>
 * When requesting BLOBs via {@link #getHAFile(JobID, BlobKey)}, the cache will first attempt to
 * serve the file from its local cache. Only if the local cache does not contain the desired BLOB,
 * it will try to download it from a distributed HA file system (if available) or the BLOB server.
 * <p>
 * If files for a job are not needed any more, they will enter a staged, i.e. deferred, cleanup.
 * Files may thus still be be accessible upon recovery and do not need to be re-downloaded.
 */
public class PermanentBlobCache extends TimerTask implements PermanentBlobService {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(PermanentBlobCache.class);

	/** Counter to generate unique names for temporary files. */
	private final AtomicLong tempFileCounter = new AtomicLong(0);

	private final InetSocketAddress serverAddress;

	/** Root directory for local file storage */
	private final File storageDir;

	/** Blob store for distributed file storage, e.g. in HA */
	private final BlobView blobView;

	private final AtomicBoolean shutdownRequested = new AtomicBoolean();

	/** Shutdown hook thread to ensure deletion of the storage directory. */
	private final Thread shutdownHook;

	/** The number of retries when the transfer fails */
	private final int numFetchRetries;

	/** Configuration for the blob client like ssl parameters required to connect to the blob server */
	private final Configuration blobClientConfig;

	/** Lock guarding concurrent file accesses */
	private final ReadWriteLock readWriteLock;

	// --------------------------------------------------------------------------------------------

	/**
	 * Job reference counters with a time-to-live (TTL).
	 */
	@VisibleForTesting
	static class RefCount {
		/**
		 * Number of references to a job.
		 */
		public int references = 0;

		/**
		 * Timestamp in milliseconds when any job data should be cleaned up (no cleanup for
		 * non-positive values).
		 */
		public long keepUntil = -1;
	}

	/** Map to store the number of references to a specific job */
	private final Map<JobID, RefCount> jobRefCounters = new HashMap<>();

	/** Time interval (ms) to run the cleanup task; also used as the default TTL. */
	private final long cleanupInterval;

	private final Timer cleanupTimer;

	/**
	 * Instantiates a new cache for permanent BLOBs which are also available in an HA store.
	 *
	 * @param serverAddress
	 * 		address of the {@link BlobServer} to use for fetching files from
	 * @param blobClientConfig
	 * 		global configuration
	 * @param blobView
	 * 		(distributed) HA blob store file system to retrieve files from first
	 *
	 * @throws IOException
	 * 		thrown if the (local or distributed) file storage cannot be created or is not usable
	 */
	public PermanentBlobCache(
		final InetSocketAddress serverAddress,
		final Configuration blobClientConfig,
		final BlobView blobView) throws IOException {

		this.serverAddress = checkNotNull(serverAddress);
		this.blobClientConfig = checkNotNull(blobClientConfig);
		this.blobView = checkNotNull(blobView, "blobStore");
		this.readWriteLock = new ReentrantReadWriteLock();

		// configure and create the storage directory
		String storageDirectory = blobClientConfig.getString(BlobServerOptions.STORAGE_DIRECTORY);
		this.storageDir = BlobUtils.initLocalStorageDirectory(storageDirectory);
		LOG.info("Created permanent BLOB cache storage directory " + storageDir);

		// configure the number of fetch retries
		final int fetchRetries = blobClientConfig.getInteger(BlobServerOptions.FETCH_RETRIES);
		if (fetchRetries >= 0) {
			this.numFetchRetries = fetchRetries;
		} else {
			LOG.warn("Invalid value for {}. System will attempt no retries on failed fetch operations of BLOBs.",
				BlobServerOptions.FETCH_RETRIES.key());
			this.numFetchRetries = 0;
		}

		// Initializing the clean up task
		this.cleanupTimer = new Timer(true);

		this.cleanupInterval = blobClientConfig.getLong(BlobServerOptions.CLEANUP_INTERVAL) * 1000;
		this.cleanupTimer.schedule(this, cleanupInterval, cleanupInterval);

		// Add shutdown hook to delete storage directory
		shutdownHook = BlobUtils.addShutdownHook(this, LOG);
	}

	/**
	 * Registers use of job-related BLOBs.
	 * <p>
	 * Using any other method to access BLOBs, e.g. {@link #getHAFile}, is only valid within calls
	 * to <tt>registerJob(JobID)</tt> and {@link #releaseJob(JobID)}.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 *
	 * @see #releaseJob(JobID)
	 */
	public void registerJob(JobID jobId) {
		synchronized (jobRefCounters) {
			RefCount ref = jobRefCounters.get(jobId);
			if (ref == null) {
				ref = new RefCount();
				jobRefCounters.put(jobId, ref);
			} else {
				// reset cleanup timeout
				ref.keepUntil = -1;
			}
			++ref.references;
		}
	}

	/**
	 * Unregisters use of job-related BLOBs and allow them to be released.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 *
	 * @see #registerJob(JobID)
	 */
	public void releaseJob(JobID jobId) {
		synchronized (jobRefCounters) {
			RefCount ref = jobRefCounters.get(jobId);

			if (ref == null) {
				LOG.warn("improper use of releaseJob() without a matching number of registerJob() calls for jobId " + jobId);
				return;
			}

			--ref.references;
			if (ref.references == 0) {
				ref.keepUntil = System.currentTimeMillis() + cleanupInterval;
			}
		}
	}

	public int getNumberOfReferenceHolders(JobID jobId) {
		synchronized (jobRefCounters) {
			RefCount ref = jobRefCounters.get(jobId);
			if (ref == null) {
				return 0;
			} else {
				return ref.references;
			}
		}
	}

	public int getNumberOfCachedJobs() {
		return jobRefCounters.size();
	}

	/**
	 * Returns the path to a local copy of the file associated with the provided job ID and blob
	 * key.
	 * <p>
	 * We will first attempt to serve the BLOB from the local storage. If the BLOB is not in
	 * there, we will try to download it from the HA store, or directly from the {@link BlobServer}.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		blob key associated with the requested file
	 *
	 * @return The path to the file.
	 *
	 * @throws java.io.FileNotFoundException
	 * 		if the BLOB does not exist;
	 * @throws IOException
	 * 		if any other error occurs when retrieving the file
	 */
	@Override
	public File getHAFile(JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		return getHAFileInternal(jobId, key);
	}

	/**
	 * Returns local copy of the file for the BLOB with the given key.
	 * <p>
	 * The method will first attempt to serve the BLOB from its local cache. If the BLOB is not in
	 * the cache, the method will try to download it from this cache's BLOB server.
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
	private File getHAFileInternal(@Nullable JobID jobId, BlobKey blobKey) throws IOException {
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
				blobView.get(jobId, blobKey, incomingFile);
				BlobUtils.moveTempFileToStore(
					incomingFile, jobId, blobKey, localFile, readWriteLock.writeLock(), LOG, null);

				return localFile;
			} catch (Exception e) {
				LOG.info("Failed to copy from blob store. Downloading from BLOB server instead.", e);
			}

			// fallback: download from the BlobServer
			BlobClient.downloadFromBlobServer(
				jobId, blobKey, true, incomingFile, serverAddress, blobClientConfig, numFetchRetries);
			BlobUtils.moveTempFileToStore(
				incomingFile, jobId, blobKey, localFile, readWriteLock.writeLock(), LOG, null);

			return localFile;
		} finally {
			// delete incomingFile from a failed download
			if (!incomingFile.delete() && incomingFile.exists()) {
				LOG.warn("Could not delete the staging file {} for blob key {} and job {}.",
					incomingFile, blobKey, jobId);
			}
		}
	}

	public int getPort() {
		return serverAddress.getPort();
	}

	/**
	 * Returns the job reference counters - for testing purposes only!
	 *
	 * @return job reference counters (internal state!)
	 */
	@VisibleForTesting
	Map<JobID, RefCount> getJobRefCounters() {
		return jobRefCounters;
	}

	/**
	 * Returns a file handle to the file associated with the given blob key on the blob
	 * server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key
	 * 		identifying the file
	 *
	 * @return file handle to the file
	 *
	 * @throws IOException
	 * 		if creating the directory fails
	 */
	@VisibleForTesting
	public File getStorageLocation(JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		return BlobUtils.getStorageLocation(storageDir, jobId, key);
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

	/**
	 * Cleans up BLOBs which are not referenced anymore.
	 */
	@Override
	public void run() {
		synchronized (jobRefCounters) {
			Iterator<Map.Entry<JobID, RefCount>> entryIter = jobRefCounters.entrySet().iterator();
			final long currentTimeMillis = System.currentTimeMillis();

			while (entryIter.hasNext()) {
				Map.Entry<JobID, RefCount> entry = entryIter.next();
				RefCount ref = entry.getValue();

				if (ref.references <= 0 && ref.keepUntil > 0 && currentTimeMillis >= ref.keepUntil) {
					JobID jobId = entry.getKey();

					final File localFile =
						new File(BlobUtils.getStorageLocationPath(storageDir.getAbsolutePath(), jobId));

					/*
					 * NOTE: normally it is not required to acquire the write lock to delete the job's
					 *       storage directory since there should be noone accessing it with the ref
					 *       counter being 0 - acquire it just in case, to always be on the safe side
					 */
					readWriteLock.writeLock().lock();

					boolean success = false;
					try {
						FileUtils.deleteDirectory(localFile);
						success = true;
					} catch (Throwable t) {
						LOG.warn("Failed to locally delete job directory " + localFile.getAbsolutePath(), t);
					} finally {
						readWriteLock.writeLock().unlock();
					}

					// let's only remove this directory from cleanup if the cleanup was successful
					// (does not need the write lock)
					if (success) {
						entryIter.remove();
					}
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		cleanupTimer.cancel();

		if (shutdownRequested.compareAndSet(false, true)) {
			LOG.info("Shutting down permanent BlobCache");

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
						LOG.warn("Exception while unregistering BLOB cache's cleanup shutdown hook.");
					}
				}
			}
		}
	}
}
