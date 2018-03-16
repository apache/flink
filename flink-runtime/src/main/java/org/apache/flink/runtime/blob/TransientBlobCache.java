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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;

import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides access to transient BLOB files stored at the {@link BlobServer}.
 *
 * <p>TODO: make this truly transient by returning file streams to a local copy with the remote
 * being removed upon retrieval and the local copy being deleted at the end of the stream.
 */
public class TransientBlobCache extends AbstractBlobCache implements TransientBlobService {

	/**
	 * Map to store the TTL of each element stored in the local storage, i.e. via one of the {@link
	 * #getFile} methods.
	 **/
	private final ConcurrentHashMap<Tuple2<JobID, TransientBlobKey>, Long> blobExpiryTimes =
		new ConcurrentHashMap<>();

	/**
	 * Time interval (ms) to run the cleanup task; also used as the default TTL.
	 */
	private final long cleanupInterval;

	/**
	 * Timer task to execute the cleanup at regular intervals.
	 */
	private final Timer cleanupTimer;

	/**
	 * Instantiates a new BLOB cache.
	 *
	 * @param blobClientConfig
	 * 		global configuration
	 * @param serverAddress
	 * 		address of the {@link BlobServer} to use for fetching files from or {@code null} if none yet
	 * @throws IOException
	 * 		thrown if the (local or distributed) file storage cannot be created or is not usable
	 */
	public TransientBlobCache(
			final Configuration blobClientConfig,
			@Nullable final InetSocketAddress serverAddress) throws IOException {

		super(blobClientConfig, new VoidBlobStore(), LoggerFactory.getLogger(TransientBlobCache.class), serverAddress
		);

		// Initializing the clean up task
		this.cleanupTimer = new Timer(true);

		this.cleanupInterval = blobClientConfig.getLong(BlobServerOptions.CLEANUP_INTERVAL) * 1000;
		this.cleanupTimer
			.schedule(new TransientBlobCleanupTask(blobExpiryTimes, readWriteLock.writeLock(),
				storageDir, log), cleanupInterval, cleanupInterval);
	}

	@Override
	public File getFile(TransientBlobKey key) throws IOException {
		return getFileInternal(null, key);
	}

	@Override
	public File getFile(JobID jobId, TransientBlobKey key) throws IOException {
		checkNotNull(jobId);
		return getFileInternal(jobId, key);
	}

	@Override
	protected File getFileInternal(@Nullable JobID jobId, BlobKey blobKey) throws IOException {
		File file = super.getFileInternal(jobId, blobKey);

		readWriteLock.readLock().lock();
		try {
			// regarding concurrent operations, it is not really important which timestamp makes
			// it into the map as they are close to each other anyway, also we can simply
			// overwrite old values as long as we are in the read (or write) lock
			blobExpiryTimes.put(Tuple2.of(jobId, (TransientBlobKey) blobKey),
				System.currentTimeMillis() + cleanupInterval);
		} finally {
			readWriteLock.readLock().unlock();
		}

		return file;
	}

	@Override
	public TransientBlobKey putTransient(byte[] value) throws IOException {
		try (BlobClient bc = createClient()) {
			return (TransientBlobKey) bc.putBuffer(null, value, 0, value.length, TRANSIENT_BLOB);
		}
	}

	@Override
	public TransientBlobKey putTransient(JobID jobId, byte[] value) throws IOException {
		checkNotNull(jobId);
		try (BlobClient bc = createClient()) {
			return (TransientBlobKey) bc.putBuffer(jobId, value, 0, value.length, TRANSIENT_BLOB);
		}
	}

	@Override
	public TransientBlobKey putTransient(InputStream inputStream) throws IOException {
		try (BlobClient bc = createClient()) {
			return (TransientBlobKey) bc.putInputStream(null, inputStream, TRANSIENT_BLOB);
		}
	}

	@Override
	public TransientBlobKey putTransient(JobID jobId, InputStream inputStream) throws IOException {
		checkNotNull(jobId);
		try (BlobClient bc = createClient()) {
			return (TransientBlobKey) bc.putInputStream(jobId, inputStream, TRANSIENT_BLOB);
		}
	}

	@Override
	public boolean deleteFromCache(TransientBlobKey key) {
		return deleteInternal(null, key);
	}

	@Override
	public boolean deleteFromCache(JobID jobId, TransientBlobKey key) {
		checkNotNull(jobId);
		return deleteInternal(jobId, key);
	}

	/**
	 * Deletes the file associated with the blob key in this BLOB cache.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @return  <tt>true</tt> if the given blob is successfully deleted or non-existing;
	 *          <tt>false</tt> otherwise
	 */
	private boolean deleteInternal(@Nullable JobID jobId, TransientBlobKey key) {
		final File localFile =
			new File(BlobUtils.getStorageLocationPath(storageDir.getAbsolutePath(), jobId, key));

		readWriteLock.writeLock().lock();
		try {
			if (!localFile.delete() && localFile.exists()) {
				log.warn("Failed to delete locally cached BLOB {} at {}", key,
					localFile.getAbsolutePath());
				return false;
			} else {
				// this needs to happen inside the write lock in case of concurrent getFile() calls
				blobExpiryTimes.remove(Tuple2.of(jobId, key));
			}
		} finally {
			readWriteLock.writeLock().unlock();
		}
		return true;
	}

	/**
	 * Returns the blob expiry times - for testing purposes only!
	 *
	 * @return blob expiry times (internal state!)
	 */
	@VisibleForTesting
	ConcurrentMap<Tuple2<JobID, TransientBlobKey>, Long> getBlobExpiryTimes() {
		return blobExpiryTimes;
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
	public File getStorageLocation(@Nullable JobID jobId, BlobKey key) throws IOException {
		return BlobUtils.getStorageLocation(storageDir, jobId, key);
	}

	private BlobClient createClient() throws IOException {
		final InetSocketAddress currentServerAddress = serverAddress;

		if (currentServerAddress != null) {
			return new BlobClient(currentServerAddress, blobClientConfig);
		} else {
			throw new IOException("Could not create BlobClient because the BlobServer address is unknown.");
		}
	}

	@Override
	protected void cancelCleanupTask() {
		cleanupTimer.cancel();
	}
}
