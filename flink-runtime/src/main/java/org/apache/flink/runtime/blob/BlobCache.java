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
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The BLOB cache implements a local cache for content-addressable BLOBs.
 *
 * <p>When requesting BLOBs through the {@link BlobCache#getFile} methods, the
 * BLOB cache will first attempt to serve the file from its local cache. Only if
 * the local cache does not contain the desired BLOB, the BLOB cache will try to
 * download it from a distributed file system (if available) or the BLOB
 * server.</p>
 */
public final class BlobCache implements BlobService {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(BlobCache.class);

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

	/**
	 * Instantiates a new BLOB cache.
	 *
	 * @param serverAddress
	 * 		address of the {@link BlobServer} to use for fetching files from
	 * @param blobClientConfig
	 * 		global configuration
	 * @param blobView
	 * 		(distributed) blob store file system to retrieve files from first
	 *
	 * @throws IOException
	 * 		thrown if the (local or distributed) file storage cannot be created or is not usable
	 */
	public BlobCache(
			final InetSocketAddress serverAddress,
			final Configuration blobClientConfig,
			final BlobView blobView) throws IOException {
		this.serverAddress = checkNotNull(serverAddress);
		this.blobClientConfig = checkNotNull(blobClientConfig);
		this.blobView = checkNotNull(blobView, "blobStore");

		// configure and create the storage directory
		String storageDirectory = blobClientConfig.getString(BlobServerOptions.STORAGE_DIRECTORY);
		this.storageDir = BlobUtils.initLocalStorageDirectory(storageDirectory);
		LOG.info("Created BLOB cache storage directory " + storageDir);

		// configure the number of fetch retries
		final int fetchRetries = blobClientConfig.getInteger(BlobServerOptions.FETCH_RETRIES);
		if (fetchRetries >= 0) {
			this.numFetchRetries = fetchRetries;
		}
		else {
			LOG.warn("Invalid value for {}. System will attempt no retires on failed fetches of BLOBs.",
				BlobServerOptions.FETCH_RETRIES.key());
			this.numFetchRetries = 0;
		}

		// Add shutdown hook to delete storage directory
		shutdownHook = BlobUtils.addShutdownHook(this, LOG);
	}

	/**
	 * Returns local copy of the (job-unrelated) file for the BLOB with the given key.
	 * <p>
	 * The method will first attempt to serve the BLOB from its local cache. If the BLOB is not in
	 * the cache, the method will try to download it from this cache's BLOB server.
	 *
	 * @param key
	 * 		The key of the desired BLOB.
	 *
	 * @return file referring to the local storage location of the BLOB.
	 *
	 * @throws IOException
	 * 		Thrown if an I/O error occurs while downloading the BLOBs from the BLOB server.
	 */
	@Override
	public File getFile(BlobKey key) throws IOException {
		return getFileInternal(null, key);
	}

	/**
	 * Returns local copy of the file for the BLOB with the given key.
	 * <p>
	 * The method will first attempt to serve the BLOB from its local cache. If the BLOB is not in
	 * the cache, the method will try to download it from this cache's BLOB server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		The key of the desired BLOB.
	 *
	 * @return file referring to the local storage location of the BLOB.
	 *
	 * @throws IOException
	 * 		Thrown if an I/O error occurs while downloading the BLOBs from the BLOB server.
	 */
	@Override
	public File getFile(@Nonnull JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		return getFileInternal(jobId, key);
	}

	/**
	 * Returns local copy of the file for the BLOB with the given key.
	 * <p>
	 * The method will first attempt to serve the BLOB from its local cache. If the BLOB is not in
	 * the cache, the method will try to download it from this cache's BLOB server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param requiredBlob
	 * 		The key of the desired BLOB.
	 *
	 * @return file referring to the local storage location of the BLOB.
	 *
	 * @throws IOException
	 * 		Thrown if an I/O error occurs while downloading the BLOBs from the BLOB server.
	 */
	private File getFileInternal(@Nullable JobID jobId, BlobKey requiredBlob) throws IOException {
		checkArgument(requiredBlob != null, "BLOB key cannot be null.");

		final File localJarFile = BlobUtils.getStorageLocation(storageDir, jobId, requiredBlob);

		if (localJarFile.exists()) {
			return localJarFile;
		}

		// first try the distributed blob store (if available)
		try {
			blobView.get(jobId, requiredBlob, localJarFile);
		} catch (Exception e) {
			LOG.info("Failed to copy from blob store. Downloading from BLOB server instead.", e);
		}

		if (localJarFile.exists()) {
			return localJarFile;
		}

		// fallback: download from the BlobServer
		final byte[] buf = new byte[BlobServerProtocol.BUFFER_SIZE];
		LOG.info("Downloading {}/{} from {}", jobId, requiredBlob, serverAddress);

		// loop over retries
		int attempt = 0;
		while (true) {
			try (
				final BlobClient bc = new BlobClient(serverAddress, blobClientConfig);
				final InputStream is = bc.getInternal(jobId, requiredBlob);
				final OutputStream os = new FileOutputStream(localJarFile)
			) {
				while (true) {
					final int read = is.read(buf);
					if (read < 0) {
						break;
					}
					os.write(buf, 0, read);
				}

				// success, we finished
				return localJarFile;
			}
			catch (Throwable t) {
				String message = "Failed to fetch BLOB " + jobId + "/" + requiredBlob + " from " + serverAddress +
					" and store it under " + localJarFile.getAbsolutePath();
				if (attempt < numFetchRetries) {
					if (LOG.isDebugEnabled()) {
						LOG.debug(message + " Retrying...", t);
					} else {
						LOG.error(message + " Retrying...");
					}
				}
				else {
					LOG.error(message + " No retries left.", t);
					throw new IOException(message, t);
				}

				// retry
				++attempt;
				LOG.info("Downloading {}/{} from {} (retry {})", jobId, requiredBlob, serverAddress, attempt);
			}
		} // end loop over retries
	}

	/**
	 * Deletes the (job-unrelated) file associated with the blob key in this BLOB cache.
	 *
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @throws IOException
	 */
	@Override
	public void delete(BlobKey key) throws IOException {
		deleteInternal(null, key);
	}

	/**
	 * Deletes the file associated with the blob key in this BLOB cache.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @throws IOException
	 */
	@Override
	public void delete(@Nonnull JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		deleteInternal(jobId, key);
	}

	/**
	 * Deletes the file associated with the blob key in this BLOB cache.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @throws IOException
	 */
	private void deleteInternal(@Nullable JobID jobId, BlobKey key) throws IOException{
		final File localFile = BlobUtils.getStorageLocation(storageDir, jobId, key);
		if (!localFile.delete() && localFile.exists()) {
			LOG.warn("Failed to delete locally cached BLOB {} at {}", key, localFile.getAbsolutePath());
		}
	}

	/**
	 * Deletes the (job-unrelated) file associated with the given key from the BLOB cache and
	 * BLOB server.
	 *
	 * @param key
	 * 		referring to the file to be deleted
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while transferring the request to the BLOB server or if the
	 * 		BLOB server cannot delete the file
	 */
	public void deleteGlobal(BlobKey key) throws IOException {
		deleteGlobalInternal(null, key);
	}

	/**
	 * Deletes the file associated with the given key from the BLOB cache and BLOB server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to
	 * @param key
	 * 		referring to the file to be deleted
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while transferring the request to the BLOB server or if the
	 * 		BLOB server cannot delete the file
	 */
	public void deleteGlobal(@Nonnull JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		deleteGlobalInternal(jobId, key);
	}

	/**
	 * Deletes the file associated with the given key from the BLOB cache and
	 * BLOB server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key
	 * 		referring to the file to be deleted
	 *
	 * @throws IOException
	 * 		thrown if an I/O error occurs while transferring the request to the BLOB server or if the
	 * 		BLOB server cannot delete the file
	 */
	private void deleteGlobalInternal(@Nullable JobID jobId, BlobKey key) throws IOException {
		// delete locally
		deleteInternal(jobId, key);
		// then delete on the BLOB server
		// (don't use the distributed storage directly - this way the blob
		// server is aware of the delete operation, too)
		try (BlobClient bc = createClient()) {
			bc.deleteInternal(jobId, key);
		}
	}

	@Override
	public int getPort() {
		return serverAddress.getPort();
	}

	@Override
	public void close() throws IOException {
		if (shutdownRequested.compareAndSet(false, true)) {
			LOG.info("Shutting down BlobCache");

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

	@Override
	public BlobClient createClient() throws IOException {
		return new BlobClient(serverAddress, blobClientConfig);
	}

	public File getStorageDir() {
		return this.storageDir;
	}

}
