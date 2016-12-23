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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The BLOB cache implements a local cache for content-addressable BLOBs.
 *
 * <p>When requesting BLOBs through the {@link BlobCache#getURL} methods, the
 * BLOB cache will first attempt to serve the file from a distributed file
 * system (if available, i.e. high availability is set) or from its local
 * cache. If neither contains the desired BLOB, the BLOB cache will try to
 * download it from the BLOB server.</p>
 */
public final class BlobCache implements BlobService {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(BlobCache.class);

	private final InetSocketAddress serverAddress;

	/** The blob store. */
	private final BlobStore blobStore;

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
	 * @param serverAddress address of the {@link BlobServer} to use for
	 *                      fetching files from
	 * @param blobClientConfig global configuration
	 * @throws IOException
	 *         thrown if the file storage cannot be created or is not usable
	 */
	public BlobCache(InetSocketAddress serverAddress, Configuration blobClientConfig) throws IOException {
		if (serverAddress == null || blobClientConfig == null) {
			throw new NullPointerException();
		}

		this.serverAddress = serverAddress;

		this.blobClientConfig = blobClientConfig;

		// configure and create the storage directory
		this.blobStore = new FileSystemBlobStore(blobClientConfig, false);

		// configure the number of fetch retries
		final int fetchRetries = blobClientConfig.getInteger(
			ConfigConstants.BLOB_FETCH_RETRIES_KEY, ConfigConstants.DEFAULT_BLOB_FETCH_RETRIES);
		if (fetchRetries >= 0) {
			this.numFetchRetries = fetchRetries;
		}
		else {
			LOG.warn("Invalid value for {}. System will attempt no retires on failed fetches of BLOBs.",
				ConfigConstants.BLOB_FETCH_RETRIES_KEY);
			this.numFetchRetries = 0;
		}

		// Add shutdown hook to delete storage directory
		shutdownHook = BlobUtils.addShutdownHook(this, LOG);
	}

	/**
	 * Returns the URL for the BLOB with the given key. The method will first attempt to serve
	 * the BLOB from its local cache. If the BLOB is not in the cache, the method will try to download it
	 * from this cache's BLOB server.
	 *
	 * @param requiredBlob The key of the desired BLOB.
	 * @return URL referring to the local storage location of the BLOB.
	 * @throws IOException Thrown if an I/O error occurs if a file is not
	 *                     available or while downloading the BLOBs from the
	 *                     BLOB server.
	 */
	public URL getURL(final BlobKey requiredBlob) throws IOException {
		if (requiredBlob == null) {
			throw new IllegalArgumentException("BLOB key cannot be null.");
		}

		try {
			return blobStore.getFileStatus(requiredBlob).getPath().toUri().toURL();
		} catch (FileNotFoundException fnfe) {
			if (blobStore.isDistributed()) {
				// the blob server only has access to the same file system and
				// the file is simply not there!
				throw fnfe;
			}

			final byte[] buf = new byte[BlobServerProtocol.BUFFER_SIZE];

			// loop over retries
			int attempt = 0;
			while (true) {

				if (attempt == 0) {
					LOG.info("Downloading {} from {}", requiredBlob, serverAddress);
				} else {
					LOG.info("Downloading {} from {} (retry {})", requiredBlob, serverAddress, attempt);
				}

				try {
					BlobClient bc = null;
					InputStream is = null;
					String incomingFile = null;
					FSDataOutputStream fos = null;

					try {
						bc = new BlobClient(serverAddress, blobClientConfig);
						is = bc.get(requiredBlob);
						incomingFile = blobStore.getTempFilename();
						fos = blobStore.createTempFile(incomingFile);

						while (true) {
							final int read = is.read(buf);
							if (read < 0) {
								break;
							}
							fos.write(buf, 0, read);
						}
						blobStore.persistTempFile(incomingFile, requiredBlob);
						incomingFile = null;

						// we do explicitly not use a finally block, because we want the closing
						// in the regular case to throw exceptions and cause the writing to fail.
						// But, the closing on exception should not throw further exceptions and
						// let us keep the root exception
						fos.close();
						fos = null;
						is.close();
						is = null;
						bc.close();
						bc = null;

						// success, we finished
						break;
					}
					catch (Throwable t) {
						// we use "catch (Throwable)" to keep the root exception. Otherwise that exception
						// it would be replaced by any exception thrown in the finally block
						closeSilently(fos);
						closeSilently(is);
						closeSilently(bc);
						if (incomingFile != null) {
							try {
								blobStore.deleteTempFile(incomingFile);
							} catch (Throwable t1) {
								LOG.warn("Cannot remove temporary file", t1);
							}
						}

						if (t instanceof IOException) {
							throw (IOException) t;
						} else {
							throw new IOException(t.getMessage(), t);
						}
					}
				}
				catch (IOException e) {
					String message = "Failed to fetch BLOB " + requiredBlob + " from " + serverAddress +
						" and store it under " + blobStore.getBasePath();
					if (attempt < numFetchRetries) {
						attempt++;
						if (LOG.isDebugEnabled()) {
							LOG.debug(message + " Retrying...", e);
						} else {
							LOG.error(message + " Retrying...");
						}
					}
					else {
						LOG.error(message + " No retries left.", e);
						throw new IOException(message, e);
					}
				}
			} // end loop over retries
		}

		return blobStore.getFileStatus(requiredBlob).getPath().toUri().toURL();
	}

	/**
	 * Deletes the file associated with the given key from the BLOB cache.
	 *
	 * @param key referring to the file to be deleted
	 * @return <tt>true</tt> if the delete was successful or the file never
	 *         existed; <tt>false</tt> otherwise
	 */
	@Override
	public boolean delete(BlobKey key) {
		return blobStore.delete(key);
	}

	/**
	 * Deletes the file associated with the given key from the BLOB cache and
	 * BLOB server.
	 *
	 * @param key referring to the file to be deleted
	 * @throws IOException
	 *         thrown if an I/O error occurs while transferring the request to
	 *         the BLOB server or if the BLOB server cannot delete the file
	 */
	public void deleteGlobal(BlobKey key) throws IOException {
		BlobClient bc = createClient();
		try {
			delete(key);
			bc.delete(key);
		}
		finally {
			bc.close();
		}
	}

	@Override
	public int getPort() {
		return serverAddress.getPort();
	}

	@Override
	public void shutdown() {
		if (shutdownRequested.compareAndSet(false, true)) {
			LOG.info("Shutting down BlobCache");

			// Clean up the storage directory
			blobStore.cleanUp();

			// Remove shutdown hook to prevent resource leaks, unless this is invoked by the shutdown hook itself
			if (shutdownHook != null && shutdownHook != Thread.currentThread()) {
				try {
					Runtime.getRuntime().removeShutdownHook(shutdownHook);
				}
				catch (IllegalStateException e) {
					// race, JVM is in shutdown already, we can safely ignore this
				}
				catch (Throwable t) {
					LOG.warn("Exception while unregistering BLOB cache's cleanup shutdown hook.");
				}
			}
		}
	}

	@Override
	public BlobClient createClient() throws IOException {
		return new BlobClient(serverAddress, blobClientConfig);
	}

	public String getStorageDir() {
		return this.blobStore.getBasePath();
	}

	// ------------------------------------------------------------------------
	//  Miscellaneous
	// ------------------------------------------------------------------------

	private void closeSilently(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (Throwable t) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Error while closing resource after BLOB transfer.", t);
				}
			}
		}
	}
}
