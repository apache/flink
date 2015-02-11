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

import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The BLOB cache implements a local cache for content-addressable BLOBs. When requesting BLOBs through the
 * {@link BlobCache#getURL} methods, the BLOB cache will first attempt serve the file from its local cache. Only if the
 * local cache does not contain the desired BLOB, the BLOB cache will try to download it from the BLOB server.
 */
public final class BlobCache implements BlobService {

	/**
	 * The log object used for debugging.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(BlobCache.class);

	private final InetSocketAddress serverAddress;

	private final File storageDir;

	private final AtomicBoolean shutdownRequested = new AtomicBoolean();

	/** Shutdown hook thread to ensure deletion of the storage directory. */
	private final Thread shutdownHook;


	public BlobCache(InetSocketAddress serverAddress, Configuration configuration) {
		if (serverAddress == null || configuration == null) {
			throw new NullPointerException();
		}

		this.serverAddress = serverAddress;

		String storageDirectory = configuration.getString(ConfigConstants.BLOB_STORAGE_DIRECTORY_KEY, null);
		this.storageDir = BlobUtils.initStorageDirectory(storageDirectory);
		LOG.info("Created BLOB cache storage directory " + storageDir);

		// Add shutdown hook to delete storage directory
		shutdownHook = BlobUtils.addShutdownHook(this, LOG);
	}

	/**
	 * Returns the URL for the content-addressable BLOB with the given key. The method will first attempt to serve
	 * the BLOB from its local cache. If one or more BLOB are not in the cache, the method will try to download them
	 * from the BLOB server with the given address.
	 * 
	 * @param requiredBlob
	 *        the key of the desired content-addressable BLOB
	 * @return URL referring to the local storage location of the BLOB
	 * @throws IOException
	 *         thrown if an I/O error occurs while downloading the BLOBs from the BLOB server
	 */
	public URL getURL(final BlobKey requiredBlob) throws IOException {
		if (requiredBlob == null) {
			throw new IllegalArgumentException("Required BLOB cannot be null.");
		}

		BlobClient bc = null;
		byte[] buf = null;
		URL url = null;

		try {
			final File localJarFile = BlobUtils.getStorageLocation(storageDir, requiredBlob);

			if (!localJarFile.exists()) {

				if (LOG.isDebugEnabled()) {
					LOG.debug("Trying to download " + requiredBlob + " from " + serverAddress);
				}

				bc = new BlobClient(serverAddress);
				buf = new byte[BlobServer.BUFFER_SIZE];

				InputStream is = null;
				OutputStream os = null;
				try {
					is = bc.get(requiredBlob);
					os = new FileOutputStream(localJarFile);

					while (true) {

						final int read = is.read(buf);
						if (read < 0) {
							break;
						}

						os.write(buf, 0, read);
					}
				} finally {
					if (is != null) {
						is.close();
					}
					if (os != null) {
						os.close();
					}
				}
			}
			url = localJarFile.toURI().toURL();


		} finally {
			if (bc != null) {
				bc.close();
			}
		}

		return url;
	}

	/**
	 * Deletes the file associated with the given key from the BLOB cache.
	 * @param key referring to the file to be deleted
	 */
	public void delete(BlobKey key) throws IOException{
		final File localFile = BlobUtils.getStorageLocation(storageDir, key);

		if(localFile.exists()) {
			localFile.delete();
		}
	}

	@Override
	public int getPort() {
		return serverAddress.getPort();
	}

	@Override
	public void shutdown() {
		if (shutdownRequested.compareAndSet(false, true)) {
			// Clean up the storage directory
			try {
				FileUtils.deleteDirectory(storageDir);
			}
			catch (IOException e) {
				LOG.error("BLOB cache failed to properly clean up its storage directory.");
			}

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
}
