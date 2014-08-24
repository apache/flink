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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The BLOB cache implements a local cache for content-addressable BLOBs. When requesting BLOBs through the
 * {@link BlobCache#getURLs} methods, the BLOB cache will first attempt serve the file from its local cache. Only if the
 * local cache does not contain the desired BLOB, the BLOB cache will try to download it from the BLOB server.
 * <p>
 * This class is thread-safe.
 */
public final class BlobCache {

	/**
	 * The log object used for debugging.
	 */
	private static final Log LOG = LogFactory.getLog(BlobCache.class);

	/**
	 * Private constructor to prevent instantiation.
	 */
	private BlobCache() {
	}

	/**
	 * Returns the URLs for the content-addressable BLOBs with the given keys. The method will first attempt to serve
	 * the BLOBs from its local cache. If one or more BLOBs are not in the cache, the method will try to download them
	 * from the BLOB server with the given address.
	 * 
	 * @param serverAddress
	 *        the address of the BLOB server.
	 * @param requiredJarFiles
	 *        the keys of the desired content-addressable BLOB
	 * @return an array of URLs referring to the local storage locations of the BLOBs
	 * @throws IOException
	 *         thrown if an I/O error occurs while downloading the BLOBs from the BLOB server
	 */
	public static URL[] getURLs(final InetSocketAddress serverAddress, final Collection<BlobKey> requiredBlobs)
			throws IOException {

		if (requiredBlobs == null || requiredBlobs.isEmpty()) {
			return new URL[0];
		}

		final URL[] urls = new URL[requiredBlobs.size()];
		int count = 0;
		BlobClient bc = null;
		byte[] buf = null;

		try {
			for (final Iterator<BlobKey> it = requiredBlobs.iterator(); it.hasNext();) {

				final BlobKey blobKey = it.next();
				final File localJarFile = BlobServer.getStorageLocation(blobKey);

				if (!localJarFile.exists()) {

					if (LOG.isDebugEnabled()) {
						LOG.debug("Trying to download " + blobKey + " from " + serverAddress);
					}

					if (bc == null) {

						if (serverAddress == null) {
							throw new IllegalArgumentException(
								"Argument serverAddress is null: Cannot download libraries from BLOB server");
						}

						bc = new BlobClient(serverAddress);
						buf = new byte[BlobServer.BUFFER_SIZE];
					}

					InputStream is = null;
					OutputStream os = null;
					try {
						is = bc.get(blobKey);
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
				urls[count++] = localJarFile.toURI().toURL();

			}
		} finally {
			if (bc != null) {
				bc.close();
			}
		}

		return urls;
	}
}
