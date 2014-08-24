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

	public static URL[] getURLs(final InetSocketAddress serverAddress, final Collection<BlobKey> requiredJarFiles)
			throws IOException {

		if (requiredJarFiles == null || requiredJarFiles.isEmpty()) {
			return new URL[0];
		}

		final URL[] urls = new URL[requiredJarFiles.size()];
		int count = 0;
		BlobClient bc = null;
		byte[] buf = null;

		try {
			for (final Iterator<BlobKey> it = requiredJarFiles.iterator(); it.hasNext();) {

				final BlobKey jarFileKey = it.next();
				final File localJarFile = BlobServer.getStorageLocation(jarFileKey);

				if (!localJarFile.exists()) {

					if (LOG.isDebugEnabled()) {
						LOG.debug("Trying to download " + jarFileKey + " from " + serverAddress);
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
						is = bc.get(jarFileKey);
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
