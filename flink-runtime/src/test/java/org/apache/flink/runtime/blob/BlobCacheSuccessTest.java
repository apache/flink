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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class contains unit tests for the {@link BlobCache}.
 */
public class BlobCacheSuccessTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * BlobCache with no HA. BLOBs need to be downloaded form a working
	 * BlobServer.
	 */
	@Test
	public void testBlobCache() {
		Configuration config = new Configuration();
		uploadFileGetTest(config, false, false);
	}

	/**
	 * BlobCache is configured in HA mode and the cache can download files from
	 * the file system directly and does not need to download BLOBs from the
	 * BlobServer.
	 */
	@Test
	public void testBlobCacheHa() {
		Configuration config = new Configuration();
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.getRoot().getPath());
		uploadFileGetTest(config, true, true);
	}

	/**
	 * BlobCache is configured in HA mode but the cache itself cannot access the
	 * file system and thus needs to download BLOBs from the BlobServer.
	 */
	@Test
	public void testBlobCacheHaFallback() {
		Configuration config = new Configuration();
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.getRoot().getPath());
		uploadFileGetTest(config, false, false);
	}

	private void uploadFileGetTest(final Configuration config, boolean cacheWorksWithoutServer,
		boolean cacheHasAccessToFs) {
		// First create two BLOBs and upload them to BLOB server
		final byte[] buf = new byte[128];
		final List<BlobKey> blobKeys = new ArrayList<BlobKey>(2);

		BlobServer blobServer = null;
		BlobCache blobCache = null;
		try {

			// Start the BLOB server
			blobServer = new BlobServer(config);
			final InetSocketAddress serverAddress = new InetSocketAddress(blobServer.getPort());

			// Upload BLOBs
			BlobClient blobClient = null;
			try {

				blobClient = new BlobClient(serverAddress, config);

				blobKeys.add(blobClient.put(buf));
				buf[0] = 1; // Make sure the BLOB key changes
				blobKeys.add(blobClient.put(buf));
			} finally {
				if (blobClient != null) {
					blobClient.close();
				}
			}

			if (cacheWorksWithoutServer) {
				// Now, shut down the BLOB server, the BLOBs must still be accessible through the cache.
				blobServer.shutdown();
				blobServer = null;
			}

			final Configuration cacheConfig;
			if (cacheHasAccessToFs) {
				cacheConfig = config;
			} else {
				// just in case parameters are still read from the server,
				// create a separate configuration object for the cache
				cacheConfig = new Configuration(config);
				cacheConfig.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
					temporaryFolder.getRoot().getPath() + "/does-not-exist");
			}

			blobCache = new BlobCache(serverAddress, cacheConfig);

			for (BlobKey blobKey : blobKeys) {
				blobCache.getURL(blobKey);
			}

			if (blobServer != null) {
				// Now, shut down the BLOB server, the BLOBs must still be accessible through the cache.
				blobServer.shutdown();
				blobServer = null;
			}

			final URL[] urls = new URL[blobKeys.size()];

			for(int i = 0; i < blobKeys.size(); i++){
				urls[i] = blobCache.getURL(blobKeys.get(i));
			}

			// Verify the result
			assertEquals(blobKeys.size(), urls.length);

			for (final URL url : urls) {

				assertNotNull(url);

				try {
					final File cachedFile = new File(url.toURI());

					assertTrue(cachedFile.exists());
					assertEquals(buf.length, cachedFile.length());

				} catch (URISyntaxException e) {
					fail(e.getMessage());
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (blobServer != null) {
				blobServer.shutdown();
			}

			if(blobCache != null){
				blobCache.shutdown();
			}
		}
	}
}
