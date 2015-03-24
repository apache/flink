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

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class contains unit tests for the {@link BlobCache}.
 */
public class BlobCacheSuccessTest {

	@Test
	public void testBlobCache() {

		// First create two BLOBs and upload them to BLOB server
		final byte[] buf = new byte[128];
		final List<BlobKey> blobKeys = new ArrayList<BlobKey>(2);

		BlobServer blobServer = null;
		BlobCache blobCache = null;
		try {

			// Start the BLOB server
			blobServer = new BlobServer(new Configuration());
			final InetSocketAddress serverAddress = new InetSocketAddress(blobServer.getPort());

			// Upload BLOBs
			BlobClient blobClient = null;
			try {

				blobClient = new BlobClient(serverAddress);

				blobKeys.add(blobClient.put(buf));
				buf[0] = 1; // Make sure the BLOB key changes
				blobKeys.add(blobClient.put(buf));
			} finally {
				if (blobClient != null) {
					blobClient.close();
				}
			}

			blobCache = new BlobCache(serverAddress, new Configuration());

			for (BlobKey blobKey : blobKeys) {
				blobCache.getURL(blobKey);
			}

			// Now, shut down the BLOB server, the BLOBs must still be accessible through the cache.
			blobServer.shutdown();
			blobServer = null;

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
