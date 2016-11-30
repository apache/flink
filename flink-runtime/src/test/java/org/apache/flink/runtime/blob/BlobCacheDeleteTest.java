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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This class contains unit tests for the {@link BlobCache}.
 */
public class BlobCacheDeleteTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests that with {@link HighAvailabilityMode#ZOOKEEPER} distributed JARs
	 * are not deletable from a {@link BlobCache}.
	 *
	 * When using a shared directory, only the blob server is allowed to delete
	 * BLOBs. Here, we try on the blob cache and expect it to fail but also
	 * verify that the server is able to delete the BLOB.
	 */
	@Test
	public void testBlobCacheDeleteHA() throws Exception {
		Random rand = new Random();

		BlobServer server = null;
		BlobClient client = null;
		BlobCache cache = null;

		Configuration config = new Configuration();
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(ConfigConstants.STATE_BACKEND, "FILESYSTEM");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.getRoot().getPath());

		try {
			server = new BlobServer(config);
			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());

			client = new BlobClient(serverAddress, config);

			// Random data
			byte[] expected = new byte[1024];
			rand.nextBytes(expected);

			// Put data
			BlobKey key = client.put(expected);
			assertNotNull(key);

			// Close the client
			client.close();
			client = null;

			// try to access the file from the blob cache
			cache = new BlobCache(serverAddress, config);
			URL url = cache.getURL(key);

			// try to delete the file at the blob cache
			assertFalse("BlobCache should not be able to delete files on " +
				"distributed file systems", cache.delete(key));

			assertEquals(url, cache.getURL(key));

			// try to delete at the server and verify the file is gone
			try {
				cache.deleteGlobal(key);
			} catch (IOException e) {
				fail("BlobServer should be able to delete files on distributed" +
					"file systems");
			}
			try {
				cache.getURL(key);
				fail("deleted BLOB is still available");
			} catch (FileNotFoundException e) {
				// ignore
			}
		}
		finally {
			if (server != null) {
				server.shutdown();
			}

			if (client != null) {
				client.close();
			}

			if (cache != null) {
				cache.shutdown();
			}
		}

		// Verify everything is clean below recoveryDir/<cluster_id>
		final String clusterId = config.getString(HighAvailabilityOptions.HA_CLUSTER_ID);
		File haBlobStoreDir = new File(temporaryFolder.getRoot(), clusterId);
		File[] recoveryFiles = haBlobStoreDir.listFiles();
		assertNotNull("HA storage directory does not exist", recoveryFiles);
		assertEquals("Unclean state backend: " + Arrays.toString(recoveryFiles),
			0, recoveryFiles.length);
	}
}
