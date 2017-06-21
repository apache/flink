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
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * This class contains unit tests for the {@link BlobCache}.
 */
public class BlobCacheSuccessTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * BlobCache with no HA, job-unrelated BLOBs. BLOBs need to be downloaded form a working
	 * BlobServer.
	 */
	@Test
	public void testBlobNoJobCache() throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		uploadFileGetTest(config, null, false, false);
	}

	/**
	 * BlobCache with no HA, job-related BLOBS. BLOBs need to be downloaded form a working
	 * BlobServer.
	 */
	@Test
	public void testBlobForJobCache() throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		uploadFileGetTest(config, new JobID(), false, false);
	}

	/**
	 * BlobCache is configured in HA mode and the cache can download files from
	 * the file system directly and does not need to download BLOBs from the
	 * BlobServer. Using job-unrelated BLOBs.
	 */
	@Test
	public void testBlobNoJobCacheHa() throws IOException {
		testBlobCacheHa(null);
	}

	/**
	 * BlobCache is configured in HA mode and the cache can download files from
	 * the file system directly and does not need to download BLOBs from the
	 * BlobServer. Using job-related BLOBs.
	 */
	@Test
	public void testBlobForJobCacheHa() throws IOException {
		testBlobCacheHa(new JobID());
	}

	private void testBlobCacheHa(final JobID jobId) throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.newFolder().getPath());
		uploadFileGetTest(config, jobId, true, true);
	}

	/**
	 * BlobCache is configured in HA mode but the cache itself cannot access the
	 * file system and thus needs to download BLOBs from the BlobServer. Using job-unrelated BLOBs.
	 */
	@Test
	public void testBlobNoJobCacheHaFallback() throws IOException {
		testBlobCacheHaFallback(null);
	}

	/**
	 * BlobCache is configured in HA mode but the cache itself cannot access the
	 * file system and thus needs to download BLOBs from the BlobServer. Using job-related BLOBs.
	 */
	@Test
	public void testBlobForJobCacheHaFallback() throws IOException {
		testBlobCacheHaFallback(new JobID());
	}

	private void testBlobCacheHaFallback(final JobID jobId) throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.newFolder().getPath());
		uploadFileGetTest(config, jobId, false, false);
	}

	private void uploadFileGetTest(final Configuration config, JobID jobId, boolean cacheWorksWithoutServer,
		boolean cacheHasAccessToFs) throws IOException {
		// First create two BLOBs and upload them to BLOB server
		final byte[] buf = new byte[128];
		final List<BlobKey> blobKeys = new ArrayList<BlobKey>(2);

		BlobServer blobServer = null;
		BlobCache blobCache = null;
		BlobStoreService blobStoreService = null;
		try {
			final Configuration cacheConfig = new Configuration(config);
			cacheConfig.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());
			if (!cacheHasAccessToFs) {
				// make sure the cache cannot access the HA store directly
				cacheConfig.setString(BlobServerOptions.STORAGE_DIRECTORY,
					temporaryFolder.newFolder().getAbsolutePath());
				cacheConfig.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
					temporaryFolder.newFolder().getPath() + "/does-not-exist");
			}

			blobStoreService = BlobUtils.createBlobStoreFromConfig(cacheConfig);

			// Start the BLOB server
			blobServer = new BlobServer(config, blobStoreService);
			final InetSocketAddress serverAddress = new InetSocketAddress(blobServer.getPort());

			// Upload BLOBs
			BlobClient blobClient = null;
			try {

				blobClient = new BlobClient(serverAddress, config);

				blobKeys.add(blobClient.put(jobId, buf));
				buf[0] = 1; // Make sure the BLOB key changes
				blobKeys.add(blobClient.put(jobId, buf));
			} finally {
				if (blobClient != null) {
					blobClient.close();
				}
			}

			if (cacheWorksWithoutServer) {
				// Now, shut down the BLOB server, the BLOBs must still be accessible through the cache.
				blobServer.close();
				blobServer = null;
			}

			blobCache = new BlobCache(serverAddress, cacheConfig, blobStoreService);

			for (BlobKey blobKey : blobKeys) {
				blobCache.getFile(jobId, blobKey);
			}

			if (blobServer != null) {
				// Now, shut down the BLOB server, the BLOBs must still be accessible through the cache.
				blobServer.close();
				blobServer = null;
			}

			final File[] files = new File[blobKeys.size()];

			for(int i = 0; i < blobKeys.size(); i++){
				files[i] = blobCache.getFile(jobId, blobKeys.get(i));
			}

			// Verify the result
			assertEquals(blobKeys.size(), files.length);

			for (final File file : files) {
				assertNotNull(file);

				assertTrue(file.exists());
				assertEquals(buf.length, file.length());
			}
		} finally {
			if (blobServer != null) {
				blobServer.close();
			}

			if(blobCache != null){
				blobCache.close();
			}

			if (blobStoreService != null) {
				blobStoreService.closeAndCleanupAllData();
			}
		}
	}
}
