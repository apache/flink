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

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;

import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;

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

		uploadFileGetTest(config, null, false, false, false);
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

		uploadFileGetTest(config, new JobID(), false, false, false);
	}

	/**
	 * BlobCache is configured in HA mode and the cache can download files from
	 * the file system directly and does not need to download BLOBs from the
	 * BlobServer. Using job-related BLOBs.
	 */
	@Test
	public void testBlobForJobCacheHa() throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.newFolder().getPath());

		uploadFileGetTest(config, new JobID(), true, true, true);
	}

	/**
	 * BlobCache is configured in HA mode but the cache itself cannot access the
	 * file system and thus needs to download BLOBs from the BlobServer. Using job-related BLOBs.
	 */
	@Test
	public void testBlobForJobCacheHaFallback() throws IOException {
		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.newFolder().getPath());

		uploadFileGetTest(config, new JobID(), false, false, true);
	}

	private void uploadFileGetTest(
			final Configuration config, @Nullable JobID jobId, boolean cacheWorksWithoutServer,
			boolean cacheHasAccessToFs, boolean highAvailability) throws IOException {

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

		// First create two BLOBs and upload them to BLOB server
		final byte[] data = new byte[128];
		byte[] data2 = Arrays.copyOf(data, data.length);
		data2[0] ^= 1;

		BlobStoreService blobStoreService = null;

		try {
			blobStoreService = BlobUtils.createBlobStoreFromConfig(cacheConfig);
			try (
				BlobServer server = new BlobServer(config, blobStoreService);
				BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
					cacheConfig, blobStoreService)) {

				server.start();

				// Upload BLOBs
				BlobKey key1 = put(server, jobId, data, highAvailability);
				BlobKey key2 = put(server, jobId, data2, highAvailability);

				if (cacheWorksWithoutServer) {
					// Now, shut down the BLOB server, the BLOBs must still be accessible through the cache.
					server.close();
				}

				verifyContents(cache, jobId, key1, data, highAvailability);
				verifyContents(cache, jobId, key2, data2, highAvailability);

				if (!cacheWorksWithoutServer) {
					// Now, shut down the BLOB server, the BLOBs must still be accessible through the cache.
					server.close();

					verifyContents(cache, jobId, key1, data, highAvailability);
					verifyContents(cache, jobId, key2, data2, highAvailability);
				}
			}
		} finally {

			if (blobStoreService != null) {
				blobStoreService.closeAndCleanupAllData();
			}
		}
	}
}
