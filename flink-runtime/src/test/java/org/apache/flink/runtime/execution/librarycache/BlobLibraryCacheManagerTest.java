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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.util.OperatingSystem;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.runtime.blob.BlobCacheCleanupTest.checkFileCountForJob;
import static org.apache.flink.runtime.blob.BlobCacheCleanupTest.checkFilesExist;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class BlobLibraryCacheManagerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testRegisterAndDownload() throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		JobID jobId = new JobID();
		BlobServer server = null;
		BlobCache cache = null;
		File cacheDir = null;
		try {
			// create the blob transfer services
			Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());
			config.setLong(ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL, 1_000_000L);


			server = new BlobServer(config, new VoidBlobStore());
			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			cache = new BlobCache(serverAddress, config, new VoidBlobStore());

			// upload some meaningless data to the server
			BlobClient uploader = new BlobClient(serverAddress, config);
			BlobKey dataKey1 = uploader.put(jobId, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
			BlobKey dataKey2 = uploader.put(jobId, new byte[]{11, 12, 13, 14, 15, 16, 17, 18});
			uploader.close();

			BlobLibraryCacheManager libCache = new BlobLibraryCacheManager(cache);
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			// register some BLOBs as libraries
			{
				Collection<BlobKey> keys = Collections.singleton(dataKey1);

				cache.registerJob(jobId);
				assertNotNull(libCache.getClassLoader(jobId, keys, Collections.<URL>emptyList()));
				assertEquals(1, checkFilesExist(jobId, keys, cache, true));
				checkFileCountForJob(2, jobId, server);
				checkFileCountForJob(1, jobId, cache);

				// un-register them again
				cache.releaseJob(jobId);

				// Don't fail if called again
				cache.releaseJob(jobId);

				// library is still cached (but not associated with job any more)
				checkFileCountForJob(2, jobId, server);
				checkFileCountForJob(1, jobId, cache);
			}

			// see BlobUtils for the directory layout
			cacheDir = cache.getStorageLocation(jobId, new BlobKey()).getParentFile();
			assertTrue(cacheDir.exists());

			// make sure no further blobs can be downloaded by removing the write
			// permissions from the directory
			assertTrue("Could not remove write permissions from cache directory", cacheDir.setWritable(false, false));

			// since we cannot download this library any more, this call should fail
			try {
				cache.registerJob(jobId);
				libCache.getClassLoader(jobId, Collections.singleton(dataKey2),
						Collections.<URL>emptyList());
				fail("This should fail with an IOException");
			}
			catch (IOException e) {
				// splendid!
				cache.releaseJob(jobId);
			}
		} finally {
			if (cacheDir != null) {
				if (!cacheDir.setWritable(true, false)) {
					System.err.println("Could not re-add write permissions to cache directory.");
				}
			}
			if (cache != null) {
				cache.close();
			}
			if (server != null) {
				server.close();
			}
		}
	}
}
