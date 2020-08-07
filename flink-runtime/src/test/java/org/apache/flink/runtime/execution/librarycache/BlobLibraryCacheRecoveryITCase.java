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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Integration test for {@link BlobLibraryCacheManager}.
 */
public class BlobLibraryCacheRecoveryITCase extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	/**
	 * Tests that with {@link HighAvailabilityMode#ZOOKEEPER} distributed JARs are recoverable from any
	 * participating BlobLibraryCacheManager.
	 */
	@Test
	public void testRecoveryRegisterAndDownload() throws Exception {
		Random rand = new Random();

		BlobServer[] server = new BlobServer[2];
		InetSocketAddress[] serverAddress = new InetSocketAddress[2];
		BlobLibraryCacheManager[] libServer = new BlobLibraryCacheManager[2];
		PermanentBlobCache cache = null;
		BlobStoreService blobStoreService = null;

		Configuration config = new Configuration();
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setLong(BlobServerOptions.CLEANUP_INTERVAL, 3_600L);

		try {
			blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

			final BlobLibraryCacheManager.ClassLoaderFactory classLoaderFactory = BlobLibraryCacheManager.defaultClassLoaderFactory(
				FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
				new String[0],
				null,
				true);

			for (int i = 0; i < server.length; i++) {
				server[i] = new BlobServer(config, blobStoreService);
				server[i].start();
				serverAddress[i] = new InetSocketAddress("localhost", server[i].getPort());
				libServer[i] = new BlobLibraryCacheManager(
					server[i],
					classLoaderFactory);
			}

			// Random data
			byte[] expected = new byte[1024];
			rand.nextBytes(expected);

			ArrayList<PermanentBlobKey> keys = new ArrayList<>(2);

			JobID jobId = new JobID();

			// Upload some data (libraries)
			keys.add(server[0].putPermanent(jobId, expected)); // Request 1
			byte[] expected2 = Arrays.copyOfRange(expected, 32, 288);
			keys.add(server[0].putPermanent(jobId, expected2)); // Request 2

			// The cache
			cache = new PermanentBlobCache(config, blobStoreService, serverAddress[0]);

			// Register uploaded libraries
			final LibraryCacheManager.ClassLoaderLease classLoaderLease = libServer[0].registerClassLoaderLease(jobId);
			classLoaderLease.getOrResolveClassLoader(keys, Collections.emptyList());

			// Verify key 1
			File f = cache.getFile(jobId, keys.get(0));
			assertEquals(expected.length, f.length());

			try (FileInputStream fis = new FileInputStream(f)) {
				for (int i = 0; i < expected.length && fis.available() > 0; i++) {
					assertEquals(expected[i], (byte) fis.read());
				}

				assertEquals(0, fis.available());
			}

			// Shutdown cache and start with other server
			cache.close();

			cache = new PermanentBlobCache(config, blobStoreService, serverAddress[1]);

			// Verify key 1
			f = cache.getFile(jobId, keys.get(0));
			assertEquals(expected.length, f.length());

			try (FileInputStream fis = new FileInputStream(f)) {
				for (int i = 0; i < expected.length && fis.available() > 0; i++) {
					assertEquals(expected[i], (byte) fis.read());
				}

				assertEquals(0, fis.available());
			}

			// Verify key 2
			f = cache.getFile(jobId, keys.get(1));
			assertEquals(expected2.length, f.length());

			try (FileInputStream fis = new FileInputStream(f)) {
				for (int i = 0; i < 256 && fis.available() > 0; i++) {
					assertEquals(expected2[i], (byte) fis.read());
				}

				assertEquals(0, fis.available());
			}

			// Remove blobs again
			server[1].cleanupJob(jobId, true);

			// Verify everything is clean below recoveryDir/<cluster_id>
			final String clusterId = config.getString(HighAvailabilityOptions.HA_CLUSTER_ID);
			String haBlobStorePath = config.getString(HighAvailabilityOptions.HA_STORAGE_PATH);
			File haBlobStoreDir = new File(haBlobStorePath, clusterId);
			File[] recoveryFiles = haBlobStoreDir.listFiles();
			assertNotNull("HA storage directory does not exist", recoveryFiles);
			assertEquals("Unclean state backend: " + Arrays.toString(recoveryFiles), 0, recoveryFiles.length);
		}
		finally {
			for (BlobLibraryCacheManager s : libServer) {
				if (s != null) {
					s.shutdown();
				}
			}
			for (BlobServer s : server) {
				if (s != null) {
					s.close();
				}
			}

			if (cache != null) {
				cache.close();
			}

			if (blobStoreService != null) {
				blobStoreService.closeAndCleanupAllData();
			}
		}
	}
}
