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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class BlobLibraryCacheRecoveryITCase {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();
	/**
	 * Tests that with {@link RecoveryMode#ZOOKEEPER} distributed JARs are recoverable from any
	 * participating BlobLibraryCacheManager.
	 */
	@Test
	public void testRecoveryRegisterAndDownload() throws Exception {
		Random rand = new Random();

		BlobServer[] server = new BlobServer[2];
		InetSocketAddress[] serverAddress = new InetSocketAddress[2];
		BlobLibraryCacheManager[] libServer = new BlobLibraryCacheManager[2];
		BlobCache cache = null;
		BlobLibraryCacheManager libCache = null;

		try {
			Configuration config = new Configuration();
			config.setString(ConfigConstants.RECOVERY_MODE, "ZOOKEEPER");
			config.setString(ConfigConstants.STATE_BACKEND, "FILESYSTEM");
			config.setString(ConfigConstants.STATE_BACKEND_FS_RECOVERY_PATH, temporaryFolder.getRoot().getAbsolutePath());

			for (int i = 0; i < server.length; i++) {
				server[i] = new BlobServer(config);
				serverAddress[i] = new InetSocketAddress("localhost", server[i].getPort());
				libServer[i] = new BlobLibraryCacheManager(server[i], 3600 * 1000);
			}

			// Random data
			byte[] expected = new byte[1024];
			rand.nextBytes(expected);

			List<BlobKey> keys = new ArrayList<>(2);

			// Upload some data (libraries)
			try (BlobClient client = new BlobClient(serverAddress[0])) {
				keys.add(client.put(expected)); // Request 1
				keys.add(client.put(expected, 32, 256)); // Request 2
			}

			// The cache
			cache = new BlobCache(serverAddress[0], config);
			libCache = new BlobLibraryCacheManager(cache, 3600 * 1000);

			// Register uploaded libraries
			JobID jobId = new JobID();
			ExecutionAttemptID executionId = new ExecutionAttemptID();
			libServer[0].registerTask(jobId, executionId, keys, Collections.<URL>emptyList());

			// Verify key 1
			File f = libCache.getFile(keys.get(0));
			assertEquals(expected.length, f.length());

			try (FileInputStream fis = new FileInputStream(f)) {
				for (int i = 0; i < expected.length && fis.available() > 0; i++) {
					assertEquals(expected[i], (byte) fis.read());
				}

				assertEquals(0, fis.available());
			}

			// Shutdown cache and start with other server
			cache.shutdown();
			libCache.shutdown();

			cache = new BlobCache(serverAddress[1], config);
			libCache = new BlobLibraryCacheManager(cache, 3600 * 1000);

			// Verify key 1
			f = libCache.getFile(keys.get(0));
			assertEquals(expected.length, f.length());

			try (FileInputStream fis = new FileInputStream(f)) {
				for (int i = 0; i < expected.length && fis.available() > 0; i++) {
					assertEquals(expected[i], (byte) fis.read());
				}

				assertEquals(0, fis.available());
			}

			// Verify key 2
			f = libCache.getFile(keys.get(1));
			assertEquals(256, f.length());

			try (FileInputStream fis = new FileInputStream(f)) {
				for (int i = 0; i < 256 && fis.available() > 0; i++) {
					assertEquals(expected[32 + i], (byte) fis.read());
				}

				assertEquals(0, fis.available());
			}
		}
		finally {
			for (BlobServer s : server) {
				if (s != null) {
					s.shutdown();
				}
			}

			if (cache != null) {
				cache.shutdown();
			}

			if (libCache != null) {
				libCache.shutdown();
			}
		}

		// Verify everything is clean
		File[] recoveryFiles = temporaryFolder.getRoot().listFiles();
		assertEquals("Unclean state backend: " + Arrays.toString(recoveryFiles), 0, recoveryFiles.length);
	}
}
