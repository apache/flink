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

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.RecoveryMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class BlobRecoveryITCase {

	private File recoveryDir;

	@Before
	public void setUp() throws Exception {
		recoveryDir = new File(FileUtils.getTempDirectory(), "BlobRecoveryITCaseDir");
		if (!recoveryDir.exists() && !recoveryDir.mkdirs()) {
			throw new IllegalStateException("Failed to create temp directory for test");
		}
	}

	@After
	public void cleanUp() throws Exception {
		if (recoveryDir != null) {
			FileUtils.deleteDirectory(recoveryDir);
		}
	}

	/**
	 * Tests that with {@link RecoveryMode#ZOOKEEPER} distributed JARs are recoverable from any
	 * participating BlobServer.
	 */
	@Test
	public void testBlobServerRecovery() throws Exception {
		Random rand = new Random();

		BlobServer[] server = new BlobServer[2];
		InetSocketAddress[] serverAddress = new InetSocketAddress[2];
		BlobClient client = null;

		try {
			Configuration config = new Configuration();
			config.setString(ConfigConstants.RECOVERY_MODE, "ZOOKEEPER");
			config.setString(ConfigConstants.STATE_BACKEND, "FILESYSTEM");
			config.setString(ConfigConstants.STATE_BACKEND_FS_RECOVERY_PATH, recoveryDir.getPath());

			for (int i = 0; i < server.length; i++) {
				server[i] = new BlobServer(config);
				serverAddress[i] = new InetSocketAddress("localhost", server[i].getPort());
			}

			client = new BlobClient(serverAddress[0]);

			// Random data
			byte[] expected = new byte[1024];
			rand.nextBytes(expected);

			BlobKey[] keys = new BlobKey[2];

			// Put data
			keys[0] = client.put(expected); // Request 1
			keys[1] = client.put(expected, 32, 256); // Request 2

			JobID[] jobId = new JobID[] { new JobID(), new JobID() };
			String[] testKey = new String[] { "test-key-1", "test-key-2" };

			client.put(jobId[0], testKey[0], expected); // Request 3
			client.put(jobId[1], testKey[1], expected, 32, 256); // Request 4

			// Close the client and connect to the other server
			client.close();
			client = new BlobClient(serverAddress[1]);

			// Verify request 1
			try (InputStream is = client.get(keys[0])) {
				byte[] actual = new byte[expected.length];

				BlobUtils.readFully(is, actual, 0, expected.length, null);

				for (int i = 0; i < expected.length; i++) {
					assertEquals(expected[i], actual[i]);
				}
			}

			// Verify request 2
			try (InputStream is = client.get(keys[1])) {
				byte[] actual = new byte[256];
				BlobUtils.readFully(is, actual, 0, 256, null);

				for (int i = 32, j = 0; i < 256; i++, j++) {
					assertEquals(expected[i], actual[j]);
				}
			}

			// Verify request 3
			try (InputStream is = client.get(jobId[0], testKey[0])) {
				byte[] actual = new byte[expected.length];
				BlobUtils.readFully(is, actual, 0, expected.length, null);

				for (int i = 0; i < expected.length; i++) {
					assertEquals(expected[i], actual[i]);
				}
			}

			// Verify request 4
			try (InputStream is = client.get(jobId[1], testKey[1])) {
				byte[] actual = new byte[256];
				BlobUtils.readFully(is, actual, 0, 256, null);

				for (int i = 32, j = 0; i < 256; i++, j++) {
					assertEquals(expected[i], actual[j]);
				}
			}
		}
		finally {
			for (BlobServer s : server) {
				if (s != null) {
					s.shutdown();
				}
			}

			if (client != null) {
				client.close();
			}
		}

		// Verify everything is clean
		File[] recoveryFiles = recoveryDir.listFiles();
		assertEquals("Unclean state backend: " + Arrays.toString(recoveryFiles), 0, recoveryFiles.length);
	}
}
