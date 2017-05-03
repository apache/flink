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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BlobRecoveryITCase extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests that with {@link HighAvailabilityMode#ZOOKEEPER} distributed JARs are recoverable from any
	 * participating BlobServer.
	 */
	@Test
	public void testBlobServerRecovery() throws Exception {
		Configuration config = new Configuration();
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(CoreOptions.STATE_BACKEND, "FILESYSTEM");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.getRoot().getPath());

		testBlobServerRecovery(config);
	}

	public static void testBlobServerRecovery(final Configuration config) throws IOException {
		final String clusterId = config.getString(HighAvailabilityOptions.HA_CLUSTER_ID);
		String storagePath = config.getString(HighAvailabilityOptions.HA_STORAGE_PATH) + "/" + clusterId;
		Random rand = new Random();

		BlobServer[] server = new BlobServer[2];
		InetSocketAddress[] serverAddress = new InetSocketAddress[2];
		BlobClient client = null;

		try {
			for (int i = 0; i < server.length; i++) {
				server[i] = new BlobServer(config);
				serverAddress[i] = new InetSocketAddress("localhost", server[i].getPort());
			}

			client = new BlobClient(serverAddress[0], config);

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

			// check that the storage directory exists
			final Path blobServerPath = new Path(storagePath, "blob");
			FileSystem fs = blobServerPath.getFileSystem();
			assertTrue("Unknown storage dir: " + blobServerPath, fs.exists(blobServerPath));

			// Close the client and connect to the other server
			client.close();
			client = new BlobClient(serverAddress[1], config);

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

			// Remove again
			client.delete(keys[0]);
			client.delete(keys[1]);
			client.delete(jobId[0], testKey[0]);
			client.delete(jobId[1], testKey[1]);

			// Verify everything is clean
			assertTrue("HA storage directory does not exist", fs.exists(new Path(storagePath)));
			if (fs.exists(blobServerPath)) {
				final org.apache.flink.core.fs.FileStatus[] recoveryFiles =
					fs.listStatus(blobServerPath);
				ArrayList<String> filenames = new ArrayList<String>(recoveryFiles.length);
				for (org.apache.flink.core.fs.FileStatus file: recoveryFiles) {
					filenames.add(file.toString());
				}
				fail("Unclean state backend: " + filenames);
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
	}
}
