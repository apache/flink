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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Unit tests for the blob cache retrying the connection to the server.
 */
public class BlobCacheRetriesTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * A test where the connection fails twice and then the get operation succeeds
	 * (job-unrelated blob).
	 */
	@Test
	public void testBlobFetchRetries() throws IOException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		testBlobFetchRetries(config, new VoidBlobStore(), null, false);
	}

	/**
	 * A test where the connection fails twice and then the get operation succeeds
	 * (job-related blob).
	 */
	@Test
	public void testBlobForJobFetchRetries() throws IOException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		testBlobFetchRetries(config, new VoidBlobStore(), new JobID(), false);
	}

	/**
	 * A test where the connection fails twice and then the get operation succeeds
	 * (with high availability set, job-related job).
	 */
	@Test
	public void testBlobFetchRetriesHa() throws IOException {
		testBlobFetchRetriesHa(new JobID());
	}

	private void testBlobFetchRetriesHa(final JobID jobId) throws IOException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.newFolder().getPath());

		BlobStoreService blobStoreService = null;

		try {
			blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

			testBlobFetchRetries(config, blobStoreService, jobId, true);
		} finally {
			if (blobStoreService != null) {
				blobStoreService.closeAndCleanupAllData();
			}
		}
	}

	/**
	 * A test where the BlobCache must use the BlobServer and the connection
	 * fails twice and then the get operation succeeds.
	 *
	 * @param config
	 * 		configuration to use (the BlobCache will get some additional settings
	 * 		set compared to this one)
	 */
	private static void testBlobFetchRetries(
			final Configuration config, final BlobStore blobStore, final JobID jobId,
			boolean highAvailabibility) throws IOException {
		final byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};

		BlobServer server = null;
		BlobCache cache = null;
		try {

			server = new TestingFailingBlobServer(config, blobStore, 2);

			final InetSocketAddress
				serverAddress = new InetSocketAddress("localhost", server.getPort());

			// upload some blob
			final BlobKey key;
			if (highAvailabibility) {
				key = server.putHA(jobId, data);
			} else if (jobId == null) {
				key = server.put(data);
			} else {
				key = server.put(jobId, data);
			}

			cache = new BlobCache(serverAddress, config, new VoidBlobStore());

			// trigger a download - it should fail the first two times, but retry, and succeed eventually
			final File file;
			if (highAvailabibility) {
				file = cache.getPermanentBlobStore().getHAFile(jobId, key);
			} else if (jobId == null) {
				file = cache.getTransientBlobStore().getFile(key);
			} else {
				file = cache.getTransientBlobStore().getFile(jobId, key);
			}
			try (InputStream is = new FileInputStream(file)) {
				byte[] received = new byte[data.length];
				assertEquals(data.length, is.read(received));
				assertArrayEquals(data, received);
			}
		} finally {
			if (cache != null) {
				cache.close();
			}
			if (server != null) {
				server.close();
			}
		}
	}

	/**
	 * A test where the connection fails too often and eventually fails the GET request
	 * (job-unrelated blob).
	 */
	@Test
	public void testBlobNoJobFetchWithTooManyFailures() throws IOException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		testBlobFetchWithTooManyFailures(config, new VoidBlobStore(), null, false);
	}

	/**
	 * A test where the connection fails too often and eventually fails the GET request (job-related
	 * blob).
	 */
	@Test
	public void testBlobForJobFetchWithTooManyFailures() throws IOException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		testBlobFetchWithTooManyFailures(config, new VoidBlobStore(), new JobID(), false);
	}

	/**
	 * A test where the connection fails too often and eventually fails the GET request
	 * (with high availability set, job-related blob).
	 */
	@Test
	public void testBlobForJobFetchWithTooManyFailuresHa() throws IOException {
		testBlobFetchWithTooManyFailuresHa(new JobID());
	}

	private void testBlobFetchWithTooManyFailuresHa(final JobID jobId) throws IOException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.getRoot().getPath());

		BlobStoreService blobStoreService = null;

		try {
			blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

			testBlobFetchWithTooManyFailures(config, blobStoreService, jobId, true);
		} finally {
			if (blobStoreService != null) {
				blobStoreService.closeAndCleanupAllData();
			}
		}
	}

	/**
	 * A test where the BlobCache must use the BlobServer and the connection
	 * fails too often which eventually fails the GET request.
	 *
	 * @param config
	 * 		configuration to use (the BlobCache will get some additional settings
	 * 		set compared to this one)
	 */
	private static void testBlobFetchWithTooManyFailures(
			final Configuration config, final BlobStore blobStore, final JobID jobId,
			boolean highAvailabibility) throws IOException {
		final byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

		BlobServer server = null;
		BlobCache cache = null;
		try {

			server = new TestingFailingBlobServer(config, blobStore, 0, 10);

			final InetSocketAddress
				serverAddress = new InetSocketAddress("localhost", server.getPort());

			// upload some blob
			final BlobKey key;
			if (highAvailabibility) {
				key = server.putHA(jobId, data);
			} else if (jobId == null) {
				key = server.put(data);
			} else {
				key = server.put(jobId, data);
			}

			cache = new BlobCache(serverAddress, config, new VoidBlobStore());

			// trigger a download - it should fail eventually
			try {
				if (highAvailabibility) {
					cache.getPermanentBlobStore().getHAFile(jobId, key);
				} else if (jobId == null) {
					cache.getTransientBlobStore().getFile(key);
				} else {
					cache.getTransientBlobStore().getFile(jobId, key);
				}
				fail("This should fail");
			}
			catch (IOException e) {
				// as we expected
			}
		}
		finally {
			if (cache != null) {
				cache.close();
			}
			if (server != null) {
				server.close();
			}
		}
	}
}
