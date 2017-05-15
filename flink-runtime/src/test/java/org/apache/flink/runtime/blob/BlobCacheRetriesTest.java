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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;

import static org.junit.Assert.*;

/**
 * Unit tests for the blob cache retrying the connection to the server.
 */
public class BlobCacheRetriesTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * A test where the connection fails twice and then the get operation succeeds.
	 */
	@Test
	public void testBlobFetchRetries() throws IOException {
		final Configuration config = new Configuration();

		testBlobFetchRetries(config, new VoidBlobStore());
	}

	/**
	 * A test where the connection fails twice and then the get operation succeeds
	 * (with high availability set).
	 */
	@Test
	public void testBlobFetchRetriesHa() throws IOException {
		final Configuration config = new Configuration();
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.getRoot().getPath());

		BlobStoreService blobStoreService = null;

		try {
			blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

			testBlobFetchRetries(config, blobStoreService);
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
	private void testBlobFetchRetries(final Configuration config, final BlobStore blobStore) throws IOException {
		final byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};

		BlobServer server = null;
		BlobCache cache = null;
		try {

			server = new TestingFailingBlobServer(config, blobStore, 2);

			final InetSocketAddress
				serverAddress = new InetSocketAddress("localhost", server.getPort());

			// upload some blob
			BlobClient blobClient = null;
			BlobKey key;
			try {
				blobClient = new BlobClient(serverAddress, config);

				key = blobClient.put(data);
			}
			finally {
				if (blobClient != null) {
					blobClient.close();
				}
			}

			cache = new BlobCache(serverAddress, config, new VoidBlobStore());

			// trigger a download - it should fail the first two times, but retry, and succeed eventually
			URL url = cache.getURL(key);
			InputStream is = url.openStream();
			try {
				byte[] received = new byte[data.length];
				assertEquals(data.length, is.read(received));
				assertArrayEquals(data, received);
			}
			finally {
				is.close();
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
	 * A test where the connection fails too often and eventually fails the GET request.
	 */
	@Test
	public void testBlobFetchWithTooManyFailures() throws IOException {
		final Configuration config = new Configuration();

		testBlobFetchWithTooManyFailures(config, new VoidBlobStore());
	}

	/**
	 * A test where the connection fails twice and then the get operation succeeds
	 * (with high availability set).
	 */
	@Test
	public void testBlobFetchWithTooManyFailuresHa() throws IOException {
		final Configuration config = new Configuration();
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH,
			temporaryFolder.getRoot().getPath());

		BlobStoreService blobStoreService = null;

		try {
			blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

			testBlobFetchWithTooManyFailures(config, blobStoreService);
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
	private void testBlobFetchWithTooManyFailures(final Configuration config, final BlobStore blobStore) throws IOException {
		final byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

		BlobServer server = null;
		BlobCache cache = null;
		try {

			server = new TestingFailingBlobServer(config, blobStore, 10);

			final InetSocketAddress
				serverAddress = new InetSocketAddress("localhost", server.getPort());

			// upload some blob
			BlobClient blobClient = null;
			BlobKey key;
			try {
				blobClient = new BlobClient(serverAddress, config);

				key = blobClient.put(data);
			}
			finally {
				if (blobClient != null) {
					blobClient.close();
				}
			}

			cache = new BlobCache(serverAddress, config, new VoidBlobStore());

			// trigger a download - it should fail eventually
			try {
				cache.getURL(key);
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
