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
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;

import static org.junit.Assert.*;

/**
 * Unit tests for the blob cache retrying the connection to the server.
 */
public class BlobCacheRetriesTest {

	/**
	 * A test where the connection fails twice and then the get operation succeeds.
	 */
	@Test
	public void testBlobFetchRetries() {

		final byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};

		BlobServer server = null;
		BlobCache cache = null;
		try {
			final Configuration config = new Configuration();

			server = new TestingFailingBlobServer(config, 2);

			final InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());

			// upload some blob
			BlobClient blobClient = null;
			BlobKey key;
			try {
				blobClient = new BlobClient(serverAddress);

				key = blobClient.put(data);
			}
			finally {
				if (blobClient != null) {
					blobClient.close();
				}
			}

			cache = new BlobCache(serverAddress, config);

			// trigger a download - it should fail on the first time, but retry, and succeed at the second time
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
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (cache != null) {
				cache.shutdown();
			}
			if (server != null) {
				server.shutdown();
			}
		}
	}

	/**
	 * A test where the connection fails too often and eventually fails the GET request.
	 */
	@Test
	public void testBlobFetchWithTooManyFailures() {

		final byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

		BlobServer server = null;
		BlobCache cache = null;
		try {
			final Configuration config = new Configuration();

			server = new TestingFailingBlobServer(config, 10);

			final InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());

			// upload some blob
			BlobClient blobClient = null;
			BlobKey key;
			try {
				blobClient = new BlobClient(serverAddress);

				key = blobClient.put(data);
			}
			finally {
				if (blobClient != null) {
					blobClient.close();
				}
			}

			cache = new BlobCache(serverAddress, config);

			// trigger a download - it should fail eventually
			try {
				cache.getURL(key);
				fail("This should fail");
			}
			catch (IOException e) {
				// as we expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (cache != null) {
				cache.shutdown();
			}
			if (server != null) {
				server.shutdown();
			}
		}
	}
}
