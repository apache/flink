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
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.apache.flink.runtime.blob.BlobServerGetTest.verifyDeleted;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests how failing GET requests behave in the presence of failures when used with a {@link
 * BlobCache}.
 * <p>
 * Successful GET requests are tested in conjunction wit the PUT requests.
 */
public class BlobCacheGetTest extends TestLogger {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testGetFailsDuringLookup1() throws IOException {
		testGetFailsDuringLookup(null, new JobID(), false);
	}

	@Test
	public void testGetFailsDuringLookup2() throws IOException {
		testGetFailsDuringLookup(new JobID(), new JobID(), false);
	}

	@Test
	public void testGetFailsDuringLookup3() throws IOException {
		testGetFailsDuringLookup(new JobID(), null, false);
	}

	@Test
	public void testGetFailsDuringLookupHa() throws IOException {
		testGetFailsDuringLookup(new JobID(), new JobID(), true);
	}

	/**
	 * Checks the correct result if a GET operation fails during the lookup of the file.
	 *
	 * @param jobId1 first job ID or <tt>null</tt> if job-unrelated
	 * @param jobId2 second job ID different to <tt>jobId1</tt>
	 */
	private void testGetFailsDuringLookup(final JobID jobId1, final JobID jobId2, boolean highAvailabibility)
		throws IOException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = put(server, jobId1, data, highAvailabibility);
			assertNotNull(key);

			// delete file to make sure that GET requests fail
			File blobFile = server.getStorageLocation(jobId1, key);
			assertTrue(blobFile.delete());

			// issue a GET request that fails
			verifyDeleted(cache, jobId1, key, highAvailabibility);

			// add the same data under a second jobId
			BlobKey key2 = put(server, jobId2, data, highAvailabibility);
			assertNotNull(key);
			assertEquals(key, key2);

			// request for jobId2 should succeed
			get(cache, jobId2, key, highAvailabibility);
			// request for jobId1 should still fail
			verifyDeleted(cache, jobId1, key, highAvailabibility);

			// delete on cache, try to retrieve again
			if (highAvailabibility) {
				blobFile = cache.getPermanentBlobStore().getStorageLocation(jobId2, key);
			} else {
				blobFile = cache.getTransientBlobStore().getStorageLocation(jobId2, key);
			}
			assertTrue(blobFile.delete());
			get(cache, jobId2, key, highAvailabibility);

			// delete on cache and server, verify that it is not accessible anymore
			if (highAvailabibility) {
				blobFile = cache.getPermanentBlobStore().getStorageLocation(jobId2, key);
			} else {
				blobFile = cache.getTransientBlobStore().getStorageLocation(jobId2, key);
			}
			assertTrue(blobFile.delete());
			blobFile = server.getStorageLocation(jobId2, key);
			assertTrue(blobFile.delete());
			verifyDeleted(cache, jobId2, key, highAvailabibility);
		}
	}

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent get operations don't concurrently access the BlobStore to download a blob.
	 */
	@Test
	public void testConcurrentGetOperationsNoJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentGetOperations(null, false, false);
	}

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent get operations don't concurrently access the BlobStore to download a blob.
	 */
	@Test
	public void testConcurrentGetOperationsForJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentGetOperations(new JobID(), false, false);
	}

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent get operations don't concurrently access the BlobStore to download a blob.
	 */
	@Test
	public void testConcurrentGetOperationsForJobHa() throws IOException, ExecutionException, InterruptedException {
		testConcurrentGetOperations(new JobID(), true, false);
	}

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent get operations don't concurrently access the BlobStore to download a blob.
	 */
	@Test
	public void testConcurrentGetOperationsForJobHa2() throws IOException, ExecutionException, InterruptedException {
		testConcurrentGetOperations(new JobID(), true, true);
	}

	private void testConcurrentGetOperations(final JobID jobId, final boolean highAvailabibility,
			final boolean cacheAccessesHAStore)
			throws IOException, InterruptedException, ExecutionException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		final BlobStore blobStoreServer = mock(BlobStore.class);
		final BlobStore blobStoreCache = mock(BlobStore.class);

		final int numberConcurrentGetOperations = 3;
		final List<Future<File>> getOperations = new ArrayList<>(numberConcurrentGetOperations);

		final byte[] data = {1, 2, 3, 4, 99, 42};

		MessageDigest md = BlobUtils.createMessageDigest();

		// create the correct blob key by hashing our input data
		final BlobKey blobKey = new BlobKey(md.digest(data));

		final ExecutorService executor = Executors.newFixedThreadPool(numberConcurrentGetOperations);

		try (
			final BlobServer server = new BlobServer(config, blobStoreServer);
			final BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, cacheAccessesHAStore ? blobStoreServer : blobStoreCache)) {

			// upload data first
			assertEquals(blobKey, put(server, jobId, data, highAvailabibility));

			// now try accessing it concurrently (only HA mode will be able to retrieve it from HA store!)
			for (int i = 0; i < numberConcurrentGetOperations; i++) {
				Future<File> getOperation = FlinkCompletableFuture
					.supplyAsync(new Callable<File>() {
					@Override
					public File call() throws Exception {
						File file = get(cache, jobId, blobKey, highAvailabibility);
						// check that we have read the right data
						try (InputStream is = new FileInputStream(file)) {
							BlobClientTest.validateGet(is, data);
						}
						return file;
					}
				}, executor);

				getOperations.add(getOperation);
			}

			Future<Collection<File>> filesFuture = FutureUtils.combineAll(getOperations);
			filesFuture.get();

			// TODO: verify that the file is written only once (concurrent access in PermanentBlobCache)
			if (cacheAccessesHAStore) {
				// ...
			} else {
				// verify that we did not download the requested blob from the HA store
				verify(blobStoreServer, times(0)).get(eq(jobId), eq(blobKey), any(File.class));
				// ...
			}
		} finally {
			executor.shutdownNow();
		}
	}
}
