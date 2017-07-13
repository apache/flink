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
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobServerGetTest.verifyDeleted;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * Tests how DELETE requests behave.
 */
public class BlobCacheDeleteTest extends TestLogger {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testDeleteTransient1() throws IOException {
		testDeleteTransient(null, new JobID());
	}

	@Test
	public void testDeleteTransient2() throws IOException {
		testDeleteTransient(new JobID(), null);
	}

	@Test
	public void testDeleteTransient3() throws IOException {
		testDeleteTransient(null, null);
	}

	@Test
	public void testDeleteTransient4() throws IOException {
		testDeleteTransient(new JobID(), new JobID());
	}

	/**
	 * Uploads a (different) byte array for each of the given jobs and verifies that deleting one of
	 * them (via the {@link BlobCache}) does not influence the other.
	 *
	 * @param jobId1
	 * 		first job id
	 * @param jobId2
	 * 		second job id
	 */
	private void testDeleteTransient(@Nullable JobID jobId1, @Nullable JobID jobId2)
			throws IOException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			byte[] data2 = Arrays.copyOf(data, data.length);
			data2[0] ^= 1;

			// put first BLOB
			BlobKey key1 = put(server, jobId1, data, false);
			assertNotNull(key1);

			// put two more BLOBs (same key, other key) for another job ID
			BlobKey key2a = put(server, jobId2, data, false);
			assertNotNull(key2a);
			assertEquals(key1, key2a);
			BlobKey key2b = put(server, jobId2, data2, false);
			assertNotNull(key2b);

			// issue a DELETE request
			assertTrue(delete(cache, jobId1, key1));

			verifyDeleted(cache, jobId1, key1, false);
			verifyDeleted(server, jobId1, key1, false);
			// deleting a one BLOB should not affect another BLOB, even with the same key if job IDs are different
			if ((jobId1 == null && jobId2 != null) || (jobId1 != null && !jobId1.equals(jobId2))) {
				verifyContents(server, jobId2, key2a, data, false);
			}
			verifyContents(server, jobId2, key2b, data2, false);

			// delete first file of second job
			assertTrue(delete(cache, jobId2, key2a));
			verifyDeleted(cache, jobId2, key2a, false);
			verifyDeleted(server, jobId2, key2a, false);
			verifyContents(server, jobId2, key2b, data2, false);

			// delete second file of second job
			assertTrue(delete(cache, jobId2, key2a));
			verifyDeleted(cache, jobId2, key2a, false);
			verifyDeleted(server, jobId2, key2a, false);
			verifyContents(server, jobId2, key2b, data2, false);
		}
	}

	@Test
	public void testDeleteTransientAlreadyDeletedNoJob() throws IOException {
		testDeleteTransientAlreadyDeleted(null);
	}

	@Test
	public void testDeleteTransientAlreadyDeletedForJob() throws IOException {
		testDeleteTransientAlreadyDeleted(new JobID());
	}

	/**
	 * Uploads a byte array for the given job and verifies that deleting it (via the {@link
	 * BlobCache}) does not fail independent of whether the file exists.
	 *
	 * @param jobId
	 * 		job id
	 */
	private void testDeleteTransientAlreadyDeleted(@Nullable final JobID jobId) throws IOException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put BLOB
			BlobKey key = put(server, jobId, data, false);
			assertNotNull(key);

			File blobFile = server.getStorageLocation(jobId, key);
			assertTrue(blobFile.delete());

			// DELETE operation should not fail if file is already deleted
			assertTrue(delete(cache, jobId, key));
			verifyDeleted(cache, jobId, key, false);

			// one more deleta call that should not fail
			assertTrue(delete(cache, jobId, key));
			verifyDeleted(cache, jobId, key, false);
		}
	}

	@Test
	public void testDeleteTransientLocalFailsNoJob() throws IOException {
		testDeleteTransientLocalFails(null);
	}

	@Test
	public void testDeleteTransientLocalFailsForJob() throws IOException {
		testDeleteTransientLocalFails(new JobID());
	}

	/**
	 * Uploads a byte array for the given job and verifies that a delete operation (via the {@link
	 * BlobCache}) does not fail even if the file is not deletable locally, e.g. via restricting
	 * the permissions.
	 *
	 * @param jobId
	 * 		job id
	 */
	private void testDeleteTransientLocalFails(@Nullable final JobID jobId) throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		File blobFile = null;
		File directory = null;
		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			try {
				byte[] data = new byte[2000000];
				rnd.nextBytes(data);

				// put BLOB
				BlobKey key = put(server, jobId, data, false);
				assertNotNull(key);

				// access from cache once to have it available there
				verifyContents(cache, jobId, key, data, false);

				blobFile = cache.getTransientBlobStore().getStorageLocation(jobId, key);
				directory = blobFile.getParentFile();

				assertTrue(blobFile.setWritable(false, false));
				assertTrue(directory.setWritable(false, false));

				// issue a DELETE request
				assertFalse(delete(cache, jobId, key));
				verifyDeleted(server, jobId, key, false);

				// the file should still be there on both cache and server
				verifyContents(cache, jobId, key, data, false);
			} finally {
				if (blobFile != null && directory != null) {
					//noinspection ResultOfMethodCallIgnored
					blobFile.setWritable(true, false);
					//noinspection ResultOfMethodCallIgnored
					directory.setWritable(true, false);
				}
			}
		}
	}

	@Test
	public void testDeleteTransientRemoteFailsNoJob() throws IOException {
		testDeleteTransientRemoteFails(null);
	}

	@Test
	public void testDeleteTransientRemoteFailsForJob() throws IOException {
		testDeleteTransientRemoteFails(new JobID());
	}

	/**
	 * Uploads a byte array for the given job and verifies that a delete operation (via the {@link
	 * BlobCache}) does not fail even if the file is not deletable on the {@link BlobServer}, e.g.
	 * via restricting the permissions.
	 *
	 * @param jobId
	 * 		job id
	 */
	private void testDeleteTransientRemoteFails(@Nullable final JobID jobId) throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		File blobFile = null;
		File directory = null;

		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			try {
				byte[] data = new byte[2000000];
				rnd.nextBytes(data);

				// put BLOB
				BlobKey key = put(server, jobId, data, false);
				assertNotNull(key);

				// access from cache once to have it available there
				verifyContents(cache, jobId, key, data, false);

				blobFile = server.getStorageLocation(jobId, key);
				directory = blobFile.getParentFile();

				assertTrue(blobFile.setWritable(false, false));
				assertTrue(directory.setWritable(false, false));

				// issue a DELETE request
				assertFalse(delete(cache, jobId, key));
				File blobFileAtCache = cache.getTransientBlobStore().getStorageLocation(jobId, key);
				assertFalse(blobFileAtCache.exists());

				// the file should still be there on the server
				verifyContents(server, jobId, key, data, false);
				// ... and may be retrieved by the cache
				verifyContents(cache, jobId, key, data, false);
			} finally {
				if (blobFile != null && directory != null) {
					//noinspection ResultOfMethodCallIgnored
					blobFile.setWritable(true, false);
					//noinspection ResultOfMethodCallIgnored
					directory.setWritable(true, false);
				}
			}
		}
	}

	@Test
	public void testConcurrentDeleteOperationsNoJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentDeleteOperations(null);
	}

	@Test
	public void testConcurrentDeleteOperationsForJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentDeleteOperations(new JobID());
	}

	/**
	 * [FLINK-6020] Tests that concurrent delete operations don't interfere with each other.
	 * <p>
	 * Note: This test checks that there cannot be two threads which have checked whether a given
	 * blob file exist and then one of them fails deleting it. Without the introduced lock, this
	 * situation should rarely happen and make this test fail. Thus, if this test should become
	 * "unstable", then the delete atomicity is most likely broken.
	 *
	 * @param jobId
	 * 		job ID to use (or <tt>null</tt> if job-unrelated)
	 */
	private void testConcurrentDeleteOperations(@Nullable final JobID jobId)
			throws IOException, InterruptedException, ExecutionException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		final int concurrentDeleteOperations = 3;
		final ExecutorService executor = Executors.newFixedThreadPool(concurrentDeleteOperations);

		final List<Future<Void>> deleteFutures = new ArrayList<>(concurrentDeleteOperations);

		final byte[] data = {1, 2, 3};

		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			final BlobKey blobKey = put(server, jobId, data, false);

			assertTrue(server.getStorageLocation(jobId, blobKey).exists());

			for (int i = 0; i < concurrentDeleteOperations; i++) {
				Future<Void> deleteFuture = FlinkCompletableFuture.supplyAsync(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						boolean deleted = delete(cache, jobId, blobKey);
						assertTrue(deleted);
						assertFalse(cache.getTransientBlobStore().getStorageLocation(jobId, blobKey).exists());
						assertFalse(server.getStorageLocation(jobId, blobKey).exists());
						return null;
					}
				}, executor);

				deleteFutures.add(deleteFuture);
			}

			Future<Void> waitFuture = FutureUtils.waitForAll(deleteFutures);

			// make sure all delete operation have completed successfully
			// in case of no lock, one of the delete operations should eventually fail
			waitFuture.get();

			assertFalse(server.getStorageLocation(jobId, blobKey).exists());

		} finally {
			executor.shutdownNow();
		}
	}

	/**
	 * Deletes a transient BLOB from the given BLOB service.
	 *
	 * @param service
	 * 		blob service
	 * @param jobId
	 * 		job ID (or <tt>null</tt> if job-unrelated)
	 * @param key
	 * 		blob key
	 *
	 * @return delete success
	 */
	static boolean delete(BlobService service, @Nullable JobID jobId, BlobKey key) {
		if (jobId == null) {
			return service.getTransientBlobStore().delete(key);
		} else {
			return service.getTransientBlobStore().delete(jobId, key);
		}
	}
}
