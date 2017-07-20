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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobServerCleanupTest.checkFileCountForJob;
import static org.apache.flink.runtime.blob.BlobServerCleanupTest.checkFilesExist;
import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.apache.flink.runtime.blob.BlobServerGetTest.verifyDeleted;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.junit.Assert.assertEquals;

/**
 * A few tests for the cleanup of {@link PermanentBlobCache} and {@link TransientBlobCache}.
 */
public class BlobCacheCleanupTest {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests that {@link PermanentBlobCache} cleans up after calling {@link PermanentBlobCache#releaseJob(JobID)}.
	 */
	@Test
	public void testPermanentBlobCleanup() throws IOException, InterruptedException {

		JobID jobId = new JobID();
		List<BlobKey> keys = new ArrayList<>();
		BlobServer server = null;
		PermanentBlobCache cache = null;

		final byte[] buf = new byte[128];

		try {
			Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());
			config.setLong(BlobServerOptions.CLEANUP_INTERVAL, 1L);

			server = new BlobServer(config, new VoidBlobStore());
			server.start();
			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			cache = new PermanentBlobCache(serverAddress, config, new VoidBlobStore());

			keys.add(server.putHA(jobId, buf));
			buf[0] += 1;
			keys.add(server.putHA(jobId, buf));

			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			// register once
			cache.registerJob(jobId);

			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			for (BlobKey key : keys) {
				cache.getHAFile(jobId, key);
			}

			// register again (let's say, from another thread or so)
			cache.registerJob(jobId);
			for (BlobKey key : keys) {
				cache.getHAFile(jobId, key);
			}

			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			// after releasing once, nothing should change
			cache.releaseJob(jobId);

			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			// after releasing the second time, the job is up for deferred cleanup
			cache.releaseJob(jobId);

			// because we cannot guarantee that there are not thread races in the build system, we
			// loop for a certain while until the references disappear
			{
				long deadline = System.currentTimeMillis() + 30_000L;
				do {
					Thread.sleep(100);
				}
				while (checkFilesExist(jobId, keys, cache, false) != 0 &&
					System.currentTimeMillis() < deadline);
			}

			// the blob cache should no longer contain the files
			// this fails if we exited via a timeout
			checkFileCountForJob(0, jobId, cache);
			// server should be unaffected
			checkFileCountForJob(2, jobId, server);
		}
		finally {
			if (cache != null) {
				cache.close();
			}

			if (server != null) {
				server.close();
			}
			// now everything should be cleaned up
			checkFileCountForJob(0, jobId, server);
		}
	}

	/**
	 * Tests the deferred cleanup of {@link PermanentBlobCache}, i.e. after calling {@link
	 * PermanentBlobCache#releaseJob(JobID)} the file should be preserved a bit longer and then
	 * cleaned up.
	 */
	@Test
	@Ignore("manual test due to stalling: ensures a BLOB is retained first and only deleted after the (long) timeout ")
	public void testPermanentBlobDeferredCleanup() throws IOException, InterruptedException {
		// file should be deleted between 5 and 10s after last job release
		long cleanupInterval = 5L;

		JobID jobId = new JobID();
		List<BlobKey> keys = new ArrayList<>();
		BlobServer server = null;
		PermanentBlobCache cache = null;

		final byte[] buf = new byte[128];

		try {
			Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());
			config.setLong(BlobServerOptions.CLEANUP_INTERVAL, cleanupInterval);

			server = new BlobServer(config, new VoidBlobStore());
			server.start();
			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			cache = new PermanentBlobCache(serverAddress, config, new VoidBlobStore());

			keys.add(server.putHA(jobId, buf));
			buf[0] += 1;
			keys.add(server.putHA(jobId, buf));

			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			// register once
			cache.registerJob(jobId);

			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			for (BlobKey key : keys) {
				cache.getHAFile(jobId, key);
			}

			// register again (let's say, from another thread or so)
			cache.registerJob(jobId);
			for (BlobKey key : keys) {
				cache.getHAFile(jobId, key);
			}

			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			// after releasing once, nothing should change
			cache.releaseJob(jobId);

			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			// after releasing the second time, the job is up for deferred cleanup
			cache.releaseJob(jobId);

			// files should still be accessible for now
			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, cache);

			Thread.sleep(cleanupInterval / 5);
			// still accessible...
			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, cache);

			Thread.sleep((cleanupInterval * 4) / 5);

			// files are up for cleanup now...wait for it:
			// because we cannot guarantee that there are not thread races in the build system, we
			// loop for a certain while until the references disappear
			{
				long deadline = System.currentTimeMillis() + 30_000L;
				do {
					Thread.sleep(100);
				}
				while (checkFilesExist(jobId, keys, cache, false) != 0 &&
					System.currentTimeMillis() < deadline);
			}

			// the blob cache should no longer contain the files
			// this fails if we exited via a timeout
			checkFileCountForJob(0, jobId, cache);
			// server should be unaffected
			checkFileCountForJob(2, jobId, server);
		}
		finally {
			if (cache != null) {
				cache.close();
			}

			if (server != null) {
				server.close();
			}
			// now everything should be cleaned up
			checkFileCountForJob(0, jobId, server);
		}
	}

	@Test
	public void testTransientBlobNoJobCleanup()
			throws IOException, InterruptedException, ExecutionException {
		testTransientBlobCleanup(null);
	}

	@Test
	public void testTransientBlobForJobCleanup()
			throws IOException, InterruptedException, ExecutionException {
		testTransientBlobCleanup(new JobID());
	}

	/**
	 * Tests that {@link TransientBlobCache} cleans up after a default TTL and keeps files which are
	 * constantly accessed.
	 */
	private void testTransientBlobCleanup(@Nullable final JobID jobId)
			throws IOException, InterruptedException, ExecutionException {

		// 1s should be a safe-enough buffer to still check for existence after a BLOB's last access
		long cleanupInterval = 1L; // in seconds
		final int numberConcurrentGetOperations = 3;

		final List<BlobKey> keys = new ArrayList<>();
		final List<Future<Void>> getOperations = new ArrayList<>(numberConcurrentGetOperations);

		byte[] data = new byte[2000000];
		rnd.nextBytes(data);
		byte[] data2 = Arrays.copyOfRange(data, 10, 54);

		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setLong(BlobServerOptions.CLEANUP_INTERVAL, cleanupInterval);

		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			final BlobCache cache = new BlobCache(
				new InetSocketAddress("localhost", server.getPort()), config,
				new VoidBlobStore())) {

			server.start();

			keys.add(put(server, jobId, data, false));
			keys.add(put(server, jobId, data2, false));

			verifyContents(cache, jobId, keys.get(0), data, false);
			verifyContents(cache, jobId, keys.get(1), data2, false);

			// files are cached now for the given TTL - remove from server so that they are not re-downloaded
			if (jobId != null) {
				server.cleanupJob(jobId);
			} else {
				server.delete(keys.get(0));
				server.delete(keys.get(1));
			}
			checkFileCountForJob(0, jobId, server);

			// cleanup task is run every cleanupInterval seconds
			// => unaccessed file should remain at most 2*cleanupInterval seconds
			final long finishTime = System.currentTimeMillis() + 2 * cleanupInterval;

			final ExecutorService executor = Executors.newFixedThreadPool(numberConcurrentGetOperations);
			for (int i = 0; i < numberConcurrentGetOperations; i++) {
				Future<Void> getOperation = FlinkCompletableFuture
					.supplyAsync(new Callable<Void>() {
						@Override
						public Void call() throws Exception {
							// constantly access key1 so this should not get deleted
							while (System.currentTimeMillis() < finishTime) {
								get(cache, jobId, keys.get(0), false);
							}
							return null;
						}
					}, executor);

				getOperations.add(getOperation);
			}

			Future<Collection<Void>> filesFuture = FutureUtils.combineAll(getOperations);
			filesFuture.get();

			// file 1 should still be accessible until 1s after the last access which should give
			// us enough time to still check for its existence
			verifyContents(cache, jobId, keys.get(0), data, false);

			// because we cannot guarantee that there are not thread races in the build system, we
			// loop for a certain while until both BLOBs disappear
			{
				long deadline = System.currentTimeMillis() + 10_000L;
				do {
					Thread.sleep(100);
				} while (checkFilesExist(jobId, keys, cache.getTransientBlobStore(), false) != 0 &&
					System.currentTimeMillis() < deadline);

				verifyDeleted(cache, jobId, keys.get(0), false);
				verifyDeleted(cache, jobId, keys.get(1), false);
			}
		}
	}
}
