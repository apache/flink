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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobCachePutTest.verifyDeletedEventually;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerCleanupTest.checkFileCountForJob;
import static org.apache.flink.runtime.blob.BlobServerCleanupTest.checkFilesExist;
import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * A few tests for the cleanup of {@link PermanentBlobCache} and {@link TransientBlobCache}.
 */
public class BlobCacheCleanupTest extends TestLogger {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests that {@link PermanentBlobCache} cleans up after calling {@link PermanentBlobCache#releaseJob(JobID)}.
	 */
	@Test
	public void testPermanentBlobCleanup() throws IOException, InterruptedException {

		JobID jobId = new JobID();
		List<PermanentBlobKey> keys = new ArrayList<>();
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
			cache = new PermanentBlobCache(config, new VoidBlobStore(), serverAddress);

			// upload blobs
			keys.add(server.putPermanent(jobId, buf));
			buf[0] += 1;
			keys.add(server.putPermanent(jobId, buf));

			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			// register once
			cache.registerJob(jobId);

			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			for (PermanentBlobKey key : keys) {
				cache.getFile(jobId, key);
			}

			// register again (let's say, from another thread or so)
			cache.registerJob(jobId);
			for (PermanentBlobKey key : keys) {
				cache.getFile(jobId, key);
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
			verifyJobCleanup(cache, jobId, keys);
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
	 * Tests that {@link PermanentBlobCache} sets the expected reference counts and cleanup timeouts
	 * when registering, releasing, and re-registering jobs.
	 */
	@Test
	public void testPermanentJobReferences() throws IOException, InterruptedException {

		JobID jobId = new JobID();

		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setLong(BlobServerOptions.CLEANUP_INTERVAL, 3_600_000L); // 1 hour should effectively prevent races

		// NOTE: use fake address - we will not connect to it here
		InetSocketAddress serverAddress = new InetSocketAddress("localhost", 12345);

		try (PermanentBlobCache cache = new PermanentBlobCache(config, new VoidBlobStore(), serverAddress)) {

			// register once
			cache.registerJob(jobId);
			assertEquals(1, cache.getJobRefCounters().get(jobId).references);
			assertEquals(-1, cache.getJobRefCounters().get(jobId).keepUntil);

			// register a second time
			cache.registerJob(jobId);
			assertEquals(2, cache.getJobRefCounters().get(jobId).references);
			assertEquals(-1, cache.getJobRefCounters().get(jobId).keepUntil);

			// release once
			cache.releaseJob(jobId);
			assertEquals(1, cache.getJobRefCounters().get(jobId).references);
			assertEquals(-1, cache.getJobRefCounters().get(jobId).keepUntil);

			// release a second time
			long cleanupLowerBound =
				System.currentTimeMillis() + config.getLong(BlobServerOptions.CLEANUP_INTERVAL);
			cache.releaseJob(jobId);
			assertEquals(0, cache.getJobRefCounters().get(jobId).references);
			assertThat(cache.getJobRefCounters().get(jobId).keepUntil,
				greaterThanOrEqualTo(cleanupLowerBound));

			// register again
			cache.registerJob(jobId);
			assertEquals(1, cache.getJobRefCounters().get(jobId).references);
			assertEquals(-1, cache.getJobRefCounters().get(jobId).keepUntil);

			// finally release the job
			cleanupLowerBound =
				System.currentTimeMillis() + config.getLong(BlobServerOptions.CLEANUP_INTERVAL);
			cache.releaseJob(jobId);
			assertEquals(0, cache.getJobRefCounters().get(jobId).references);
			assertThat(cache.getJobRefCounters().get(jobId).keepUntil,
				greaterThanOrEqualTo(cleanupLowerBound));
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
		List<PermanentBlobKey> keys = new ArrayList<>();
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
			cache = new PermanentBlobCache(config, new VoidBlobStore(), serverAddress);

			// upload blobs
			keys.add(server.putPermanent(jobId, buf));
			buf[0] += 1;
			keys.add(server.putPermanent(jobId, buf));

			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			// register once
			cache.registerJob(jobId);

			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			for (PermanentBlobKey key : keys) {
				cache.getFile(jobId, key);
			}

			// register again (let's say, from another thread or so)
			cache.registerJob(jobId);
			for (PermanentBlobKey key : keys) {
				cache.getFile(jobId, key);
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
			verifyJobCleanup(cache, jobId, keys);
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

		final List<CompletableFuture<Void>> getOperations = new ArrayList<>(numberConcurrentGetOperations);

		byte[] data = new byte[2000000];
		rnd.nextBytes(data);
		byte[] data2 = Arrays.copyOfRange(data, 10, 54);

		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setLong(BlobServerOptions.CLEANUP_INTERVAL, cleanupInterval);

		long cleanupLowerBound;

		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			final BlobCacheService cache = new BlobCacheService(
				config, new VoidBlobStore(), new InetSocketAddress("localhost", server.getPort())
			)) {
			ConcurrentMap<Tuple2<JobID, TransientBlobKey>, Long> transientBlobExpiryTimes =
				cache.getTransientBlobService().getBlobExpiryTimes();

			server.start();

			final TransientBlobKey key1 =
				(TransientBlobKey) put(server, jobId, data, TRANSIENT_BLOB);
			final TransientBlobKey key2 =
				(TransientBlobKey) put(server, jobId, data2, TRANSIENT_BLOB);

			// access key1, verify expiry times
			cleanupLowerBound = System.currentTimeMillis() + cleanupInterval;
			verifyContents(cache, jobId, key1, data);
			final Long key1ExpiryFirstAccess = transientBlobExpiryTimes.get(Tuple2.of(jobId, key1));
			assertThat(key1ExpiryFirstAccess, greaterThanOrEqualTo(cleanupLowerBound));
			assertNull(transientBlobExpiryTimes.get(Tuple2.of(jobId, key2)));

			// access key2, verify expiry times (delay at least 1ms to also verify key1 expiry is unchanged)
			Thread.sleep(1);
			cleanupLowerBound = System.currentTimeMillis() + cleanupInterval;
			verifyContents(cache, jobId, key2, data2);
			assertEquals(key1ExpiryFirstAccess, transientBlobExpiryTimes.get(Tuple2.of(jobId, key1)));
			assertThat(transientBlobExpiryTimes.get(Tuple2.of(jobId, key2)),
				greaterThanOrEqualTo(cleanupLowerBound));

			// files are cached now for the given TTL - remove from server so that they are not re-downloaded
			if (jobId != null) {
				server.cleanupJob(jobId);
			} else {
				server.deleteFromCache(key1);
				server.deleteFromCache(key2);
			}
			checkFileCountForJob(0, jobId, server);

			// cleanup task is run every cleanupInterval seconds
			// => unaccessed file should remain at most 2*cleanupInterval seconds
			// (use 3*cleanupInterval to check that we can still access it)
			final long finishTime = System.currentTimeMillis() + 3 * cleanupInterval;

			final ExecutorService executor = Executors.newFixedThreadPool(numberConcurrentGetOperations);
			for (int i = 0; i < numberConcurrentGetOperations; i++) {
				CompletableFuture<Void> getOperation = CompletableFuture
					.supplyAsync(
						() -> {
							try {
								// constantly access key1 so this should not get deleted
								while (System.currentTimeMillis() < finishTime) {
									get(cache, jobId, key1);
								}

								return null;
							} catch (IOException e) {
								throw new CompletionException(new FlinkException(
									"Could not retrieve blob.", e));
							}
					}, executor);

				getOperations.add(getOperation);
			}

			FutureUtils.ConjunctFuture<Collection<Void>> filesFuture = FutureUtils.combineAll(getOperations);
			filesFuture.get();

			verifyDeletedEventually(server, jobId, key1, key2);
		}
	}

	/**
	 * Checks that BLOBs for the given <tt>jobId</tt> are cleaned up eventually (after calling
	 * {@link PermanentBlobCache#releaseJob(JobID)}, which is not done by this method!) (waits at
	 * most 30s).
	 *
	 * @param cache
	 * 		BLOB server
	 * @param jobId
	 * 		job ID or <tt>null</tt> if job-unrelated
	 * @param keys
	 * 		keys identifying BLOBs which were previously registered for the <tt>jobId</tt>
	 */
	static void verifyJobCleanup(PermanentBlobCache cache, JobID jobId, List<? extends BlobKey> keys)
		throws InterruptedException, IOException {
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
	}
}
