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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.junit.Assert.assertEquals;

/**
 * A few tests for the cleanup of transient BLOBs at the {@link BlobServer}.
 */
public class BlobServerCleanupTest {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

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

		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			server.start();

			// after put(), files are cached for the given TTL
			keys.add(put(server, jobId, data, false));
			keys.add(put(server, jobId, data2, false));

			// check that HA contents are not cleaned up
			final JobID jobIdHA = (jobId == null) ? new JobID() : jobId;
			final BlobKey keyHA = put(server, jobIdHA, data, true);

			verifyContents(server, jobId, keys.get(0), data, false);
			verifyContents(server, jobId, keys.get(1), data2, false);

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
								get(server, jobId, keys.get(0), false);
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
			verifyContents(server, jobId, keys.get(0), data, false);

			// because we cannot guarantee that there are not thread races in the build system, we
			// loop for a certain while until both BLOBs disappear
			{
				long deadline = System.currentTimeMillis() + 10_000L;
				do {
					Thread.sleep(100);
				} while (checkFilesExist(jobId, keys, server, false) != 0 &&
					System.currentTimeMillis() < deadline);

				verifyDeleted(server, jobId, keys.get(0), false);
				verifyDeleted(server, jobId, keys.get(1), false);
			}

			// HA content should be unaffected
			verifyContents(server, jobIdHA, keyHA, data, true);
		}
	}

	/**
	 * Checks how many of the files given by blob keys are accessible.
	 *
	 * @param jobId
	 * 		ID of a job or <tt>null</tt> if job-unrelated and not using a {@link PermanentBlobCache} as
	 * 		the <tt>blobService</tt>
	 * @param keys
	 * 		blob keys to check
	 * @param blobService
	 * 		BLOB store to use
	 * @param doThrow
	 * 		whether exceptions should be ignored (<tt>false</tt>), or thrown (<tt>true</tt>)
	 *
	 * @return number of files stored at the <tt>blobService</tt>
	 */
	public static <T> int checkFilesExist(
			@Nullable JobID jobId, Collection<BlobKey> keys, T blobService, boolean doThrow)
			throws IOException {

		int numFiles = 0;

		for (BlobKey key : keys) {
			final File blobFile;
			if (blobService instanceof BlobServer) {
				BlobServer server = (BlobServer) blobService;
				blobFile = server.getStorageLocation(jobId, key);
			} else if (blobService instanceof PermanentBlobCache) {
				PermanentBlobCache cache = (PermanentBlobCache) blobService;
				blobFile = cache.getStorageLocation(jobId, key);
			} else if (blobService instanceof TransientBlobCache) {
				TransientBlobCache cache = (TransientBlobCache) blobService;
				blobFile = cache.getStorageLocation(jobId, key);
			} else {
				throw new UnsupportedOperationException(
					"unsupported BLOB service class: " + blobService.getClass().getCanonicalName());
			}
			if (blobFile.exists()) {
				++numFiles;
			} else if (doThrow) {
				throw new IOException("File " + blobFile + " does not exist.");
			}
		}

		return numFiles;
	}

	/**
	 * Checks how many of the files given by blob keys are accessible.
	 *
	 * @param expectedCount
	 * 		number of expected files in the blob service for the given job
	 * @param jobId
	 * 		ID of a job
	 * @param blobService
	 * 		BLOB store to use
	 */
	public static void checkFileCountForJob(
			int expectedCount, JobID jobId, PermanentBlobService blobService)
			throws IOException {

		final File jobDir;
		if (blobService instanceof BlobServer) {
			BlobServer server = (BlobServer) blobService;
			jobDir = server.getStorageLocation(jobId, new BlobKey()).getParentFile();
		} else {
			PermanentBlobCache cache = (PermanentBlobCache) blobService;
			jobDir = cache.getStorageLocation(jobId, new BlobKey()).getParentFile();
		}
		File[] blobsForJob = jobDir.listFiles();
		if (blobsForJob == null) {
			if (expectedCount != 0) {
				throw new IOException("File " + jobDir + " does not exist.");
			}
		} else {
			assertEquals("Too many/few files in job dir: " +
					Arrays.asList(blobsForJob).toString(), expectedCount,
				blobsForJob.length);
		}
	}
}
