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
import org.apache.flink.util.TestLogger;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * A few tests for the deferred ref-counting based cleanup inside the {@link PermanentBlobCache}.
 */
public class BlobCacheCleanupTest extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests that {@link PermanentBlobCache} cleans up after calling {@link PermanentBlobCache#releaseJob(JobID)}.
	 */
	@Test
	public void testJobCleanup() throws IOException, InterruptedException {

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

			// upload blobs
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
	 * Tests that {@link BlobCache} sets the expected reference counts and cleanup timeouts when
	 * registering, releasing, and re-registering jobs.
	 */
	@Test
	public void testJobReferences() throws IOException, InterruptedException {

		JobID jobId = new JobID();

		Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());
		config.setLong(BlobServerOptions.CLEANUP_INTERVAL, 3_600_000L); // 1 hour should effectively prevent races

		// NOTE: use fake address - we will not connect to it here
		InetSocketAddress serverAddress = new InetSocketAddress("localhost", 12345);

		try (PermanentBlobCache cache = new PermanentBlobCache(serverAddress, config, new VoidBlobStore())) {

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
	 * Tests that {@link PermanentBlobCache} cleans up after calling {@link PermanentBlobCache#releaseJob(JobID)}
	 * but only after preserving the file for a bit longer.
	 */
	@Test
	@Ignore("manual test due to stalling: ensures a BLOB is retained first and only deleted after the (long) timeout ")
	public void testJobDeferredCleanup() throws IOException, InterruptedException {
		// file should be deleted between 5 and 10s after last job release
		long cleanupInterval = 5L;

		JobID jobId = new JobID();
		List<BlobKey> keys = new ArrayList<BlobKey>();
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

			// upload blobs
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

	/**
	 * Checks how many of the files given by blob keys are accessible.
	 *
	 * @param jobId
	 * 		ID of a job
	 * @param keys
	 * 		blob keys to check
	 * @param blobService
	 * 		BLOB store to use
	 * @param doThrow
	 * 		whether exceptions should be ignored (<tt>false</tt>), or thrown (<tt>true</tt>)
	 *
	 * @return number of files we were able to retrieve via {@link PermanentBlobService#getHAFile}
	 */
	public static int checkFilesExist(
		JobID jobId, Collection<BlobKey> keys, PermanentBlobService blobService, boolean doThrow)
		throws IOException {

		int numFiles = 0;

		for (BlobKey key : keys) {
			final File blobFile;
			if (blobService instanceof BlobServer) {
				BlobServer server = (BlobServer) blobService;
				blobFile = server.getStorageLocation(jobId, key);
			} else {
				PermanentBlobCache cache = (PermanentBlobCache) blobService;
				blobFile = cache.getStorageLocation(jobId, key);
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
