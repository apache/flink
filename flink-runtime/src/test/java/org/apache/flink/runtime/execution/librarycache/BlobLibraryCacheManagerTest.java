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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.util.OperatingSystem;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class BlobLibraryCacheManagerTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests that the {@link BlobLibraryCacheManager} cleans up after calling {@link
	 * BlobLibraryCacheManager#unregisterJob(JobID)} when used with a {@link BlobCache}
	 * (note that the {@link BlobServer} does not perform cleanups based on ref-counting
	 * anymore).
	 */
	@Test
	public void testLibraryCacheManagerJobCleanup() throws IOException, InterruptedException {

		JobID jobId = new JobID();
		List<BlobKey> keys = new ArrayList<BlobKey>();
		BlobServer server = null;
		BlobCache cache = null;
		BlobLibraryCacheManager libraryCacheManager = null;

		final byte[] buf = new byte[128];

		try {
			Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());
			config.setLong(ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL, 1L);

			server = new BlobServer(config, new VoidBlobStore());
			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			BlobClient bc = new BlobClient(serverAddress, config);
			cache = new BlobCache(serverAddress, config, new VoidBlobStore());

			keys.add(bc.put(jobId, buf));
			buf[0] += 1;
			keys.add(bc.put(jobId, buf));

			bc.close();

			libraryCacheManager = new BlobLibraryCacheManager(cache);
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			libraryCacheManager.registerJob(jobId, keys, Collections.<URL>emptyList());
			libraryCacheManager.registerJob(jobId, keys, Collections.<URL>emptyList());

			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			libraryCacheManager.unregisterJob(jobId);

			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			libraryCacheManager.unregisterJob(jobId);

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
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (libraryCacheManager != null) {
				try {
					libraryCacheManager.shutdown();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}

			// should have been closed by the libraryCacheManager, but just in case
			if (cache != null) {
				cache.close();
			}

			if (server != null) {
				server.close();
			}
		}
	}

	/**
	 * Tests that the {@link BlobLibraryCacheManager} does not clean up after calling {@link
	 * BlobLibraryCacheManager#unregisterJob(JobID)} when used with a {@link BlobServer}
	 * (note that the {@link BlobServer} does not perform cleanups based on ref-counting
	 * anymore).
	 */
	@Test
	public void testLibraryCacheManagerJobCleanupAtServer() throws IOException, InterruptedException {

		JobID jobId = new JobID();
		List<BlobKey> keys = new ArrayList<BlobKey>();
		BlobServer server = null;
		BlobLibraryCacheManager libraryCacheManager = null;

		final byte[] buf = new byte[128];

		try {
			Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());
			config.setLong(ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL, 1L);

			server = new BlobServer(config, new VoidBlobStore());
			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			BlobClient bc = new BlobClient(serverAddress, config);

			keys.add(bc.put(jobId, buf));
			buf[0] += 1;
			keys.add(bc.put(jobId, buf));

			bc.close();

			libraryCacheManager = new BlobLibraryCacheManager(server);
			checkFileCountForJob(2, jobId, server);

			libraryCacheManager.registerJob(jobId, keys, Collections.<URL>emptyList());
			libraryCacheManager.registerJob(jobId, keys, Collections.<URL>emptyList());

			assertEquals(2, checkFilesExist(jobId, keys, server, true));
			checkFileCountForJob(2, jobId, server);

			libraryCacheManager.unregisterJob(jobId);

			// still one job registered
			assertEquals(2, checkFilesExist(jobId, keys, server, true));
			checkFileCountForJob(2, jobId, server);

			libraryCacheManager.unregisterJob(jobId);

			// the last unregister should NOT cleanup at the BlobServer!
			checkFileCountForJob(2, jobId, server);
			assertEquals(2, checkFilesExist(jobId, keys, server, true));

			server.cleanupJob(jobId);
			checkFileCountForJob(0, jobId, server);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (libraryCacheManager != null) {
				try {
					libraryCacheManager.shutdown();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}

			// should have been closed by the libraryCacheManager, but just in case
			if (server != null) {
				server.close();
			}
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
	 * @return number of files we were able to retrieve via {@link BlobService#getFile}
	 */
	private static int checkFilesExist(
			JobID jobId, Collection<BlobKey> keys, BlobService blobService, boolean doThrow)
			throws IOException {

		int numFiles = 0;

		for (BlobKey key : keys) {
			final File blobFile;
			if (blobService instanceof BlobServer) {
				BlobServer server = (BlobServer) blobService;
				blobFile = server.getStorageLocation(jobId, key);
			} else {
				BlobCache cache = (BlobCache) blobService;
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
	 *
	 * @return number of files we were able to retrieve via {@link BlobService#getFile}
	 */
	private static void checkFileCountForJob(
			int expectedCount, JobID jobId, BlobService blobService)
			throws IOException {

		final File jobDir;
		if (blobService instanceof BlobServer) {
			BlobServer server = (BlobServer) blobService;
			jobDir = server.getStorageLocation(jobId, new BlobKey()).getParentFile();
		} else {
			BlobCache cache = (BlobCache) blobService;
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

	@Test
	public void testRegisterAndDownload() throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		JobID jobId = new JobID();
		BlobServer server = null;
		BlobCache cache = null;
		File cacheDir = null;
		try {
			// create the blob transfer services
			Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());
			config.setLong(ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL, 1_000_000L);


			server = new BlobServer(config, new VoidBlobStore());
			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			cache = new BlobCache(serverAddress, config, new VoidBlobStore());

			// upload some meaningless data to the server
			BlobClient uploader = new BlobClient(serverAddress, config);
			BlobKey dataKey1 = uploader.put(jobId, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
			BlobKey dataKey2 = uploader.put(jobId, new byte[]{11, 12, 13, 14, 15, 16, 17, 18});
			uploader.close();

			BlobLibraryCacheManager libCache = new BlobLibraryCacheManager(cache);
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			// register some BLOBs as libraries
			{
				Collection<BlobKey> keys = Collections.singleton(dataKey1);

				assertNotNull(libCache.registerJob(jobId, keys, Collections.<URL>emptyList()));
				assertEquals(1, checkFilesExist(jobId, keys, cache, true));
				checkFileCountForJob(2, jobId, server);
				checkFileCountForJob(1, jobId, cache);

				// un-register them again
				libCache.unregisterJob(jobId);

				// Don't fail if called again
				libCache.unregisterJob(jobId);

				// library is still cached (but not associated with job any more)
				checkFileCountForJob(2, jobId, server);
				// TODO: fix check when deferred cleanup is implemented
				checkFileCountForJob(0, jobId, cache);
			}

			// see BlobUtils for the directory layout
			cacheDir = cache.getStorageLocation(jobId, new BlobKey()).getParentFile();
			assertTrue(cacheDir.exists());

			// make sure no further blobs can be downloaded by removing the write
			// permissions from the directory
			assertTrue("Could not remove write permissions from cache directory", cacheDir.setWritable(false, false));

			// since we cannot download this library any more, this call should fail
			try {
				libCache.registerJob(jobId, Collections.singleton(dataKey2),
						Collections.<URL>emptyList());
				fail("This should fail with an IOException");
			}
			catch (IOException e) {
				// splendid!
			}
		} finally {
			if (cacheDir != null) {
				if (!cacheDir.setWritable(true, false)) {
					System.err.println("Could not re-add write permissions to cache directory.");
				}
			}
			if (cache != null) {
				cache.close();
			}
			if (server != null) {
				server.close();
			}
		}
	}
}
