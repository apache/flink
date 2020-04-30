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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobCache;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.runtime.blob.BlobServerCleanupTest.checkFileCountForJob;
import static org.apache.flink.runtime.blob.BlobServerCleanupTest.checkFilesExist;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for {@link BlobLibraryCacheManager}.
 */
public class BlobLibraryCacheManagerTest extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Tests that the {@link BlobLibraryCacheManager} cleans up after calling {@link
	 * BlobLibraryCacheManager#unregisterJob(JobID)}.
	 */
	@Test
	public void testLibraryCacheManagerJobCleanup() throws IOException, InterruptedException {

		JobID jobId1 = new JobID();
		JobID jobId2 = new JobID();
		List<PermanentBlobKey> keys1 = new ArrayList<>();
		List<PermanentBlobKey> keys2 = new ArrayList<>();
		BlobServer server = null;
		PermanentBlobCache cache = null;
		BlobLibraryCacheManager libCache = null;

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

			keys1.add(server.putPermanent(jobId1, buf));
			buf[0] += 1;
			keys1.add(server.putPermanent(jobId1, buf));
			keys2.add(server.putPermanent(jobId2, buf));

			libCache = new BlobLibraryCacheManager(cache, FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST, new String[0]);
			cache.registerJob(jobId1);
			cache.registerJob(jobId2);

			assertEquals(0, libCache.getNumberOfManagedJobs());
			assertEquals(0, libCache.getNumberOfReferenceHolders(jobId1));
			checkFileCountForJob(2, jobId1, server);
			checkFileCountForJob(0, jobId1, cache);
			checkFileCountForJob(1, jobId2, server);
			checkFileCountForJob(0, jobId2, cache);

			libCache.registerJob(jobId1, keys1, Collections.<URL>emptyList());
			ClassLoader classLoader1 = libCache.getClassLoader(jobId1);

			assertEquals(1, libCache.getNumberOfManagedJobs());
			assertEquals(1, libCache.getNumberOfReferenceHolders(jobId1));
			assertEquals(0, libCache.getNumberOfReferenceHolders(jobId2));
			assertEquals(2, checkFilesExist(jobId1, keys1, cache, true));
			checkFileCountForJob(2, jobId1, server);
			checkFileCountForJob(2, jobId1, cache);
			assertEquals(0, checkFilesExist(jobId2, keys2, cache, false));
			checkFileCountForJob(1, jobId2, server);
			checkFileCountForJob(0, jobId2, cache);

			libCache.registerJob(jobId2, keys2, Collections.<URL>emptyList());
			ClassLoader classLoader2 = libCache.getClassLoader(jobId2);
			assertNotEquals(classLoader1, classLoader2);

			try {
				libCache.registerJob(jobId2, keys1, Collections.<URL>emptyList());
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// that's what we want
			}

			try {
				libCache.registerJob(
					jobId2, keys2,
					Collections.singletonList(new URL("file:///tmp/does-not-exist")));
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// that's what we want
			}

			assertEquals(2, libCache.getNumberOfManagedJobs());
			assertEquals(1, libCache.getNumberOfReferenceHolders(jobId1));
			assertEquals(1, libCache.getNumberOfReferenceHolders(jobId2));
			assertEquals(2, checkFilesExist(jobId1, keys1, cache, true));
			checkFileCountForJob(2, jobId1, server);
			checkFileCountForJob(2, jobId1, cache);
			assertEquals(1, checkFilesExist(jobId2, keys2, cache, true));
			checkFileCountForJob(1, jobId2, server);
			checkFileCountForJob(1, jobId2, cache);

			libCache.unregisterJob(jobId1);

			assertEquals(1, libCache.getNumberOfManagedJobs());
			assertEquals(0, libCache.getNumberOfReferenceHolders(jobId1));
			assertEquals(1, libCache.getNumberOfReferenceHolders(jobId2));
			assertEquals(2, checkFilesExist(jobId1, keys1, cache, true));
			checkFileCountForJob(2, jobId1, server);
			checkFileCountForJob(2, jobId1, cache);
			assertEquals(1, checkFilesExist(jobId2, keys2, cache, true));
			checkFileCountForJob(1, jobId2, server);
			checkFileCountForJob(1, jobId2, cache);

			libCache.unregisterJob(jobId2);

			assertEquals(0, libCache.getNumberOfManagedJobs());
			assertEquals(0, libCache.getNumberOfReferenceHolders(jobId1));
			assertEquals(0, libCache.getNumberOfReferenceHolders(jobId2));
			assertEquals(2, checkFilesExist(jobId1, keys1, cache, true));
			checkFileCountForJob(2, jobId1, server);
			checkFileCountForJob(2, jobId1, cache);
			assertEquals(1, checkFilesExist(jobId2, keys2, cache, true));
			checkFileCountForJob(1, jobId2, server);
			checkFileCountForJob(1, jobId2, cache);

			// only PermanentBlobCache#releaseJob() calls clean up files (tested in BlobCacheCleanupTest etc.
		}
		finally {
			if (libCache != null) {
				libCache.shutdown();
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
	 * Tests that the {@link BlobLibraryCacheManager} cleans up after calling {@link
	 * BlobLibraryCacheManager#unregisterTask(JobID, ExecutionAttemptID)}.
	 */
	@Test
	public void testLibraryCacheManagerTaskCleanup() throws IOException, InterruptedException {

		JobID jobId = new JobID();
		ExecutionAttemptID attempt1 = new ExecutionAttemptID();
		ExecutionAttemptID attempt2 = new ExecutionAttemptID();
		List<PermanentBlobKey> keys = new ArrayList<>();
		BlobServer server = null;
		PermanentBlobCache cache = null;
		BlobLibraryCacheManager libCache = null;

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

			keys.add(server.putPermanent(jobId, buf));
			buf[0] += 1;
			keys.add(server.putPermanent(jobId, buf));

			libCache = new BlobLibraryCacheManager(cache, FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST, new String[0]);
			cache.registerJob(jobId);

			assertEquals(0, libCache.getNumberOfManagedJobs());
			assertEquals(0, libCache.getNumberOfReferenceHolders(jobId));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			libCache.registerTask(jobId, attempt1, keys, Collections.<URL>emptyList());
			ClassLoader classLoader1 = libCache.getClassLoader(jobId);

			assertEquals(1, libCache.getNumberOfManagedJobs());
			assertEquals(1, libCache.getNumberOfReferenceHolders(jobId));
			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			libCache.registerTask(jobId, attempt2, keys, Collections.<URL>emptyList());
			ClassLoader classLoader2 = libCache.getClassLoader(jobId);
			assertEquals(classLoader1, classLoader2);

			try {
				libCache.registerTask(
					jobId, new ExecutionAttemptID(), Collections.emptyList(),
					Collections.emptyList());
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// that's what we want
			}

			try {
				libCache.registerTask(
					jobId, new ExecutionAttemptID(), keys,
					Collections.singletonList(new URL("file:///tmp/does-not-exist")));
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// that's what we want
			}

			assertEquals(1, libCache.getNumberOfManagedJobs());
			assertEquals(2, libCache.getNumberOfReferenceHolders(jobId));
			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			libCache.unregisterTask(jobId, attempt1);

			assertEquals(1, libCache.getNumberOfManagedJobs());
			assertEquals(1, libCache.getNumberOfReferenceHolders(jobId));
			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			libCache.unregisterTask(jobId, attempt2);

			assertEquals(0, libCache.getNumberOfManagedJobs());
			assertEquals(0, libCache.getNumberOfReferenceHolders(jobId));
			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			// only PermanentBlobCache#releaseJob() calls clean up files (tested in BlobCacheCleanupTest etc.
		}
		finally {
			if (libCache != null) {
				libCache.shutdown();
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
	 * Tests that the {@link BlobLibraryCacheManager} cleans up after calling {@link
	 * BlobLibraryCacheManager#unregisterTask(JobID, ExecutionAttemptID)}.
	 */
	@Test
	public void testLibraryCacheManagerMixedJobTaskCleanup() throws IOException, InterruptedException {

		JobID jobId = new JobID();
		ExecutionAttemptID attempt1 = new ExecutionAttemptID();
		List<PermanentBlobKey> keys = new ArrayList<>();
		BlobServer server = null;
		PermanentBlobCache cache = null;
		BlobLibraryCacheManager libCache = null;

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

			keys.add(server.putPermanent(jobId, buf));
			buf[0] += 1;
			keys.add(server.putPermanent(jobId, buf));

			libCache = new BlobLibraryCacheManager(cache, FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST, new String[0]);
			cache.registerJob(jobId);

			assertEquals(0, libCache.getNumberOfManagedJobs());
			assertEquals(0, libCache.getNumberOfReferenceHolders(jobId));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			libCache.registerJob(jobId, keys, Collections.<URL>emptyList());
			ClassLoader classLoader1 = libCache.getClassLoader(jobId);

			assertEquals(1, libCache.getNumberOfManagedJobs());
			assertEquals(1, libCache.getNumberOfReferenceHolders(jobId));
			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			libCache.registerTask(jobId, attempt1, keys, Collections.<URL>emptyList());
			ClassLoader classLoader2 = libCache.getClassLoader(jobId);
			assertEquals(classLoader1, classLoader2);

			try {
				libCache.registerTask(
					jobId, new ExecutionAttemptID(), Collections.emptyList(),
					Collections.emptyList());
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// that's what we want
			}

			try {
				libCache.registerTask(
					jobId, new ExecutionAttemptID(), keys,
					Collections.singletonList(new URL("file:///tmp/does-not-exist")));
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// that's what we want
			}

			assertEquals(1, libCache.getNumberOfManagedJobs());
			assertEquals(2, libCache.getNumberOfReferenceHolders(jobId));
			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			libCache.unregisterJob(jobId);

			assertEquals(1, libCache.getNumberOfManagedJobs());
			assertEquals(1, libCache.getNumberOfReferenceHolders(jobId));
			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			libCache.unregisterTask(jobId, attempt1);

			assertEquals(0, libCache.getNumberOfManagedJobs());
			assertEquals(0, libCache.getNumberOfReferenceHolders(jobId));
			assertEquals(2, checkFilesExist(jobId, keys, cache, true));
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(2, jobId, cache);

			// only PermanentBlobCache#releaseJob() calls clean up files (tested in BlobCacheCleanupTest etc.
		}
		finally {
			if (libCache != null) {
				libCache.shutdown();
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

	@Test
	public void testRegisterAndDownload() throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		JobID jobId = new JobID();
		BlobServer server = null;
		PermanentBlobCache cache = null;
		BlobLibraryCacheManager libCache = null;
		File cacheDir = null;
		try {
			// create the blob transfer services
			Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());
			config.setLong(BlobServerOptions.CLEANUP_INTERVAL, 1_000_000L);

			server = new BlobServer(config, new VoidBlobStore());
			server.start();
			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			cache = new PermanentBlobCache(config, new VoidBlobStore(), serverAddress);

			// upload some meaningless data to the server
			PermanentBlobKey dataKey1 = server.putPermanent(jobId, new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
			PermanentBlobKey dataKey2 = server.putPermanent(jobId, new byte[]{11, 12, 13, 14, 15, 16, 17, 18});

			libCache = new BlobLibraryCacheManager(cache, FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST, new String[0]);
			assertEquals(0, libCache.getNumberOfManagedJobs());
			checkFileCountForJob(2, jobId, server);
			checkFileCountForJob(0, jobId, cache);

			// first try to access a non-existing entry
			assertEquals(0, libCache.getNumberOfReferenceHolders(new JobID()));
			try {
				libCache.getClassLoader(new JobID());
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// that's what we want
			}

			// register some BLOBs as libraries
			{
				Collection<PermanentBlobKey> keys = Collections.singleton(dataKey1);

				cache.registerJob(jobId);
				ExecutionAttemptID executionId = new ExecutionAttemptID();
				libCache.registerTask(jobId, executionId, keys, Collections.<URL>emptyList());
				ClassLoader classLoader1 = libCache.getClassLoader(jobId);
				assertEquals(1, libCache.getNumberOfManagedJobs());
				assertEquals(1, libCache.getNumberOfReferenceHolders(jobId));
				assertEquals(1, checkFilesExist(jobId, keys, cache, true));
				checkFileCountForJob(2, jobId, server);
				checkFileCountForJob(1, jobId, cache);
				assertNotNull(libCache.getClassLoader(jobId));

				libCache.registerJob(jobId, keys, Collections.<URL>emptyList());
				ClassLoader classLoader2 = libCache.getClassLoader(jobId);
				assertEquals(classLoader1, classLoader2);
				assertEquals(1, libCache.getNumberOfManagedJobs());
				assertEquals(2, libCache.getNumberOfReferenceHolders(jobId));
				assertEquals(1, checkFilesExist(jobId, keys, cache, true));
				checkFileCountForJob(2, jobId, server);
				checkFileCountForJob(1, jobId, cache);
				assertNotNull(libCache.getClassLoader(jobId));

				// un-register the job
				libCache.unregisterJob(jobId);
				// still one task
				assertEquals(1, libCache.getNumberOfManagedJobs());
				assertEquals(1, libCache.getNumberOfReferenceHolders(jobId));
				assertEquals(1, checkFilesExist(jobId, keys, cache, true));
				checkFileCountForJob(2, jobId, server);
				checkFileCountForJob(1, jobId, cache);

				// unregister the task registration
				libCache.unregisterTask(jobId, executionId);
				assertEquals(0, libCache.getNumberOfManagedJobs());
				assertEquals(0, libCache.getNumberOfReferenceHolders(jobId));
				// changing the libCache registration does not influence the BLOB stores...
				checkFileCountForJob(2, jobId, server);
				checkFileCountForJob(1, jobId, cache);

				// Don't fail if called again
				libCache.unregisterJob(jobId);
				assertEquals(0, libCache.getNumberOfManagedJobs());
				assertEquals(0, libCache.getNumberOfReferenceHolders(jobId));

				libCache.unregisterTask(jobId, executionId);
				assertEquals(0, libCache.getNumberOfManagedJobs());
				assertEquals(0, libCache.getNumberOfReferenceHolders(jobId));

				cache.releaseJob(jobId);

				// library is still cached (but not associated with job any more)
				checkFileCountForJob(2, jobId, server);
				checkFileCountForJob(1, jobId, cache);
			}

			// see BlobUtils for the directory layout
			cacheDir = cache.getStorageLocation(jobId, new PermanentBlobKey()).getParentFile();
			assertTrue(cacheDir.exists());

			// make sure no further blobs can be downloaded by removing the write
			// permissions from the directory
			assertTrue("Could not remove write permissions from cache directory", cacheDir.setWritable(false, false));

			// since we cannot download this library any more, this call should fail
			try {
				cache.registerJob(jobId);
				libCache.registerTask(jobId, new ExecutionAttemptID(), Collections.singleton(dataKey2),
					Collections.<URL>emptyList());
				fail("This should fail with an IOException");
			}
			catch (IOException e) {
				// splendid!
				cache.releaseJob(jobId);
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
			if (libCache != null) {
				libCache.shutdown();
			}
			if (server != null) {
				server.close();
			}
		}
	}
}
