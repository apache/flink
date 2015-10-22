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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.util.OperatingSystem;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class BlobLibraryCacheManagerTest {

	@Test
	public void testLibraryCacheManagerCleanup() {
		Configuration config = new Configuration();

		config.setLong(ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL, 1);
		GlobalConfiguration.includeConfiguration(config);

		JobID jid = new JobID();
		List<BlobKey> keys = new ArrayList<BlobKey>();
		BlobServer server = null;
		BlobLibraryCacheManager libraryCacheManager = null;

		final byte[] buf = new byte[128];

		try {
			server = new BlobServer(new Configuration());
			InetSocketAddress blobSocketAddress = new InetSocketAddress(server.getPort());
			BlobClient bc = new BlobClient(blobSocketAddress);

			keys.add(bc.put(buf));
			buf[0] += 1;
			keys.add(bc.put(buf));

			long cleanupInterval = 1000l;
			libraryCacheManager = new BlobLibraryCacheManager(server, cleanupInterval);
			libraryCacheManager.registerJob(jid, keys, Collections.<URL>emptyList());

			List<File> files = new ArrayList<File>();

			for (BlobKey key : keys) {
				files.add(libraryCacheManager.getFile(key));
			}

			assertEquals(2, files.size());
			files.clear();

			libraryCacheManager.unregisterJob(jid);

			// because we cannot guarantee that there are not thread races in the build system, we
			// loop for a certain while until the references disappear
			{
				long deadline = System.currentTimeMillis() + 30000;
				do {
					Thread.sleep(500);
				}
				while (libraryCacheManager.getNumberOfCachedLibraries() > 0 &&
						System.currentTimeMillis() < deadline);
			}

			// this fails if we exited via a timeout
			assertEquals(0, libraryCacheManager.getNumberOfCachedLibraries());

			int caughtExceptions = 0;

			for (BlobKey key : keys) {
				// the blob cache should no longer contain the files
				try {
					files.add(libraryCacheManager.getFile(key));
				}
				catch (IOException ioe) {
					caughtExceptions++;
				}
			}

			assertEquals(2, caughtExceptions);

			bc.close();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (server != null) {
				server.shutdown();
			}

			if (libraryCacheManager != null) {
				try {
					libraryCacheManager.shutdown();
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Test
	public void testRegisterAndDownload() {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		BlobServer server = null;
		BlobCache cache = null;
		File cacheDir = null;
		try {
			// create the blob transfer services
			Configuration config = new Configuration();
			server = new BlobServer(config);
			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			cache = new BlobCache(serverAddress, config);

			// upload some meaningless data to the server
			BlobClient uploader = new BlobClient(serverAddress);
			BlobKey dataKey1 = uploader.put(new byte[]{1, 2, 3, 4, 5, 6, 7, 8});
			BlobKey dataKey2 = uploader.put(new byte[]{11, 12, 13, 14, 15, 16, 17, 18});
			uploader.close();

			BlobLibraryCacheManager libCache = new BlobLibraryCacheManager(cache, 1000000000L);

			assertEquals(0, libCache.getNumberOfCachedLibraries());

			// first try to access a non-existing entry
			try {
				libCache.getClassLoader(new JobID());
				fail("Should fail with an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// that#s what we want
			}

			// now register some BLOBs as libraries
			{
				JobID jid = new JobID();
				ExecutionAttemptID executionId = new ExecutionAttemptID();
				Collection<BlobKey> keys = Collections.singleton(dataKey1);

				libCache.registerTask(jid, executionId, keys, Collections.<URL>emptyList());
				assertEquals(1, libCache.getNumberOfReferenceHolders(jid));
				assertEquals(1, libCache.getNumberOfCachedLibraries());
				assertNotNull(libCache.getClassLoader(jid));

				// un-register them again
				libCache.unregisterTask(jid, executionId);

				// Don't fail if called again
				libCache.unregisterTask(jid, executionId);

				assertEquals(0, libCache.getNumberOfReferenceHolders(jid));

				// library is still cached (but not associated with job any more)
				assertEquals(1, libCache.getNumberOfCachedLibraries());

				// should not be able to access the classloader any more
				try {
					libCache.getClassLoader(jid);
					fail("Should fail with an IllegalStateException");
				}
				catch (IllegalStateException e) {
					// that#s what we want
				}
			}

			cacheDir = new File(cache.getStorageDir(), "cache");
			assertTrue(cacheDir.exists());

			// make sure no further blobs can be downloaded by removing the write
			// permissions from the directory
			assertTrue("Could not remove write permissions from cache directory", cacheDir.setWritable(false, false));

			// since we cannot download this library any more, this call should fail
			try {
				libCache.registerTask(new JobID(), new ExecutionAttemptID(), Collections.singleton(dataKey2),
						Collections.<URL>emptyList());
				fail("This should fail with an IOException");
			}
			catch (IOException e) {
				// splendid!
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (cacheDir != null) {
				if (!cacheDir.setWritable(true, false)) {
					System.err.println("Could not re-add write permissions to cache directory.");
				}
			}
			if (cache != null) {
				cache.shutdown();
			}
			if (server != null) {
				server.shutdown();
			}
		}
	}
}
