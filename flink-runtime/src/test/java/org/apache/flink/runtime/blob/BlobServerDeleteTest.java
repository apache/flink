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
import org.apache.flink.runtime.concurrent.FlinkFutureException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobCacheCleanupTest.checkFileCountForJob;
import static org.apache.flink.runtime.blob.BlobCacheCleanupTest.checkFilesExist;
import static org.apache.flink.runtime.blob.BlobClientTest.validateGetAndClose;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests how DELETE requests behave.
 */
public class BlobServerDeleteTest extends TestLogger {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testDeleteSingleByBlobKey() throws IOException {
		BlobServer server = null;
		BlobClient client = null;
		BlobStore blobStore = new VoidBlobStore();

		try {
			final Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, blobStore);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put job-unrelated (like libraries)
			BlobKey key1 = client.put(null, data);
			assertNotNull(key1);

			// second job-unrelated item
			data[0] ^= 1;
			BlobKey key2 = client.put(null, data);
			assertNotNull(key2);
			assertNotEquals(key1, key2);

			// put job-related with same key1 as non-job-related
			data[0] ^= 1; // back to the original data
			final JobID jobId = new JobID();
			BlobKey key1b = client.put(jobId, data);
			assertNotNull(key1b);
			assertEquals(key1, key1b);

			// issue a DELETE request via the client
			client.delete(key1);
			client.close();

			client = new BlobClient(serverAddress, config);
			try (InputStream ignored = client.get(key1)) {
				fail("BLOB should have been deleted");
			}
			catch (IOException e) {
				// expected
			}

			ensureClientIsClosed(client);

			client = new BlobClient(serverAddress, config);
			try {
				// NOTE: the server will stall in its send operation until either the data is fully
				//       read or the socket is closed, e.g. via a client.close() call
				validateGetAndClose(client.get(jobId, key1), data);
			}
			catch (IOException e) {
				fail("Deleting a job-unrelated BLOB should not affect a job-related BLOB with the same key");
			}
			client.close();

			// delete a file directly on the server
			server.delete(key2);
			try {
				server.getFile(key2);
				fail("BLOB should have been deleted");
			}
			catch (IOException e) {
				// expected
			}
		}
		finally {
			cleanup(server, client);
		}
	}

	private static void ensureClientIsClosed(final BlobClient client) throws IOException {
		try {
			client.put(null, new byte[1]);
			fail("client should be closed after erroneous operation");
		}
		catch (IllegalStateException e) {
			// expected
		} finally {
			client.close();
		}
	}

	@Test
	public void testDeleteAlreadyDeletedNoJob() throws IOException {
		testDeleteAlreadyDeleted(null);
	}

	@Test
	public void testDeleteAlreadyDeletedForJob() throws IOException {
		testDeleteAlreadyDeleted(new JobID());
	}

	private void testDeleteAlreadyDeleted(final JobID jobId) throws IOException {
		BlobServer server = null;
		BlobClient client = null;
		BlobStore blobStore = new VoidBlobStore();

		try {
			final Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, blobStore);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put file
			BlobKey key = client.put(jobId, data);
			assertNotNull(key);

			File blobFile = server.getStorageLocation(jobId, key);
			assertTrue(blobFile.delete());

			// issue a DELETE request via the client
			try {
				deleteHelper(client, jobId, key);
			}
			catch (IOException e) {
				fail("DELETE operation should not fail if file is already deleted");
			}

			// issue a DELETE request on the server
			if (jobId == null) {
				server.delete(key);
			} else {
				server.delete(jobId, key);
			}
		}
		finally {
			cleanup(server, client);
		}
	}

	private static void deleteHelper(BlobClient client, JobID jobId, BlobKey key) throws IOException {
		if (jobId == null) {
			client.delete(key);
		} else {
			client.delete(jobId, key);
		}
	}

	@Test
	public void testDeleteFailsNoJob() throws IOException {
		testDeleteFails(null);
	}

	@Test
	public void testDeleteFailsForJob() throws IOException {
		testDeleteFails(new JobID());
	}

	private void testDeleteFails(final JobID jobId) throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		BlobServer server = null;
		BlobClient client = null;
		BlobStore blobStore = new VoidBlobStore();

		File blobFile = null;
		File directory = null;
		try {
			final Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, blobStore);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = client.put(jobId, data);
			assertNotNull(key);

			blobFile = server.getStorageLocation(jobId, key);
			directory = blobFile.getParentFile();

			assertTrue(blobFile.setWritable(false, false));
			assertTrue(directory.setWritable(false, false));

			// issue a DELETE request via the client
			deleteHelper(client, jobId, key);

			// issue a DELETE request on the server
			if (jobId == null) {
				server.delete(key);
			} else {
				server.delete(jobId, key);
			}

			// the file should still be there
			if (jobId == null) {
				server.getFile(key);
			} else {
				server.getFile(jobId, key);
			}
		} finally {
			if (blobFile != null && directory != null) {
				//noinspection ResultOfMethodCallIgnored
				blobFile.setWritable(true, false);
				//noinspection ResultOfMethodCallIgnored
				directory.setWritable(true, false);
			}
			cleanup(server, client);
		}
	}

	/**
	 * Tests that {@link BlobServer} cleans up after calling {@link BlobServer#cleanupJob(JobID)}.
	 */
	@Test
	public void testJobCleanup() throws IOException, InterruptedException {

		JobID jobId1 = new JobID();
		List<BlobKey> keys1 = new ArrayList<BlobKey>();
		JobID jobId2 = new JobID();
		List<BlobKey> keys2 = new ArrayList<BlobKey>();
		BlobServer server = null;

		final byte[] buf = new byte[128];

		try {
			Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY,
				temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, new VoidBlobStore());
			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			BlobClient bc = new BlobClient(serverAddress, config);

			keys1.add(bc.put(jobId1, buf));
			keys2.add(bc.put(jobId2, buf));
			assertEquals(keys2.get(0), keys1.get(0));

			buf[0] += 1;
			keys1.add(bc.put(jobId1, buf));

			bc.close();

			assertEquals(2, checkFilesExist(jobId1, keys1, server, true));
			checkFileCountForJob(2, jobId1, server);
			assertEquals(1, checkFilesExist(jobId2, keys2, server, true));
			checkFileCountForJob(1, jobId2, server);

			server.cleanupJob(jobId1);

			checkFileCountForJob(0, jobId1, server);
			assertEquals(1, checkFilesExist(jobId2, keys2, server, true));
			checkFileCountForJob(1, jobId2, server);

			server.cleanupJob(jobId2);

			checkFileCountForJob(0, jobId1, server);
			checkFileCountForJob(0, jobId2, server);

			// calling a second time should not fail
			server.cleanupJob(jobId2);
		}
		finally {
			if (server != null) {
				server.close();
			}
		}
	}

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent delete operations don't interfere with each other.
	 *
	 * Note: The test checks that there cannot be two threads which have checked whether a given blob file exist
	 * and then one of them fails deleting it. Without the introduced lock, this situation should rarely happen
	 * and make this test fail. Thus, if this test should become "unstable", then the delete atomicity is most likely
	 * broken.
	 */
	@Test
	public void testConcurrentDeleteOperationsNoJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentDeleteOperations(null);
	}

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent delete operations don't interfere with each other.
	 *
	 * Note: The test checks that there cannot be two threads which have checked whether a given blob file exist
	 * and then one of them fails deleting it. Without the introduced lock, this situation should rarely happen
	 * and make this test fail. Thus, if this test should become "unstable", then the delete atomicity is most likely
	 * broken.
	 */
	@Test
	public void testConcurrentDeleteOperationsForJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentDeleteOperations(new JobID());
	}

	private void testConcurrentDeleteOperations(final JobID jobId)
		throws IOException, InterruptedException, ExecutionException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
		final BlobStore blobStore = mock(BlobStore.class);

		final int concurrentDeleteOperations = 3;
		final ExecutorService executor = Executors.newFixedThreadPool(concurrentDeleteOperations);

		final List<CompletableFuture<Void>> deleteFutures = new ArrayList<>(concurrentDeleteOperations);

		final byte[] data = {1, 2, 3};

		try (final BlobServer blobServer = new BlobServer(config, blobStore)) {

			final BlobKey blobKey;

			try (BlobClient client = blobServer.createClient()) {
				blobKey = client.put(jobId, data);
			}

			assertTrue(blobServer.getStorageLocation(jobId, blobKey).exists());

			for (int i = 0; i < concurrentDeleteOperations; i++) {
				CompletableFuture<Void> deleteFuture = CompletableFuture.supplyAsync(
					() -> {
						try (BlobClient blobClient = blobServer.createClient()) {
							deleteHelper(blobClient, jobId, blobKey);
						} catch (IOException e) {
							throw new FlinkFutureException("Could not delete the given blob key " + blobKey + '.', e);
						}

						return null;
					},
					executor);

				deleteFutures.add(deleteFuture);
			}

			CompletableFuture<Void> waitFuture = FutureUtils.waitForAll(deleteFutures);

			// make sure all delete operation have completed successfully
			// in case of no lock, one of the delete operations should eventually fail
			waitFuture.get();

			assertFalse(blobServer.getStorageLocation(jobId, blobKey).exists());
		} finally {
			executor.shutdownNow();
		}
	}

	private static void cleanup(BlobServer server, BlobClient client) {
		if (client != null) {
			try {
				client.close();
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}
		if (server != null) {
			try {
				server.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
