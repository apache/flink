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
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.concurrent.FlinkFutureException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobClientTest.validateGetAndClose;
import static org.apache.flink.runtime.blob.BlobServerGetTest.getFileHelper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for successful and failing PUT operations against the BLOB server,
 * and successful GET operations.
 */
public class BlobServerPutTest extends TestLogger {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	// --- concurrency tests for utility methods which could fail during the put operation ---

	/**
	 * Checked thread that calls {@link BlobServer#getStorageLocation(JobID, BlobKey)}.
	 */
	public static class ContentAddressableGetStorageLocation extends CheckedThread {
		private final BlobServer server;
		private final JobID jobId;
		private final BlobKey key;

		public ContentAddressableGetStorageLocation(BlobServer server, JobID jobId, BlobKey key) {
			this.server = server;
			this.jobId = jobId;
			this.key = key;
		}

		@Override
		public void go() throws Exception {
			server.getStorageLocation(jobId, key);
		}
	}

	/**
	 * Tests concurrent calls to {@link BlobServer#getStorageLocation(JobID, BlobKey)}.
	 */
	@Test
	public void testServerContentAddressableGetStorageLocationConcurrentNoJob() throws Exception {
		testServerContentAddressableGetStorageLocationConcurrent(null);
	}

	/**
	 * Tests concurrent calls to {@link BlobServer#getStorageLocation(JobID, BlobKey)}.
	 */
	@Test
	public void testServerContentAddressableGetStorageLocationConcurrentForJob() throws Exception {
		testServerContentAddressableGetStorageLocationConcurrent(new JobID());
	}

	private void testServerContentAddressableGetStorageLocationConcurrent(final JobID jobId)
		throws Exception {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		BlobServer server = new BlobServer(config, new VoidBlobStore());

		try {
			BlobKey key = new BlobKey();
			CheckedThread[] threads = new CheckedThread[] {
				new ContentAddressableGetStorageLocation(server, jobId, key),
				new ContentAddressableGetStorageLocation(server, jobId, key),
				new ContentAddressableGetStorageLocation(server, jobId, key)
			};
			checkedThreadSimpleTest(threads);
		} finally {
			server.close();
		}
	}

	/**
	 * Helper method to first start all threads and then wait for their completion.
	 *
	 * @param threads threads to use
	 * @throws Exception exceptions that are thrown from the threads
	 */
	protected void checkedThreadSimpleTest(CheckedThread[] threads)
		throws Exception {

		// start all threads
		for (CheckedThread t: threads) {
			t.start();
		}

		// wait for thread completion and check exceptions
		for (CheckedThread t: threads) {
			t.sync();
		}
	}

	// --------------------------------------------------------------------------------------------

	@Test
	public void testPutBufferSuccessfulGet1() throws IOException {
		testPutBufferSuccessfulGet(null, null);
	}

	@Test
	public void testPutBufferSuccessfulGet2() throws IOException {
		testPutBufferSuccessfulGet(null, new JobID());
	}

	@Test
	public void testPutBufferSuccessfulGet3() throws IOException {
		testPutBufferSuccessfulGet(new JobID(), new JobID());
	}

	@Test
	public void testPutBufferSuccessfulGet4() throws IOException {
		testPutBufferSuccessfulGet(new JobID(), null);
	}

	private void testPutBufferSuccessfulGet(final JobID jobId1, final JobID jobId2)
		throws IOException {
		BlobServer server = null;
		BlobClient client = null;

		try {
			final Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, new VoidBlobStore());

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put data for jobId1 and verify
			BlobKey key1a = client.put(jobId1, data);
			assertNotNull(key1a);

			BlobKey key1b = client.put(jobId1, data, 10, 44);
			assertNotNull(key1b);

			testPutBufferSuccessfulGet(jobId1, key1a, key1b, data, serverAddress, config);

			// now put data for jobId2 and verify that both are ok
			BlobKey key2a = client.put(jobId2, data);
			assertNotNull(key2a);
			assertEquals(key1a, key2a);

			BlobKey key2b = client.put(jobId2, data, 10, 44);
			assertNotNull(key2b);
			assertEquals(key1b, key2b);


			testPutBufferSuccessfulGet(jobId1, key1a, key1b, data, serverAddress, config);
			testPutBufferSuccessfulGet(jobId2, key2a, key2b, data, serverAddress, config);


		} finally {
			if (client != null) {
				client.close();
			}
			if (server != null) {
				server.close();
			}
		}
	}

	/**
	 * GET the data stored at the two keys and check that it is equal to <tt>data</tt>.
	 *
	 * @param jobId
	 * 		job ID or <tt>null</tt> if job-unrelated
	 * @param key1
	 * 		first key for 44 bytes starting at byte 10 of data in the BLOB
	 * @param key2
	 * 		second key for the complete data in the BLOB
	 * @param data
	 * 		expected data
	 * @param serverAddress
	 * 		BlobServer address to connect to
	 * @param config
	 * 		client configuration
	 */
	private static void testPutBufferSuccessfulGet(
			JobID jobId, BlobKey key1, BlobKey key2, byte[] data,
			InetSocketAddress serverAddress, Configuration config) throws IOException {

		BlobClient client = new BlobClient(serverAddress, config);

		// one get request on the same client
		try (InputStream is1 = getFileHelper(client, jobId, key2)) {
			byte[] result1 = new byte[44];
			BlobUtils.readFully(is1, result1, 0, result1.length, null);
			is1.close();

			for (int i = 0, j = 10; i < result1.length; i++, j++) {
				assertEquals(data[j], result1[i]);
			}

			// close the client and create a new one for the remaining request
			client.close();
			client = new BlobClient(serverAddress, config);

			validateGetAndClose(getFileHelper(client, jobId, key1), data);
		} finally {
			client.close();
		}
	}

	@Test
	public void testPutStreamSuccessfulNoJob() throws IOException {
		testPutStreamSuccessful(null);
	}

	@Test
	public void testPutStreamSuccessfulForJob() throws IOException {
		testPutStreamSuccessful(new JobID());
	}

	private void testPutStreamSuccessful(final JobID jobId) throws IOException {
		BlobServer server = null;
		BlobClient client = null;

		try {
			final Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, new VoidBlobStore());

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			{
				BlobKey key1;
				if (jobId == null) {
					key1 = client.put(new ByteArrayInputStream(data));
				} else {
					key1 = client.put(jobId, new ByteArrayInputStream(data));
				}
				assertNotNull(key1);
			}
		} finally {
			if (client != null) {
				try {
					client.close();
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
			if (server != null) {
				server.close();
			}
		}
	}

	@Test
	public void testPutChunkedStreamSuccessfulNoJob() throws IOException {
		testPutChunkedStreamSuccessful(null);
	}

	@Test
	public void testPutChunkedStreamSuccessfulForJob() throws IOException {
		testPutChunkedStreamSuccessful(new JobID());
	}

	private void testPutChunkedStreamSuccessful(final JobID jobId) throws IOException {
		BlobServer server = null;
		BlobClient client = null;

		try {
			final Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, new VoidBlobStore());

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			{
				BlobKey key1;
				if (jobId == null) {
					key1 = client.put(new ChunkedInputStream(data, 19));
				} else {
					key1 = client.put(jobId, new ChunkedInputStream(data, 19));
				}
				assertNotNull(key1);
			}
		} finally {
			if (client != null) {
				client.close();
			}
			if (server != null) {
				server.close();
			}
		}
	}

	@Test
	public void testPutBufferFailsNoJob() throws IOException {
		testPutBufferFails(null);
	}

	@Test
	public void testPutBufferFailsForJob() throws IOException {
		testPutBufferFails(new JobID());
	}

	private void testPutBufferFails(final JobID jobId) throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		BlobServer server = null;
		BlobClient client = null;

		File tempFileDir = null;
		try {
			final Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, new VoidBlobStore());

			// make sure the blob server cannot create any files in its storage dir
			tempFileDir = server.createTemporaryFilename().getParentFile().getParentFile();
			assertTrue(tempFileDir.setExecutable(true, false));
			assertTrue(tempFileDir.setReadable(true, false));
			assertTrue(tempFileDir.setWritable(false, false));

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			try {
				client.put(jobId, data);
				fail("This should fail.");
			}
			catch (IOException e) {
				assertTrue(e.getMessage(), e.getMessage().contains("Server side error"));
			}

			try {
				client.put(jobId, data);
				fail("Client should be closed");
			}
			catch (IllegalStateException e) {
				// expected
			}

		} finally {
			// set writable again to make sure we can remove the directory
			if (tempFileDir != null) {
				tempFileDir.setWritable(true, false);
			}
			if (client != null) {
				client.close();
			}
			if (server != null) {
				server.close();
			}
		}
	}

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent put operations will only upload the file once to the {@link BlobStore}.
	 */
	@Test
	public void testConcurrentPutOperationsNoJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentPutOperations(null);
	}

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent put operations will only upload the file once to the {@link BlobStore}.
	 */
	@Test
	public void testConcurrentPutOperationsForJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentPutOperations(new JobID());
	}

	private void testConcurrentPutOperations(final JobID jobId)
			throws IOException, InterruptedException, ExecutionException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		BlobStore blobStore = mock(BlobStore.class);
		int concurrentPutOperations = 2;
		int dataSize = 1024;

		final CountDownLatch countDownLatch = new CountDownLatch(concurrentPutOperations);
		final byte[] data = new byte[dataSize];

		ArrayList<CompletableFuture<BlobKey>> allFutures = new ArrayList<>(concurrentPutOperations);

		ExecutorService executor = Executors.newFixedThreadPool(concurrentPutOperations);

		try (
			final BlobServer blobServer = new BlobServer(config, blobStore)) {

			for (int i = 0; i < concurrentPutOperations; i++) {
				CompletableFuture<BlobKey> putFuture = CompletableFuture.supplyAsync(
					() -> {
						try (BlobClient blobClient = blobServer.createClient()) {
							if (jobId == null) {
								return blobClient
									.put(new BlockingInputStream(countDownLatch, data));
							} else {
								return blobClient
									.put(jobId, new BlockingInputStream(countDownLatch, data));
							}
						} catch (IOException e) {
							throw new FlinkFutureException("Could not upload blob.", e);
						}
					},
					executor);

				allFutures.add(putFuture);
			}

			FutureUtils.ConjunctFuture<Collection<BlobKey>> conjunctFuture = FutureUtils.combineAll(allFutures);

			// wait until all operations have completed and check that no exception was thrown
			Collection<BlobKey> blobKeys = conjunctFuture.get();

			Iterator<BlobKey> blobKeyIterator = blobKeys.iterator();

			assertTrue(blobKeyIterator.hasNext());

			BlobKey blobKey = blobKeyIterator.next();

			// make sure that all blob keys are the same
			while(blobKeyIterator.hasNext()) {
				assertEquals(blobKey, blobKeyIterator.next());
			}

			// check that we only uploaded the file once to the blob store
			verify(blobStore, times(1)).put(any(File.class), eq(jobId), eq(blobKey));
		} finally {
			executor.shutdownNow();
		}
	}

	private static final class BlockingInputStream extends InputStream {

		private final CountDownLatch countDownLatch;
		private final byte[] data;
		private int index = 0;

		public BlockingInputStream(CountDownLatch countDownLatch, byte[] data) {
			this.countDownLatch = Preconditions.checkNotNull(countDownLatch);
			this.data = Preconditions.checkNotNull(data);
		}

		@Override
		public int read() throws IOException {

			countDownLatch.countDown();

			try {
				countDownLatch.await();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IOException("Blocking operation was interrupted.", e);
			}

			if (index >= data.length) {
				return -1;
			} else {
				return data[index++];
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private static final class ChunkedInputStream extends InputStream {

		private final byte[][] data;

		private int x = 0, y = 0;


		private ChunkedInputStream(byte[] data, int numChunks) {
			this.data = new byte[numChunks][];

			int bytesPerChunk = data.length / numChunks;
			int bytesTaken = 0;
			for (int i = 0; i < numChunks - 1; i++, bytesTaken += bytesPerChunk) {
				this.data[i] = new byte[bytesPerChunk];
				System.arraycopy(data, bytesTaken, this.data[i], 0, bytesPerChunk);
			}

			this.data[numChunks -  1] = new byte[data.length - bytesTaken];
			System.arraycopy(data, bytesTaken, this.data[numChunks -  1], 0, this.data[numChunks -  1].length);
		}

		@Override
		public int read() {
			if (x < data.length) {
				byte[] curr = data[x];
				if (y < curr.length) {
					byte next = curr[y];
					y++;
					return next;
				}
				else {
					y = 0;
					x++;
					return read();
				}
			} else {
				return -1;
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (len == 0) {
				return 0;
			}
			if (x < data.length) {
				byte[] curr = data[x];
				if (y < curr.length) {
					int toCopy = Math.min(len, curr.length - y);
					System.arraycopy(curr, y, b, off, toCopy);
					y += toCopy;
					return toCopy;
				} else {
					y = 0;
					x++;
					return read(b, off, len);
				}
			}
			else {
				return -1;
			}
		}
	}
}
