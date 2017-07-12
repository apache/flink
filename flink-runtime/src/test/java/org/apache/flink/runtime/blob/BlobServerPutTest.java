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
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	// --- concurrency tests for utility methods which could fail during the put operation ---

	/**
	 * Checked thread that calls {@link BlobServer#getStorageLocation(JobID, BlobKey)}.
	 */
	public static class ContentAddressableGetStorageLocation extends CheckedThread {
		private final BlobServer server;
		private final JobID jobId;
		private final BlobKey key;

		ContentAddressableGetStorageLocation(BlobServer server, JobID jobId, BlobKey key) {
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

		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {
			BlobKey key = new BlobKey();
			CheckedThread[] threads = new CheckedThread[] {
				new ContentAddressableGetStorageLocation(server, jobId, key),
				new ContentAddressableGetStorageLocation(server, jobId, key),
				new ContentAddressableGetStorageLocation(server, jobId, key)
			};
			checkedThreadSimpleTest(threads);
		}
	}

	/**
	 * Helper method to first start all threads and then wait for their completion.
	 *
	 * @param threads threads to use
	 * @throws Exception exceptions that are thrown from the threads
	 */
	private void checkedThreadSimpleTest(CheckedThread[] threads)
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
		testPutBufferSuccessfulGet(null, null, false);
	}

	@Test
	public void testPutBufferSuccessfulGet2() throws IOException {
		testPutBufferSuccessfulGet(null, new JobID(), false);
	}

	@Test
	public void testPutBufferSuccessfulGet3() throws IOException {
		testPutBufferSuccessfulGet(new JobID(), new JobID(), false);
	}

	@Test
	public void testPutBufferSuccessfulGet4() throws IOException {
		testPutBufferSuccessfulGet(new JobID(), null, false);
	}

	@Test
	public void testPutBufferSuccessfulGetHa() throws IOException {
		testPutBufferSuccessfulGet(new JobID(), new JobID(), true);
	}

	private void testPutBufferSuccessfulGet(
			JobID jobId1, JobID jobId2, boolean highAvailability) throws IOException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			byte[] data2 = Arrays.copyOfRange(data, 10, 54);

			// put data for jobId1 and verify
			BlobKey key1a = put(server, jobId1, data, highAvailability);
			assertNotNull(key1a);

			BlobKey key1b = put(server, jobId1, data2, highAvailability);
			assertNotNull(key1b);

			verifyContents(server, jobId1, key1a, data, highAvailability);
			verifyContents(server, jobId1, key1b, data2, highAvailability);

			// now put data for jobId2 and verify that both are ok
			BlobKey key2a = put(server, jobId2, data, highAvailability);
			assertNotNull(key2a);
			assertEquals(key1a, key2a);

			BlobKey key2b = put(server, jobId2, data2, highAvailability);
			assertNotNull(key2b);
			assertEquals(key1b, key2b);

			// verify the accessibility and the BLOB contents
			verifyContents(server, jobId1, key1a, data, highAvailability);
			verifyContents(server, jobId1, key1b, data2, highAvailability);
			verifyContents(server, jobId2, key2a, data, highAvailability);
			verifyContents(server, jobId2, key2b, data2, highAvailability);
		}
	}

	// --------------------------------------------------------------------------------------------

	@Test
	public void testPutStreamSuccessfulGet1() throws IOException {
		testPutStreamSuccessfulGet(null, null, false);
	}

	@Test
	public void testPutStreamSuccessfulGet2() throws IOException {
		testPutStreamSuccessfulGet(null, new JobID(), false);
	}

	@Test
	public void testPutStreamSuccessfulGet3() throws IOException {
		testPutStreamSuccessfulGet(new JobID(), new JobID(), false);
	}

	@Test
	public void testPutStreamSuccessfulGet4() throws IOException {
		testPutStreamSuccessfulGet(new JobID(), null, false);
	}

	@Test
	public void testPutStreamSuccessfulGetHa() throws IOException {
		testPutStreamSuccessfulGet(new JobID(), new JobID(), true);
	}

	private void testPutStreamSuccessfulGet(
			JobID jobId1, JobID jobId2, boolean highAvailability) throws IOException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			byte[] data2 = Arrays.copyOfRange(data, 10, 54);

			// put data for jobId1 and verify
			BlobKey key1a = put(server, jobId1, new ByteArrayInputStream(data), highAvailability);
			assertNotNull(key1a);

			BlobKey key1b = put(server, jobId1, new ByteArrayInputStream(data2), highAvailability);
			assertNotNull(key1b);

			verifyContents(server, jobId1, key1a, data, highAvailability);
			verifyContents(server, jobId1, key1b, data2, highAvailability);

			// now put data for jobId2 and verify that both are ok
			BlobKey key2a = put(server, jobId2, new ByteArrayInputStream(data), highAvailability);
			assertNotNull(key2a);
			assertEquals(key1a, key2a);

			BlobKey key2b = put(server, jobId2, new ByteArrayInputStream(data2), highAvailability);
			assertNotNull(key2b);
			assertEquals(key1b, key2b);

			// verify the accessibility and the BLOB contents
			verifyContents(server, jobId1, key1a, data, highAvailability);
			verifyContents(server, jobId1, key1b, data2, highAvailability);
			verifyContents(server, jobId2, key2a, data, highAvailability);
			verifyContents(server, jobId2, key2b, data2, highAvailability);
		}
	}

	// --------------------------------------------------------------------------------------------

	@Test
	public void testPutChunkedStreamSuccessfulGet1() throws IOException {
		testPutChunkedStreamSuccessfulGet(null, null, false);
	}

	@Test
	public void testPutChunkedStreamSuccessfulGet2() throws IOException {
		testPutChunkedStreamSuccessfulGet(null, new JobID(), false);
	}

	@Test
	public void testPutChunkedStreamSuccessfulGet3() throws IOException {
		testPutChunkedStreamSuccessfulGet(new JobID(), new JobID(), false);
	}

	@Test
	public void testPutChunkedStreamSuccessfulGet4() throws IOException {
		testPutChunkedStreamSuccessfulGet(new JobID(), null, false);
	}

	@Test
	public void testPutChunkedStreamSuccessfulGetHa() throws IOException {
		testPutChunkedStreamSuccessfulGet(new JobID(), new JobID(), true);
	}

	private void testPutChunkedStreamSuccessfulGet(
			JobID jobId1, JobID jobId2, boolean highAvailability) throws IOException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			byte[] data2 = Arrays.copyOfRange(data, 10, 54);

			// put data for jobId1 and verify
			BlobKey key1a = put(server, jobId1, new ChunkedInputStream(data, 19), highAvailability);
			assertNotNull(key1a);

			BlobKey key1b = put(server, jobId1, new ChunkedInputStream(data2, 19), highAvailability);
			assertNotNull(key1b);

			verifyContents(server, jobId1, key1a, data, highAvailability);
			verifyContents(server, jobId1, key1b, data2, highAvailability);

			// now put data for jobId2 and verify that both are ok
			BlobKey key2a = put(server, jobId2, new ChunkedInputStream(data, 19), highAvailability);
			assertNotNull(key2a);
			assertEquals(key1a, key2a);

			BlobKey key2b = put(server, jobId2, new ChunkedInputStream(data2, 19), highAvailability);
			assertNotNull(key2b);
			assertEquals(key1b, key2b);

			// verify the accessibility and the BLOB contents
			verifyContents(server, jobId1, key1a, data, highAvailability);
			verifyContents(server, jobId1, key1b, data2, highAvailability);
			verifyContents(server, jobId2, key2a, data, highAvailability);
			verifyContents(server, jobId2, key2b, data2, highAvailability);
		}
	}

	// --------------------------------------------------------------------------------------------

	@Test
	public void testPutBufferFailsNoJob() throws IOException {
		testPutBufferFails(null, false);
	}

	@Test
	public void testPutBufferFailsForJob() throws IOException {
		testPutBufferFails(new JobID(), false);
	}

	@Test
	public void testPutBufferFailsForJobHa() throws IOException {
		testPutBufferFails(new JobID(), true);
	}

	private void testPutBufferFails(final JobID jobId, boolean highAvailabibility) throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		File tempFileDir = null;
		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			// make sure the blob server cannot create any files in its storage dir
			tempFileDir = server.createTemporaryFilename().getParentFile().getParentFile();
			assertTrue(tempFileDir.setExecutable(true, false));
			assertTrue(tempFileDir.setReadable(true, false));
			assertTrue(tempFileDir.setWritable(false, false));

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// test on the server directly
			exception.expect(IOException.class);
			exception.expectMessage("Cannot create directory ");

			put(server, jobId, data, highAvailabibility);

		} finally {
			// set writable again to make sure we can remove the directory
			if (tempFileDir != null) {
				//noinspection ResultOfMethodCallIgnored
				tempFileDir.setWritable(true, false);
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
		testConcurrentPutOperations(null, false);
	}

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent put operations will only upload the file once to the {@link BlobStore}.
	 */
	@Test
	public void testConcurrentPutOperationsForJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentPutOperations(new JobID(), false);
	}

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent put operations will only upload the file once to the {@link BlobStore}.
	 */
	@Test
	public void testConcurrentPutOperationsForJobHa() throws IOException, ExecutionException, InterruptedException {
		testConcurrentPutOperations(new JobID(), true);
	}

	private void testConcurrentPutOperations(final JobID jobId, final boolean highAvailabibility)
			throws IOException, InterruptedException, ExecutionException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		BlobStore blobStore = mock(BlobStore.class);
		int concurrentPutOperations = 2;
		int dataSize = 1024;

		final CountDownLatch countDownLatch = new CountDownLatch(concurrentPutOperations);
		final byte[] data = new byte[dataSize];

		ArrayList<Future<BlobKey>> allFutures = new ArrayList<>(concurrentPutOperations);

		ExecutorService executor = Executors.newFixedThreadPool(concurrentPutOperations);

		try (
			final BlobServer blobServer = new BlobServer(config, blobStore)) {

			for (int i = 0; i < concurrentPutOperations; i++) {
				Future<BlobKey> putFuture = FlinkCompletableFuture
					.supplyAsync(new Callable<BlobKey>() {
					@Override
					public BlobKey call() throws Exception {
						BlockingInputStream inputStream =
							new BlockingInputStream(countDownLatch, data);
						return put(blobServer, jobId, inputStream, highAvailabibility);
					}
				}, executor);

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

			// check the uploaded file's contents
			verifyContents(blobServer, jobId, blobKey, data, highAvailabibility);

			// check that we only uploaded the file once to the blob store
			if (highAvailabibility) {
				verify(blobStore, times(1)).put(any(File.class), eq(jobId), eq(blobKey));
			} else {
				// can't really verify much in the other cases other than that the put operations should
				// work and not corrupt files
				verify(blobStore, times(0)).put(any(File.class), eq(jobId), eq(blobKey));
			}
		} finally {
			executor.shutdownNow();
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Helper to chose the right {@link BlobServer#put} method.
	 *
	 * @return blob key for the uploaded data
	 */
	static BlobKey put(BlobServer server, JobID jobId, InputStream data, boolean highAvailabibility)
		throws IOException {
		if (highAvailabibility) {
			return server.putHA(jobId, data);
		} else if (jobId == null) {
			return server.put(data);
		} else {
			return server.put(jobId, data);
		}
	}

	/**
	 * Helper to chose the right {@link BlobServer#put} method.
	 *
	 * @return blob key for the uploaded data
	 */
	static BlobKey put(BlobServer server, JobID jobId, byte[] data, boolean highAvailabibility)
		throws IOException {
		if (highAvailabibility) {
			return server.putHA(jobId, data);
		} else if (jobId == null) {
			return server.put(data);
		} else {
			return server.put(jobId, data);
		}
	}

	/**
	 * GET the data stored at the two keys and check that it is equal to <tt>data</tt>.
	 *
	 * @param blobService
	 * 		BlobServer to use
	 * @param jobId
	 * 		job ID or <tt>null</tt> if job-unrelated
	 * @param key
	 * 		blob key
	 * @param data
	 * 		expected data
	 * @param highAvailability
	 * 		whether to use HA mode accessors
	 */
	static void verifyContents(
		BlobService blobService, JobID jobId, BlobKey key, byte[] data, boolean highAvailability)
		throws IOException {

		try (InputStream is = new FileInputStream(get(blobService, jobId, key, highAvailability))) {
			BlobClientTest.validateGet(is, data);
		}
	}

	/**
	 * GET the data stored at the two keys and check that it is equal to <tt>data</tt>.
	 *
	 * @param blobService
	 * 		BlobServer to use
	 * @param jobId
	 * 		job ID or <tt>null</tt> if job-unrelated
	 * @param key
	 * 		blob key
	 * @param data
	 * 		expected data
	 * @param highAvailability
	 * 		whether to use HA mode accessors
	 */
	static void verifyContents(
		BlobService blobService, JobID jobId, BlobKey key, InputStream data, boolean highAvailability)
		throws IOException {

		try (InputStream is = new FileInputStream(get(blobService, jobId, key, highAvailability))) {
			BlobClientTest.validateGet(is, data);
		}
	}

	// --------------------------------------------------------------------------------------------

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
