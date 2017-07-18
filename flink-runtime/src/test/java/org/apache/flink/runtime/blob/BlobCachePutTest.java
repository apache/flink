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

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobServerPutTest.BlockingInputStream;
import static org.apache.flink.runtime.blob.BlobServerPutTest.ChunkedInputStream;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.junit.Assert.assertArrayEquals;
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
public class BlobCachePutTest extends TestLogger {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	// --- concurrency tests for utility methods which could fail during the put operation ---

	/**
	 * Checked thread that calls {@link TransientBlobCache#getStorageLocation(JobID, BlobKey)}.
	 */
	public static class TransientBlobCacheGetStorageLocation extends CheckedThread {
		private final TransientBlobCache cache;
		private final JobID jobId;
		private final BlobKey key;

		TransientBlobCacheGetStorageLocation(
				TransientBlobCache cache, @Nullable JobID jobId, BlobKey key) {
			this.cache = cache;
			this.jobId = jobId;
			this.key = key;
		}

		@Override
		public void go() throws Exception {
			cache.getStorageLocation(jobId, key);
		}
	}

	/**
	 * Checked thread that calls {@link PermanentBlobCache#getStorageLocation(JobID, BlobKey)}.
	 */
	public static class PermanentBlobCacheGetStorageLocation extends CheckedThread {
		private final PermanentBlobCache cache;
		private final JobID jobId;
		private final BlobKey key;

		PermanentBlobCacheGetStorageLocation(PermanentBlobCache cache, JobID jobId, BlobKey key) {
			this.cache = cache;
			this.jobId = jobId;
			this.key = key;
		}

		@Override
		public void go() throws Exception {
			cache.getStorageLocation(jobId, key);
		}
	}

	/**
	 * Tests concurrent calls to {@link TransientBlobCache#getStorageLocation(JobID, BlobKey)}.
	 */
	@Test
	public void testTransientBlobCacheGetStorageLocationConcurrentNoJob() throws Exception {
		testTransientBlobCacheGetStorageLocationConcurrent(null);
	}

	/**
	 * Tests concurrent calls to {@link TransientBlobCache#getStorageLocation(JobID, BlobKey)}.
	 */
	@Test
	public void testTransientBlobCacheGetStorageLocationConcurrentForJob() throws Exception {
		testTransientBlobCacheGetStorageLocationConcurrent(new JobID());
	}

	private void testTransientBlobCacheGetStorageLocationConcurrent(
			@Nullable final JobID jobId) throws Exception {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (BlobServer server = new BlobServer(config, new VoidBlobStore());
			final TransientBlobCache cache = new TransientBlobCache(
				new InetSocketAddress("localhost", server.getPort()), config)) {

			server.start();

			BlobKey key = new BlobKey();
			CheckedThread[] threads = new CheckedThread[] {
				new TransientBlobCacheGetStorageLocation(cache, jobId, key),
				new TransientBlobCacheGetStorageLocation(cache, jobId, key),
				new TransientBlobCacheGetStorageLocation(cache, jobId, key)
			};
			checkedThreadSimpleTest(threads);
		}
	}

	/**
	 * Tests concurrent calls to {@link PermanentBlobCache#getStorageLocation(JobID, BlobKey)}.
	 */
	@Test
	public void testPermanentBlobCacheGetStorageLocationConcurrentForJob() throws Exception {
		final JobID jobId = new JobID();
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (BlobServer server = new BlobServer(config, new VoidBlobStore());
			final PermanentBlobCache cache = new PermanentBlobCache(
				new InetSocketAddress("localhost", server.getPort()), config,
				new VoidBlobStore())) {

			server.start();

			BlobKey key = new BlobKey();
			CheckedThread[] threads = new CheckedThread[] {
				new PermanentBlobCacheGetStorageLocation(cache, jobId, key),
				new PermanentBlobCacheGetStorageLocation(cache, jobId, key),
				new PermanentBlobCacheGetStorageLocation(cache, jobId, key)
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

	/**
	 * Uploads two byte arrays for different jobs into the server via the {@link BlobCache}. File
	 * transfers should be successful.
	 *
	 * @param jobId1
	 * 		first job id
	 * @param jobId2
	 * 		second job id
	 * @param highAvailability
	 * 		whether to upload a permanent blob (<tt>true</tt>, via {@link
	 * 		BlobClient#uploadJarFiles(InetSocketAddress, Configuration, JobID, List)}) or not
	 * 		(<tt>false</tt>, via {@link TransientBlobCache#put(JobID, byte[])}
	 */
	private void testPutBufferSuccessfulGet(
			@Nullable JobID jobId1, @Nullable JobID jobId2, boolean highAvailability)
			throws IOException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			server.start();

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			byte[] data2 = Arrays.copyOfRange(data, 10, 54);

			// put data for jobId1 and verify
			BlobKey key1a = put(cache, jobId1, data, highAvailability);
			assertNotNull(key1a);

			BlobKey key1b = put(cache, jobId1, data2, highAvailability);
			assertNotNull(key1b);

			// files should be available on the server
			verifyContents(server, jobId1, key1a, data, highAvailability);
			verifyContents(server, jobId1, key1b, data2, highAvailability);

			// now put data for jobId2 and verify that both are ok
			BlobKey key2a = put(cache, jobId2, data, highAvailability);
			assertNotNull(key2a);
			assertEquals(key1a, key2a);

			BlobKey key2b = put(cache, jobId2, data2, highAvailability);
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
		testPutStreamSuccessfulGet(null, null);
	}

	@Test
	public void testPutStreamSuccessfulGet2() throws IOException {
		testPutStreamSuccessfulGet(null, new JobID());
	}

	@Test
	public void testPutStreamSuccessfulGet3() throws IOException {
		testPutStreamSuccessfulGet(new JobID(), new JobID());
	}

	@Test
	public void testPutStreamSuccessfulGet4() throws IOException {
		testPutStreamSuccessfulGet(new JobID(), null);
	}

	/**
	 * Uploads two file streams for different jobs into the server via the {@link BlobCache}. File
	 * transfers should be successful.
	 * <p>
	 * Note that high-availability uploads of streams is currently only possible at the {@link
	 * BlobServer}.
	 *
	 * @param jobId1
	 * 		first job id
	 * @param jobId2
	 * 		second job id
	 */
	private void testPutStreamSuccessfulGet(@Nullable JobID jobId1, @Nullable JobID jobId2)
			throws IOException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			server.start();

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			byte[] data2 = Arrays.copyOfRange(data, 10, 54);

			// put data for jobId1 and verify
			BlobKey key1a = put(cache, jobId1, new ByteArrayInputStream(data), false);
			assertNotNull(key1a);

			BlobKey key1b = put(cache, jobId1, new ByteArrayInputStream(data2), false);
			assertNotNull(key1b);

			// files should be available on the server
			verifyContents(server, jobId1, key1a, data, false);
			verifyContents(server, jobId1, key1b, data2, false);

			// now put data for jobId2 and verify that both are ok
			BlobKey key2a = put(cache, jobId2, new ByteArrayInputStream(data), false);
			assertNotNull(key2a);
			assertEquals(key1a, key2a);

			BlobKey key2b = put(cache, jobId2, new ByteArrayInputStream(data2), false);
			assertNotNull(key2b);
			assertEquals(key1b, key2b);

			// verify the accessibility and the BLOB contents
			verifyContents(server, jobId1, key1a, data, false);
			verifyContents(server, jobId1, key1b, data2, false);
			verifyContents(server, jobId2, key2a, data, false);
			verifyContents(server, jobId2, key2b, data2, false);
		}
	}

	// --------------------------------------------------------------------------------------------

	@Test
	public void testPutChunkedStreamSuccessfulGet1() throws IOException {
		testPutChunkedStreamSuccessfulGet(null, null);
	}

	@Test
	public void testPutChunkedStreamSuccessfulGet2() throws IOException {
		testPutChunkedStreamSuccessfulGet(null, new JobID());
	}

	@Test
	public void testPutChunkedStreamSuccessfulGet3() throws IOException {
		testPutChunkedStreamSuccessfulGet(new JobID(), new JobID());
	}

	@Test
	public void testPutChunkedStreamSuccessfulGet4() throws IOException {
		testPutChunkedStreamSuccessfulGet(new JobID(), null);
	}

	/**
	 * Uploads two chunked file streams for different jobs into the server via the {@link
	 * BlobCache}. File transfers should be successful.
	 * <p>
	 * Note that high-availability uploads of streams is currently only possible at the {@link
	 * BlobServer}.
	 *
	 * @param jobId1
	 * 		first job id
	 * @param jobId2
	 * 		second job id
	 */
	private void testPutChunkedStreamSuccessfulGet(
			@Nullable JobID jobId1, @Nullable JobID jobId2) throws IOException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			server.start();

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			byte[] data2 = Arrays.copyOfRange(data, 10, 54);

			// put data for jobId1 and verify
			BlobKey key1a = put(cache, jobId1, new ChunkedInputStream(data, 19), false);
			assertNotNull(key1a);

			BlobKey key1b = put(cache, jobId1, new ChunkedInputStream(data2, 19), false);
			assertNotNull(key1b);

			// files should be available on the server
			verifyContents(server, jobId1, key1a, data, false);
			verifyContents(server, jobId1, key1b, data2, false);

			// now put data for jobId2 and verify that both are ok
			BlobKey key2a = put(cache, jobId2, new ChunkedInputStream(data, 19), false);
			assertNotNull(key2a);
			assertEquals(key1a, key2a);

			BlobKey key2b = put(cache, jobId2, new ChunkedInputStream(data2, 19), false);
			assertNotNull(key2b);
			assertEquals(key1b, key2b);

			// verify the accessibility and the BLOB contents
			verifyContents(server, jobId1, key1a, data, false);
			verifyContents(server, jobId1, key1b, data2, false);
			verifyContents(server, jobId2, key2a, data, false);
			verifyContents(server, jobId2, key2b, data2, false);
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

	/**
	 * Uploads a byte array to a server which cannot create any files via the {@link BlobCache}.
	 * File transfers should fail.
	 *
	 * @param jobId
	 * 		job id
	 * @param highAvailability
	 * 		whether to upload a permanent blob (<tt>true</tt>, via {@link
	 * 		BlobClient#uploadJarFiles(InetSocketAddress, Configuration, JobID, List)}) or not
	 * 		(<tt>false</tt>, via {@link TransientBlobCache#put(JobID, byte[])}
	 */
	private void testPutBufferFails(@Nullable final JobID jobId, boolean highAvailability)
			throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		File tempFileDir = null;
		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			server.start();

			// make sure the blob server cannot create any files in its storage dir
			tempFileDir = server.createTemporaryFilename().getParentFile().getParentFile();
			assertTrue(tempFileDir.setExecutable(true, false));
			assertTrue(tempFileDir.setReadable(true, false));
			assertTrue(tempFileDir.setWritable(false, false));

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// upload the file to the server via the cache
			exception.expect(IOException.class);
			exception.expectMessage("PUT operation failed: ");

			put(cache, jobId, data, highAvailability);

		} finally {
			// set writable again to make sure we can remove the directory
			if (tempFileDir != null) {
				//noinspection ResultOfMethodCallIgnored
				tempFileDir.setWritable(true, false);
			}
		}
	}

	@Test
	public void testPutBufferFailsIncomingNoJob() throws IOException {
		testPutBufferFailsIncoming(null, false);
	}

	@Test
	public void testPutBufferFailsIncomingForJob() throws IOException {
		testPutBufferFailsIncoming(new JobID(), false);
	}

	@Test
	public void testPutBufferFailsIncomingForJobHa() throws IOException {
		testPutBufferFailsIncoming(new JobID(), true);
	}

	/**
	 * Uploads a byte array to a server which cannot create incoming files via the {@link
	 * BlobCache}. File transfers should fail.
	 *
	 * @param jobId
	 * 		job id
	 * @param highAvailability
	 * 		whether to upload a permanent blob (<tt>true</tt>, via {@link
	 * 		BlobClient#uploadJarFiles(InetSocketAddress, Configuration, JobID, List)}) or not
	 * 		(<tt>false</tt>, via {@link TransientBlobCache#put(JobID, byte[])}
	 */
	private void testPutBufferFailsIncoming(@Nullable final JobID jobId, boolean highAvailability)
			throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		File tempFileDir = null;
		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			server.start();

			// make sure the blob server cannot create any files in its storage dir
			tempFileDir = server.createTemporaryFilename().getParentFile();
			assertTrue(tempFileDir.setExecutable(true, false));
			assertTrue(tempFileDir.setReadable(true, false));
			assertTrue(tempFileDir.setWritable(false, false));

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// upload the file to the server via the cache
			exception.expect(IOException.class);
			exception.expectMessage("PUT operation failed: ");

			try {
				put(cache, jobId, data, highAvailability);
			} finally {
				File storageDir = tempFileDir.getParentFile();
				// only the incoming directory should exist (no job directory!)
				assertArrayEquals(new String[] {"incoming"}, storageDir.list());
			}
		} finally {
			// set writable again to make sure we can remove the directory
			if (tempFileDir != null) {
				//noinspection ResultOfMethodCallIgnored
				tempFileDir.setWritable(true, false);
			}
		}
	}

	@Test
	public void testPutBufferFailsStoreNoJob() throws IOException {
		testPutBufferFailsStore(null, false);
	}

	@Test
	public void testPutBufferFailsStoreForJob() throws IOException {
		testPutBufferFailsStore(new JobID(), false);
	}

	@Test
	public void testPutBufferFailsStoreForJobHa() throws IOException {
		testPutBufferFailsStore(new JobID(), true);
	}

	/**
	 * Uploads a byte array to a server which cannot create files via the {@link BlobCache}. File
	 * transfers should fail.
	 *
	 * @param jobId
	 * 		job id
	 * @param highAvailability
	 * 		whether to upload a permanent blob (<tt>true</tt>, via {@link
	 * 		BlobClient#uploadJarFiles(InetSocketAddress, Configuration, JobID, List)}) or not
	 * 		(<tt>false</tt>, via {@link TransientBlobCache#put(JobID, byte[])}
	 */
	private void testPutBufferFailsStore(@Nullable final JobID jobId, boolean highAvailability)
			throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		File jobStoreDir = null;
		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			server.start();

			// make sure the blob server cannot create any files in its storage dir
			jobStoreDir = server.getStorageLocation(jobId, new BlobKey()).getParentFile();
			assertTrue(jobStoreDir.setExecutable(true, false));
			assertTrue(jobStoreDir.setReadable(true, false));
			assertTrue(jobStoreDir.setWritable(false, false));

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// upload the file to the server via the cache
			exception.expect(IOException.class);
			exception.expectMessage("PUT operation failed: ");

			try {
				put(cache, jobId, data, highAvailability);
			} finally {
				// there should be no remaining incoming files
				File incomingFileDir = new File(jobStoreDir.getParent(), "incoming");
				assertArrayEquals(new String[] {}, incomingFileDir.list());

				// there should be no files in the job directory
				assertArrayEquals(new String[] {}, jobStoreDir.list());
			}
		} finally {
			// set writable again to make sure we can remove the directory
			if (jobStoreDir != null) {
				//noinspection ResultOfMethodCallIgnored
				jobStoreDir.setWritable(true, false);
			}
		}
	}

	@Test
	public void testConcurrentPutOperationsNoJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentPutOperations(null, false);
	}

	@Test
	public void testConcurrentPutOperationsForJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentPutOperations(new JobID(), false);
	}

	@Test
	public void testConcurrentPutOperationsForJobHa() throws IOException, ExecutionException, InterruptedException {
		testConcurrentPutOperations(new JobID(), true);
	}

	/**
	 * [FLINK-6020]
	 * Tests that concurrent put operations will only upload the file once to the {@link BlobStore}
	 * and that the files are not corrupt at any time.
	 *
	 * @param jobId
	 * 		job ID to use (or <tt>null</tt> if job-unrelated)
	 * @param highAvailability
	 * 		whether to use permanent (<tt>true</tt>) or transient BLOBs (<tt>false</tt>)
	 */
	private void testConcurrentPutOperations(
			@Nullable final JobID jobId, final boolean highAvailability)
			throws IOException, InterruptedException, ExecutionException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());


		final BlobStore blobStoreServer = mock(BlobStore.class);
		final BlobStore blobStoreCache = mock(BlobStore.class);

		int concurrentPutOperations = 2;
		int dataSize = 1024;

		final CountDownLatch countDownLatch = new CountDownLatch(concurrentPutOperations);
		final byte[] data = new byte[dataSize];

		final List<Path> jars;
		if (highAvailability) {
			// implement via JAR file upload instead:
			File tmpFile = temporaryFolder.newFile();
			FileUtils.writeByteArrayToFile(tmpFile, data);
			jars = Collections.singletonList(new Path(tmpFile.getAbsolutePath()));
		} else {
			jars = null;
		}

		ArrayList<Future<BlobKey>> allFutures = new ArrayList<>(concurrentPutOperations);

		ExecutorService executor = Executors.newFixedThreadPool(concurrentPutOperations);

		try (
			final BlobServer server = new BlobServer(config, blobStoreServer);
			final BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, blobStoreCache)) {

			server.start();

			// for highAvailability
			final InetSocketAddress serverAddress =
				new InetSocketAddress("localhost", server.getPort());
			// uploading HA BLOBs works on BlobServer only (and, for now, via the BlobClient)

			for (int i = 0; i < concurrentPutOperations; i++) {
				final Callable<BlobKey> callable;
				if (highAvailability) {
					// cannot use a blocking stream here (upload only possible via files)
					callable = new Callable<BlobKey>() {
						@Override
						public BlobKey call() throws Exception {
							List<BlobKey> keys =
								BlobClient.uploadJarFiles(serverAddress, config, jobId, jars);
							assertEquals(1, keys.size());
							BlobKey uploadedKey = keys.get(0);
							// check the uploaded file's contents (concurrently)
							verifyContents(server, jobId, uploadedKey, data, true);
							return uploadedKey;
						}
					};

				} else {
					callable = new Callable<BlobKey>() {
						@Override
						public BlobKey call() throws Exception {
							BlockingInputStream inputStream =
								new BlockingInputStream(countDownLatch, data);
							BlobKey uploadedKey = put(cache, jobId, inputStream, false);
							// check the uploaded file's contents (concurrently)
							verifyContents(server, jobId, uploadedKey, data, false);
							return uploadedKey;
						}
					};
				}
				Future<BlobKey> putFuture = FlinkCompletableFuture
					.supplyAsync(callable, executor);

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
			verifyContents(server, jobId, blobKey, data, highAvailability);

			// check that we only uploaded the file once to the blob store
			if (highAvailability) {
				verify(blobStoreServer, times(1)).put(any(File.class), eq(jobId), eq(blobKey));
			} else {
				// can't really verify much in the other cases other than that the put operations should
				// work and not corrupt files
				verify(blobStoreServer, times(0)).put(any(File.class), eq(jobId), eq(blobKey));
			}
			// caches must not access the blob store (they are not allowed to write there)
			verify(blobStoreCache, times(0)).put(any(File.class), eq(jobId), eq(blobKey));
		} finally {
			executor.shutdownNow();
		}
	}
}
