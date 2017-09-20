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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.AccessDeniedException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobClientTest.validateGetAndClose;
import static org.apache.flink.runtime.blob.BlobServerGetTest.get;
import static org.apache.flink.runtime.blob.BlobServerGetTest.verifyDeleted;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobUtils.JOB_DIR_PREFIX;
import static org.apache.flink.runtime.blob.BlobUtils.NO_JOB_DIR_PREFIX;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests how failing GET requests behave in the presence of failures when used with a {@link
 * BlobCache}.
 *
 * <p>Successful GET requests are tested in conjunction wit the PUT requests.
 */
public class BlobCacheGetTest extends TestLogger {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void testGetFailsDuringLookup1() throws IOException {
		testGetFailsDuringLookup(null, new JobID(), TRANSIENT_BLOB);
	}

	@Test
	public void testGetFailsDuringLookup2() throws IOException {
		testGetFailsDuringLookup(new JobID(), new JobID(), TRANSIENT_BLOB);
	}

	@Test
	public void testGetFailsDuringLookup3() throws IOException {
		testGetFailsDuringLookup(new JobID(), null, TRANSIENT_BLOB);
	}

	@Test
	public void testGetFailsDuringLookupHa() throws IOException {
		testGetFailsDuringLookup(new JobID(), new JobID(), PERMANENT_BLOB);
	}

	/**
	 * Checks the correct result if a GET operation fails during the lookup of the file.
	 *
	 * @param jobId1 first job ID or <tt>null</tt> if job-unrelated
	 * @param jobId2 second job ID different to <tt>jobId1</tt>
	 * @param blobType
	 * 		whether the BLOB should become permanent or transient
	 */
	private void testGetFailsDuringLookup(final JobID jobId1, final JobID jobId2, BlobType blobType)
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

			// put content addressable (like libraries)
			BlobKey key = put(server, jobId1, data, blobType);
			assertNotNull(key);

			// delete file to make sure that GET requests fail
			File blobFile = server.getStorageLocation(jobId1, key);
			assertTrue(blobFile.delete());

			// issue a GET request that fails
			verifyDeleted(cache, jobId1, key);

			// add the same data under a second jobId
			BlobKey key2 = put(server, jobId2, data, blobType);
			assertNotNull(key);
			assertEquals(key, key2);

			// request for jobId2 should succeed
			get(cache, jobId2, key);
			// request for jobId1 should still fail
			verifyDeleted(cache, jobId1, key);

			// delete on cache, try to retrieve again
			if (blobType == PERMANENT_BLOB) {
				blobFile = cache.getPermanentBlobService().getStorageLocation(jobId2, key);
			} else {
				blobFile = cache.getTransientBlobService().getStorageLocation(jobId2, key);
			}
			assertTrue(blobFile.delete());
			get(cache, jobId2, key);

			// delete on cache and server, verify that it is not accessible anymore
			if (blobType == PERMANENT_BLOB) {
				blobFile = cache.getPermanentBlobService().getStorageLocation(jobId2, key);
			} else {
				blobFile = cache.getTransientBlobService().getStorageLocation(jobId2, key);
			}
			assertTrue(blobFile.delete());
			blobFile = server.getStorageLocation(jobId2, key);
			assertTrue(blobFile.delete());
			verifyDeleted(cache, jobId2, key);
		}
	}

	@Test
	public void testGetFailsIncomingNoJob() throws IOException {
		testGetFailsIncoming(null, TRANSIENT_BLOB);
	}

	@Test
	public void testGetFailsIncomingForJob() throws IOException {
		testGetFailsIncoming(new JobID(), TRANSIENT_BLOB);
	}

	@Test
	public void testGetFailsIncomingForJobHa() throws IOException {
		testGetFailsIncoming(new JobID(), PERMANENT_BLOB);
	}

	/**
	 * Retrieves a BLOB via a {@link BlobCache} which cannot create incoming files. File transfers
	 * should fail.
	 *
	 * @param jobId
	 * 		job id
	 * @param blobType
	 * 		whether the BLOB should become permanent or transient
	 */
	private void testGetFailsIncoming(@Nullable final JobID jobId, BlobType blobType)
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

			// store the data on the server
			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			BlobKey blobKey = put(server, jobId, data, blobType);

			// make sure the blob cache cannot create any files in its storage dir
			if (blobType == PERMANENT_BLOB) {
				tempFileDir =
					cache.getPermanentBlobService().createTemporaryFilename().getParentFile();
			} else {
				tempFileDir =
					cache.getTransientBlobService().createTemporaryFilename().getParentFile();
			}
			assertTrue(tempFileDir.setExecutable(true, false));
			assertTrue(tempFileDir.setReadable(true, false));
			assertTrue(tempFileDir.setWritable(false, false));

			// request the file from the server via the cache
			exception.expect(IOException.class);
			exception.expectMessage("Failed to fetch BLOB ");

			try {
				get(cache, jobId, blobKey);
			} finally {
				HashSet<String> expectedDirs = new HashSet<>();
				expectedDirs.add("incoming");
				if (jobId != null) {
					// only the incoming and job directory should exist (no job directory!)
					expectedDirs.add(JOB_DIR_PREFIX + jobId);
					File storageDir = tempFileDir.getParentFile();
					String[] actualDirs = storageDir.list();
					assertNotNull(actualDirs);
					assertEquals(expectedDirs, new HashSet<>(Arrays.asList(actualDirs)));

					// job directory should be empty
					File jobDir = new File(tempFileDir.getParentFile(), JOB_DIR_PREFIX + jobId);
					assertArrayEquals(new String[] {}, jobDir.list());
				} else {
					// only the incoming and no_job directory should exist (no job directory!)
					expectedDirs.add(NO_JOB_DIR_PREFIX);
					File storageDir = tempFileDir.getParentFile();
					String[] actualDirs = storageDir.list();
					assertNotNull(actualDirs);
					assertEquals(expectedDirs, new HashSet<>(Arrays.asList(actualDirs)));

					// no_job directory should be empty
					File noJobDir = new File(tempFileDir.getParentFile(), NO_JOB_DIR_PREFIX);
					assertArrayEquals(new String[] {}, noJobDir.list());
				}
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
	public void testGetFailsStoreNoJob() throws IOException {
		testGetFailsStore(null, TRANSIENT_BLOB);
	}

	@Test
	public void testGetFailsStoreForJob() throws IOException {
		testGetFailsStore(new JobID(), TRANSIENT_BLOB);
	}

	@Test
	public void testGetFailsStoreForJobHa() throws IOException {
		testGetFailsStore(new JobID(), PERMANENT_BLOB);
	}

	/**
	 * Retrieves a BLOB via a {@link BlobCache} which cannot create the final storage file. File
	 * transfers should fail.
	 *
	 * @param jobId
	 * 		job id
	 * @param blobType
	 * 		whether the BLOB should become permanent or transient
	 */
	private void testGetFailsStore(@Nullable final JobID jobId, BlobType blobType)
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

			// store the data on the server
			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			BlobKey blobKey = put(server, jobId, data, blobType);

			// make sure the blob cache cannot create any files in its storage dir
			if (blobType == PERMANENT_BLOB) {
				jobStoreDir = cache.getPermanentBlobService()
					.getStorageLocation(jobId, new BlobKey(PERMANENT_BLOB))
					.getParentFile();
			} else {
				jobStoreDir = cache.getTransientBlobService()
					.getStorageLocation(jobId, new BlobKey(TRANSIENT_BLOB))
					.getParentFile();
			}
			assertTrue(jobStoreDir.setExecutable(true, false));
			assertTrue(jobStoreDir.setReadable(true, false));
			assertTrue(jobStoreDir.setWritable(false, false));

			// request the file from the server via the cache
			exception.expect(AccessDeniedException.class);

			try {
				get(cache, jobId, blobKey);
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

	/**
	 * Retrieves a BLOB from the HA store to a {@link BlobServer} whose HA store does not contain
	 * the file. File transfers should fail.
	 */
	@Test
	public void testGetFailsHaStoreForJobHa() throws IOException {
		final JobID jobId = new JobID();

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (
			BlobServer server = new BlobServer(config, new VoidBlobStore());
			BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, new VoidBlobStore())) {

			server.start();

			// store the data on the server (and blobStore), remove from local server store
			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			BlobKey blobKey = put(server, jobId, data, PERMANENT_BLOB);
			assertTrue(server.getStorageLocation(jobId, blobKey).delete());

			File tempFileDir = server.createTemporaryFilename().getParentFile();

			// request the file from the server via the cache
			exception.expect(IOException.class);
			exception.expectMessage("Failed to fetch BLOB ");

			try {
				get(cache, jobId, blobKey);
			} finally {
				HashSet<String> expectedDirs = new HashSet<>();
				expectedDirs.add("incoming");
				expectedDirs.add(JOB_DIR_PREFIX + jobId);
				// only the incoming and job directory should exist (no job directory!)
				File storageDir = tempFileDir.getParentFile();
				String[] actualDirs = storageDir.list();
				assertNotNull(actualDirs);
				assertEquals(expectedDirs, new HashSet<>(Arrays.asList(actualDirs)));

				// job directory should be empty
				File jobDir = new File(tempFileDir.getParentFile(), JOB_DIR_PREFIX + jobId);
				assertArrayEquals(new String[] {}, jobDir.list());
			}
		}
	}

	/**
	 * FLINK-6020
	 *
	 * <p>Tests that concurrent get operations don't concurrently access the BlobStore to download a blob.
	 */
	@Test
	public void testConcurrentGetOperationsNoJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentGetOperations(null, TRANSIENT_BLOB, false);
	}

	@Test
	public void testConcurrentGetOperationsForJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentGetOperations(new JobID(), TRANSIENT_BLOB, false);
	}

	@Test
	public void testConcurrentGetOperationsForJobHa() throws IOException, ExecutionException, InterruptedException {
		testConcurrentGetOperations(new JobID(), PERMANENT_BLOB, false);
	}

	@Test
	public void testConcurrentGetOperationsForJobHa2() throws IOException, ExecutionException, InterruptedException {
		testConcurrentGetOperations(new JobID(), PERMANENT_BLOB, true);
	}

	/**
	 * [FLINK-6020] Tests that concurrent get operations don't concurrently access the BlobStore to
	 * download a blob.
	 *
	 * @param jobId
	 * 		job ID to use (or <tt>null</tt> if job-unrelated)
	 * @param blobType
	 * 		whether the BLOB should become permanent or transient
	 * @param cacheAccessesHAStore
	 * 		whether the cache has access to the {@link BlobServer}'s HA store or not
	 */
	private void testConcurrentGetOperations(final JobID jobId, final BlobType blobType,
			final boolean cacheAccessesHAStore)
			throws IOException, InterruptedException, ExecutionException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		final BlobStore blobStoreServer = mock(BlobStore.class);
		final BlobStore blobStoreCache = mock(BlobStore.class);

		final int numberConcurrentGetOperations = 3;
		final List<CompletableFuture<File>> getOperations = new ArrayList<>(numberConcurrentGetOperations);

		final byte[] data = {1, 2, 3, 4, 99, 42};

		MessageDigest md = BlobUtils.createMessageDigest();

		// create the correct blob key by hashing our input data
		final BlobKey blobKey = new BlobKey(blobType, md.digest(data));

		final ExecutorService executor = Executors.newFixedThreadPool(numberConcurrentGetOperations);

		try (
			final BlobServer server = new BlobServer(config, blobStoreServer);
			final BlobCache cache = new BlobCache(new InetSocketAddress("localhost", server.getPort()),
				config, cacheAccessesHAStore ? blobStoreServer : blobStoreCache)) {

			server.start();

			// upload data first
			assertEquals(blobKey, put(server, jobId, data, blobType));

			// now try accessing it concurrently (only HA mode will be able to retrieve it from HA store!)
			for (int i = 0; i < numberConcurrentGetOperations; i++) {
				CompletableFuture<File> getOperation = CompletableFuture
					.supplyAsync(
						() -> {
							try {
								File file = get(cache, jobId, blobKey);
								// check that we have read the right data
								validateGetAndClose(new FileInputStream(file), data);
								return file;
							} catch (IOException e) {
								throw new CompletionException(new FlinkException(
									"Could not upload blob.", e));
							}
						}, executor);

				getOperations.add(getOperation);
			}

			FutureUtils.ConjunctFuture<Collection<File>> filesFuture = FutureUtils.combineAll(getOperations);

			// wait until all operations have completed and check that no exception was thrown
			filesFuture.get();
		} finally {
			executor.shutdownNow();
		}
	}
}
