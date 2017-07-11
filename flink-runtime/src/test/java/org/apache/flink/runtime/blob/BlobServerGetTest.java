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
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
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
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobUtils.JOB_DIR_PREFIX;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests how failing GET requests behave in the presence of failures when used with a {@link
 * BlobServer}.
 *
 * <p>Successful GET requests are tested in conjunction wit the PUT requests.
 */
public class BlobServerGetTest extends TestLogger {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void testGetFailsDuringLookup1() throws IOException {
		testGetFailsDuringLookup(null, new JobID(), false);
	}

	@Test
	public void testGetFailsDuringLookup2() throws IOException {
		testGetFailsDuringLookup(new JobID(), new JobID(), false);
	}

	@Test
	public void testGetFailsDuringLookup3() throws IOException {
		testGetFailsDuringLookup(new JobID(), null, false);
	}

	@Test
	public void testGetFailsDuringLookupHa() throws IOException {
		testGetFailsDuringLookup(new JobID(), new JobID(), true);
	}

	/**
	 * Checks the correct result if a GET operation fails during the lookup of the file.
	 *
	 * @param jobId1 first job ID or <tt>null</tt> if job-unrelated
	 * @param jobId2 second job ID different to <tt>jobId1</tt>
	 */
	private void testGetFailsDuringLookup(
			@Nullable final JobID jobId1, @Nullable final JobID jobId2, boolean highAvailability)
			throws IOException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			server.start();

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = put(server, jobId1, data, highAvailability);
			assertNotNull(key);

			// delete file to make sure that GET requests fail
			File blobFile = server.getStorageLocation(jobId1, key);
			assertTrue(blobFile.delete());

			// issue a GET request that fails
			verifyDeleted(server, jobId1, key, highAvailability);

			// add the same data under a second jobId
			BlobKey key2 = put(server, jobId2, data, highAvailability);
			assertNotNull(key);
			assertEquals(key, key2);

			// request for jobId2 should succeed
			get(server, jobId2, key, highAvailability);
			// request for jobId1 should still fail
			verifyDeleted(server, jobId1, key, highAvailability);

			// same checks as for jobId1 but for jobId2 should also work:
			blobFile = server.getStorageLocation(jobId2, key);
			assertTrue(blobFile.delete());
			verifyDeleted(server, jobId2, key, highAvailability);
		}
	}

	/**
	 * Retrieves a BLOB from the HA store to a {@link BlobServer} which cannot create incoming
	 * files. File transfers should fail.
	 */
	@Test
	public void testGetFailsIncomingForJobHa() throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		final JobID jobId = new JobID();

		final Configuration config = new Configuration();
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(CoreOptions.STATE_BACKEND, "FILESYSTEM");
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.newFolder().getPath());

		BlobStoreService blobStore = null;

		try {
			blobStore = BlobUtils.createBlobStoreFromConfig(config);

			File tempFileDir = null;
			try (BlobServer server = new BlobServer(config, blobStore)) {

				server.start();

				// store the data on the server (and blobStore), remove from local store
				byte[] data = new byte[2000000];
				rnd.nextBytes(data);
				BlobKey blobKey = put(server, jobId, data, true);
				assertTrue(server.getStorageLocation(jobId, blobKey).delete());

				// make sure the blob server cannot create any files in its storage dir
				tempFileDir = server.createTemporaryFilename().getParentFile();
				assertTrue(tempFileDir.setExecutable(true, false));
				assertTrue(tempFileDir.setReadable(true, false));
				assertTrue(tempFileDir.setWritable(false, false));

				// request the file from the BlobStore
				exception.expect(IOException.class);
				exception.expectMessage("Permission denied");

				try {
					get(server, jobId, blobKey, true);
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
			} finally {
				// set writable again to make sure we can remove the directory
				if (tempFileDir != null) {
					//noinspection ResultOfMethodCallIgnored
					tempFileDir.setWritable(true, false);
				}
			}
		} finally {
			if (blobStore != null) {
				blobStore.closeAndCleanupAllData();
			}
		}
	}

	/**
	 * Retrieves a BLOB from the HA store to a {@link BlobServer} which cannot create the final
	 * storage file. File transfers should fail.
	 */
	@Test
	public void testGetFailsStoreForJobHa() throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		final JobID jobId = new JobID();

		final Configuration config = new Configuration();
		config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
		config.setString(CoreOptions.STATE_BACKEND, "FILESYSTEM");
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.newFolder().getPath());

		BlobStoreService blobStore = null;

		try {
			blobStore = BlobUtils.createBlobStoreFromConfig(config);

			File jobStoreDir = null;
			try (BlobServer server = new BlobServer(config, blobStore)) {

				server.start();

				// store the data on the server (and blobStore), remove from local store
				byte[] data = new byte[2000000];
				rnd.nextBytes(data);
				BlobKey blobKey = put(server, jobId, data, true);
				assertTrue(server.getStorageLocation(jobId, blobKey).delete());

				// make sure the blob cache cannot create any files in its storage dir
				jobStoreDir = server.getStorageLocation(jobId, blobKey).getParentFile();
				assertTrue(jobStoreDir.setExecutable(true, false));
				assertTrue(jobStoreDir.setReadable(true, false));
				assertTrue(jobStoreDir.setWritable(false, false));

				// request the file from the BlobStore
				exception.expect(AccessDeniedException.class);

				try {
					get(server, jobId, blobKey, true);
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
		} finally {
			if (blobStore != null) {
				blobStore.closeAndCleanupAllData();
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

		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			server.start();

			// store the data on the server (and blobStore), remove from local store
			byte[] data = new byte[2000000];
			rnd.nextBytes(data);
			BlobKey blobKey = put(server, jobId, data, true);
			assertTrue(server.getStorageLocation(jobId, blobKey).delete());

			File tempFileDir = server.createTemporaryFilename().getParentFile();

			// request the file from the BlobStore
			exception.expect(NoSuchFileException.class);

			try {
				get(server, jobId, blobKey, true);
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

	@Test
	public void testConcurrentGetOperationsNoJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentGetOperations(null, false);
	}

	@Test
	public void testConcurrentGetOperationsForJob() throws IOException, ExecutionException, InterruptedException {
		testConcurrentGetOperations(new JobID(), false);
	}

	@Test
	public void testConcurrentGetOperationsForJobHa() throws IOException, ExecutionException, InterruptedException {
		testConcurrentGetOperations(new JobID(), true);
	}

	/**
	 * [FLINK-6020] Tests that concurrent get operations don't concurrently access the BlobStore to
	 * download a blob.
	 *
	 * @param jobId
	 * 		job ID to use (or <tt>null</tt> if job-unrelated)
	 * @param highAvailability
	 * 		whether to use permanent (<tt>true</tt>) or transient BLOBs (<tt>false</tt>)
	 */
	private void testConcurrentGetOperations(
			@Nullable final JobID jobId, final boolean highAvailability)
			throws IOException, InterruptedException, ExecutionException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		final BlobStore blobStore = mock(BlobStore.class);

		final int numberConcurrentGetOperations = 3;
		final List<CompletableFuture<File>> getOperations = new ArrayList<>(numberConcurrentGetOperations);

		final byte[] data = {1, 2, 3, 4, 99, 42};

		MessageDigest md = BlobUtils.createMessageDigest();

		// create the correct blob key by hashing our input data
		final BlobKey blobKey = new BlobKey(md.digest(data));

		doAnswer(
			new Answer() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					File targetFile = (File) invocation.getArguments()[2];

					FileUtils.writeByteArrayToFile(targetFile, data);

					return null;
				}
			}
		).when(blobStore).get(any(JobID.class), any(BlobKey.class), any(File.class));

		final ExecutorService executor = Executors.newFixedThreadPool(numberConcurrentGetOperations);

		try (final BlobServer server = new BlobServer(config, blobStore)) {

			server.start();

			// upload data first
			assertEquals(blobKey, put(server, jobId, data, highAvailability));

			// now try accessing it concurrently (only HA mode will be able to retrieve it from HA store!)
			if (highAvailability) {
				// remove local copy so that a transfer from HA store takes place
				assertTrue(server.getStorageLocation(jobId, blobKey).delete());
			}
			for (int i = 0; i < numberConcurrentGetOperations; i++) {
				CompletableFuture<File> getOperation = CompletableFuture.supplyAsync(
					() -> {
						try {
							File file = get(server, jobId, blobKey, highAvailability);
							// check that we have read the right data
							validateGetAndClose(new FileInputStream(file), data);
							return file;
						} catch (IOException e) {
							throw new CompletionException(new FlinkException(
								"Could not read blob for key " + blobKey + '.', e));
						}
					},
					executor);

				getOperations.add(getOperation);
			}

			CompletableFuture<Collection<File>> filesFuture = FutureUtils.combineAll(getOperations);
			filesFuture.get();
		} finally {
			executor.shutdownNow();
		}
	}

	/**
	 * Retrieves the given blob.
	 *
	 * <p>Note that if a {@link BlobCache} is used, it may try to access the {@link BlobServer} to
	 * retrieve the blob.
	 *
	 * @param service
	 * 		BLOB client to use for connecting to the BLOB service
	 * @param jobId
	 * 		job ID or <tt>null</tt> if job-unrelated
	 * @param key
	 * 		key identifying the BLOB to request
	 * @param highAvailability
	 * 		whether to check HA mode accessors
	 */
	static File get(
			BlobService service, @Nullable JobID jobId, BlobKey key, boolean highAvailability)
			throws IOException {
		if (highAvailability) {
			return service.getPermanentBlobStore().getHAFile(jobId, key);
		} else if (jobId == null) {
			return service.getTransientBlobStore().getFile(key);
		} else {
			return service.getTransientBlobStore().getFile(jobId, key);
		}
	}

	/**
	 * Checks that the given blob does not exist anymore by trying to access it.
	 *
	 * <p>Note that if a {@link BlobCache} is used, it may try to access the {@link BlobServer} to
	 * retrieve the blob.
	 *
	 * @param service
	 * 		BLOB client to use for connecting to the BLOB service
	 * @param jobId
	 * 		job ID or <tt>null</tt> if job-unrelated
	 * @param key
	 * 		key identifying the BLOB to request
	 * @param highAvailability
	 * 		whether to check HA mode accessors
	 */
	static void verifyDeleted(
			BlobService service, @Nullable JobID jobId, BlobKey key, boolean highAvailability)
			throws IOException {
		try {
			get(service, jobId, key, highAvailability);
			fail("File " + jobId + "/" + key + " should have been deleted.");
		} catch (IOException e) {
			// expected
		}
	}
}
