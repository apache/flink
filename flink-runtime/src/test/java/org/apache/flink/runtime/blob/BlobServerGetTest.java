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
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests how failing GET requests behave in the presence of failures when used with a {@link
 * BlobServer}.
 * <p>
 * Successful GET requests are tested in conjunction wit the PUT requests.
 */
public class BlobServerGetTest extends TestLogger {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
			@Nullable final JobID jobId1, @Nullable final JobID jobId2, boolean highAvailabibility)
			throws IOException {
		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		try (BlobServer server = new BlobServer(config, new VoidBlobStore())) {

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = put(server, jobId1, data, highAvailabibility);
			assertNotNull(key);

			// delete file to make sure that GET requests fail
			File blobFile = server.getStorageLocation(jobId1, key);
			assertTrue(blobFile.delete());

			// issue a GET request that fails
			verifyDeleted(server, jobId1, key, highAvailabibility);

			// add the same data under a second jobId
			BlobKey key2 = put(server, jobId2, data, highAvailabibility);
			assertNotNull(key);
			assertEquals(key, key2);

			// request for jobId2 should succeed
			get(server, jobId2, key, highAvailabibility);
			// request for jobId1 should still fail
			verifyDeleted(server, jobId1, key, highAvailabibility);

			// same checks as for jobId1 but for jobId2 should also work:
			blobFile = server.getStorageLocation(jobId2, key);
			assertTrue(blobFile.delete());
			verifyDeleted(server, jobId2, key, highAvailabibility);
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
		final List<Future<File>> getOperations = new ArrayList<>(numberConcurrentGetOperations);

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

		try (final BlobServer blobServer = new BlobServer(config, blobStore)) {
			// upload data first
			assertEquals(blobKey, put(blobServer, jobId, data, highAvailability));

			// now try accessing it concurrently (only HA mode will be able to retrieve it from HA store!)
			if (highAvailability) {
				// remove local copy so that a transfer from HA store takes place
				assertTrue(blobServer.getStorageLocation(jobId, blobKey).delete());
			}
			for (int i = 0; i < numberConcurrentGetOperations; i++) {
				Future<File> getOperation = FlinkCompletableFuture
					.supplyAsync(new Callable<File>() {
					@Override
					public File call() throws Exception {
						File file = get(blobServer, jobId, blobKey, highAvailability);
						// check that we have read the right data
						try (InputStream is = new FileInputStream(file)) {
							BlobClientTest.validateGet(is, data);
						}
						return file;
					}
				}, executor);

				getOperations.add(getOperation);
			}

			Future<Collection<File>> filesFuture = FutureUtils.combineAll(getOperations);
			filesFuture.get();

			// TODO: verify that we stored the requested blob exactly once to the local BlobStore folder
			if (highAvailability) {
				// download could be up to numberConcurrentGetOperations times:
//				verify(blobStore, times(1)).get(eq(jobId), eq(blobKey), any(File.class));
			} else {
				// can't really verify much in the other cases other than that the get operations should
				// work and retrieve correct files
				verify(blobStore, times(0)).get(eq(jobId), eq(blobKey), any(File.class));
			}
		} finally {
			executor.shutdownNow();
		}
	}

	/**
	 * Retrieves the given blob.
	 * <p>
	 * Note that if a {@link BlobCache} is used, it may try to access the {@link BlobServer} to
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
	 * <p>
	 * Note that if a {@link BlobCache} is used, it may try to access the {@link BlobServer} to
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
