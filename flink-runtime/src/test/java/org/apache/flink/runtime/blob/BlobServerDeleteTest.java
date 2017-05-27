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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertFalse;
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

	@Test
	public void testDeleteSingle() {
		BlobServer server = null;
		BlobClient client = null;
		BlobStore blobStore = new VoidBlobStore();

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config, blobStore);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = client.put(data);
			assertNotNull(key);

			// issue a DELETE request
			client.delete(key);
			client.close();

			client = new BlobClient(serverAddress, config);
			try {
				client.get(key);
				fail("BLOB should have been deleted");
			}
			catch (IOException e) {
				// expected
			}

			try {
				client.put(new byte[1]);
				fail("client should be closed after erroneous operation");
			}
			catch (IllegalStateException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			cleanup(server, client);
		}
	}

	@Test
	public void testDeleteAll() {
		BlobServer server = null;
		BlobClient client = null;
		BlobStore blobStore = new VoidBlobStore();

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config, blobStore);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			JobID jobID = new JobID();
			String name1 = "random name";
			String name2 = "any nyme";

			// put content addressable (like libraries)
			client.put(jobID, name1, data);
			client.put(jobID, name2, new byte[712]);


			// issue a DELETE ALL request
			client.deleteAll(jobID);
			client.close();

			client = new BlobClient(serverAddress, config);
			try {
				client.get(jobID, name1);
				fail("BLOB should have been deleted");
			}
			catch (IOException e) {
				// expected
			}

			try {
				client.put(new byte[1]);
				fail("client should be closed after erroneous operation");
			}
			catch (IllegalStateException e) {
				// expected
			}

			client = new BlobClient(serverAddress, config);
			try {
				client.get(jobID, name2);
				fail("BLOB should have been deleted");
			}
			catch (IOException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			cleanup(server, client);
		}
	}

	@Test
	public void testDeleteAlreadyDeletedByBlobKey() {
		BlobServer server = null;
		BlobClient client = null;
		BlobStore blobStore = new VoidBlobStore();

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config, blobStore);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = client.put(data);
			assertNotNull(key);

			File blobFile = server.getStorageLocation(key);
			assertTrue(blobFile.delete());

			// issue a DELETE request
			try {
				client.delete(key);
			}
			catch (IOException e) {
				fail("DELETE operation should not fail if file is already deleted");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			cleanup(server, client);
		}
	}

	@Test
	public void testDeleteAlreadyDeletedByName() {
		BlobServer server = null;
		BlobClient client = null;
		BlobStore blobStore = new VoidBlobStore();

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config, blobStore);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			JobID jid = new JobID();
			String name = "------------fdghljEgRJHF+##4U789Q345";

			client.put(jid, name, data);

			File blobFile = server.getStorageLocation(jid, name);
			assertTrue(blobFile.delete());

			// issue a DELETE request
			try {
				client.delete(jid, name);
			}
			catch (IOException e) {
				fail("DELETE operation should not fail if file is already deleted");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			cleanup(server, client);
		}
	}

	@Test
	public void testDeleteFails() {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		BlobServer server = null;
		BlobClient client = null;
		BlobStore blobStore = new VoidBlobStore();

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config, blobStore);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = client.put(data);
			assertNotNull(key);

			File blobFile = server.getStorageLocation(key);
			File directory = blobFile.getParentFile();

			assertTrue(blobFile.setWritable(false, false));
			assertTrue(directory.setWritable(false, false));

			// issue a DELETE request
			try {
				client.delete(key);
				fail("DELETE operation should fail if file cannot be deleted");
			}
			catch (IOException e) {
				// expected
			}
			finally {
				blobFile.setWritable(true, false);
				directory.setWritable(true, false);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			cleanup(server, client);
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
	public void testConcurrentDeleteOperations() throws IOException, ExecutionException, InterruptedException {
		final Configuration configuration = new Configuration();
		final BlobStore blobStore = mock(BlobStore.class);

		final int concurrentDeleteOperations = 3;
		final ExecutorService executor = Executors.newFixedThreadPool(concurrentDeleteOperations);

		final List<Future<Void>> deleteFutures = new ArrayList<>(concurrentDeleteOperations);

		final byte[] data = {1, 2, 3};

		try (final BlobServer blobServer = new BlobServer(configuration, blobStore)) {

			final BlobKey blobKey;

			try (BlobClient client = blobServer.createClient()) {
				blobKey = client.put(data);
			}

			assertTrue(blobServer.getStorageLocation(blobKey).exists());

			for (int i = 0; i < concurrentDeleteOperations; i++) {
				Future<Void> deleteFuture = FlinkCompletableFuture.supplyAsync(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						try (BlobClient blobClient = blobServer.createClient()) {
							blobClient.delete(blobKey);
						}

						return null;
					}
				}, executor);

				deleteFutures.add(deleteFuture);
			}

			Future<Void> waitFuture = FutureUtils.waitForAll(deleteFutures);

			// make sure all delete operation have completed successfully
			// in case of no lock, one of the delete operations should eventually fail
			waitFuture.get();

			assertFalse(blobServer.getStorageLocation(blobKey).exists());
		} finally {
			executor.shutdownNow();
		}
	}

	private void cleanup(BlobServer server, BlobClient client) {
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
