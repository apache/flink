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
import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.FlinkFutureException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests how failing GET requests behave in the presence of failures.
 * Successful GET requests are tested in conjunction wit the PUT
 * requests.
 */
public class BlobServerGetTest extends TestLogger {

	private final Random rnd = new Random();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testGetFailsDuringLookup() throws IOException {
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
			BlobKey key = client.put(data);
			assertNotNull(key);

			// delete all files to make sure that GET requests fail
			File blobFile = server.getStorageLocation(key);
			assertTrue(blobFile.delete());

			// issue a GET request that fails
			try {
				client.get(key);
				fail("This should not succeed.");
			} catch (IOException e) {
				// expected
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
	public void testGetFailsDuringStreaming() throws IOException {
		BlobServer server = null;
		BlobClient client = null;

		try {
			final Configuration config = new Configuration();
			config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			server = new BlobServer(config, new VoidBlobStore());

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[5000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = client.put(data);
			assertNotNull(key);

			// issue a GET request that succeeds
			InputStream is = client.get(key);

			byte[] receiveBuffer = new byte[data.length];
			int firstChunkLen = 50000;
			BlobUtils.readFully(is, receiveBuffer, 0, firstChunkLen, null);
			BlobUtils.readFully(is, receiveBuffer, firstChunkLen, firstChunkLen, null);

			// shut down the server
			for (BlobServerConnection conn : server.getCurrentActiveConnections()) {
				conn.close();
			}

			try {
				BlobUtils.readFully(is, receiveBuffer, 2 * firstChunkLen, data.length - 2 * firstChunkLen, null);
				// we tolerate that this succeeds, as the receiver socket may have buffered
				// everything already, but in this case, also verify the contents
				assertArrayEquals(data, receiveBuffer);
			}
			catch (IOException e) {
				// expected
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

	/**
	 * FLINK-6020
	 *
	 * Tests that concurrent get operations don't concurrently access the BlobStore to download a blob.
	 */
	@Test
	public void testConcurrentGetOperations() throws IOException, ExecutionException, InterruptedException {

		final Configuration config = new Configuration();
		config.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		final BlobStore blobStore = mock(BlobStore.class);

		final int numberConcurrentGetOperations = 3;
		final List<CompletableFuture<InputStream>> getOperations = new ArrayList<>(numberConcurrentGetOperations);

		final byte[] data = {1, 2, 3, 4, 99, 42};
		final ByteArrayInputStream bais = new ByteArrayInputStream(data);

		MessageDigest md = BlobUtils.createMessageDigest();

		// create the correct blob key by hashing our input data
		final BlobKey blobKey = new BlobKey(md.digest(data));

		doAnswer(
			new Answer() {
				@Override
				public Object answer(InvocationOnMock invocation) throws Throwable {
					File targetFile = (File) invocation.getArguments()[1];

					FileUtils.copyInputStreamToFile(bais, targetFile);

					return null;
				}
			}
		).when(blobStore).get(any(BlobKey.class), any(File.class));

		final ExecutorService executor = Executors.newFixedThreadPool(numberConcurrentGetOperations);

		try (final BlobServer blobServer = new BlobServer(config, blobStore)) {
			for (int i = 0; i < numberConcurrentGetOperations; i++) {
				CompletableFuture<InputStream> getOperation = CompletableFuture.supplyAsync(
					() -> {
						try (BlobClient blobClient = blobServer.createClient();
							 InputStream inputStream = blobClient.get(blobKey)) {
							byte[] buffer = new byte[data.length];

							IOUtils.readFully(inputStream, buffer);

							return new ByteArrayInputStream(buffer);
						} catch (IOException e) {
							throw new FlinkFutureException("Could not read blob for key " + blobKey + '.', e);
						}
					},
					executor);

				getOperations.add(getOperation);
			}

			CompletableFuture<Collection<InputStream>> inputStreamsFuture = FutureUtils.combineAll(getOperations);

			Collection<InputStream> inputStreams = inputStreamsFuture.get();

			// check that we have read the right data
			for (InputStream inputStream : inputStreams) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);

				IOUtils.copy(inputStream, baos);

				baos.close();
				byte[] input = baos.toByteArray();

				assertArrayEquals(data, input);

				inputStream.close();
			}

			// verify that we downloaded the requested blob exactly once from the BlobStore
			verify(blobStore, times(1)).get(eq(blobKey), any(File.class));
		} finally {
			executor.shutdownNow();
		}
	}
}
