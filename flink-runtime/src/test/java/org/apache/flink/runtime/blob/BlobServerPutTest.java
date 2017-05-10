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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
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

	@Test
	public void testPutBufferSuccessful() throws IOException {
		BlobServer server = null;
		BlobClient client = null;

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config, new VoidBlobStore());

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key1 = client.put(data);
			assertNotNull(key1);

			BlobKey key2 = client.put(data, 10, 44);
			assertNotNull(key2);

			// put under job and name scope
			JobID jid = new JobID();
			String stringKey = "my test key";
			client.put(jid, stringKey, data);

			// --- GET the data and check that it is equal ---

			// one get request on the same client
			InputStream is1 = client.get(key2);
			byte[] result1 = new byte[44];
			BlobUtils.readFully(is1, result1, 0, result1.length, null);
			is1.close();

			for (int i = 0, j = 10; i < result1.length; i++, j++) {
				assertEquals(data[j], result1[i]);
			}

			// close the client and create a new one for the remaining requests
			client.close();
			client = new BlobClient(serverAddress, config);

			InputStream is2 = client.get(key1);
			byte[] result2 = new byte[data.length];
			BlobUtils.readFully(is2, result2, 0, result2.length, null);
			is2.close();
			assertArrayEquals(data, result2);

			InputStream is3 = client.get(jid, stringKey);
			byte[] result3 = new byte[data.length];
			BlobUtils.readFully(is3, result3, 0, result3.length, null);
			is3.close();
			assertArrayEquals(data, result3);
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
	public void testPutStreamSuccessful() throws IOException {
		BlobServer server = null;
		BlobClient client = null;

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config, new VoidBlobStore());

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			{
				BlobKey key1 = client.put(new ByteArrayInputStream(data));
				assertNotNull(key1);

			}

			// put under job and name scope
			{
				JobID jid = new JobID();
				String stringKey = "my test key";
				client.put(jid, stringKey, new ByteArrayInputStream(data));
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
	public void testPutChunkedStreamSuccessful() throws IOException {
		BlobServer server = null;
		BlobClient client = null;

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config, new VoidBlobStore());

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			{
				BlobKey key1 = client.put(new ChunkedInputStream(data, 19));
				assertNotNull(key1);

			}

			// put under job and name scope
			{
				JobID jid = new JobID();
				String stringKey = "my test key";
				client.put(jid, stringKey, new ChunkedInputStream(data, 17));
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
	public void testPutBufferFails() throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		BlobServer server = null;
		BlobClient client = null;

		File tempFileDir = null;
		try {
			Configuration config = new Configuration();
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
				client.put(data);
				fail("This should fail.");
			}
			catch (IOException e) {
				assertTrue(e.getMessage(), e.getMessage().contains("Server side error"));
			}

			try {
				client.put(data);
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

	@Test
	public void testPutNamedBufferFails() throws IOException {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		BlobServer server = null;
		BlobClient client = null;

		File tempFileDir = null;
		try {
			Configuration config = new Configuration();
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

			// put under job and name scope
			try {
				JobID jid = new JobID();
				String stringKey = "my test key";
				client.put(jid, stringKey, data);
				fail("This should fail.");
			}
			catch (IOException e) {
				assertTrue(e.getMessage(), e.getMessage().contains("Server side error"));
			}

			try {
				JobID jid = new JobID();
				String stringKey = "another key";
				client.put(jid, stringKey, data);
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
	public void testConcurrentPutOperations() throws IOException, ExecutionException, InterruptedException {
		final Configuration configuration = new Configuration();
		BlobStore blobStore = mock(BlobStore.class);
		int concurrentPutOperations = 2;
		int dataSize = 1024;

		final CountDownLatch countDownLatch = new CountDownLatch(concurrentPutOperations);
		final byte[] data = new byte[dataSize];

		ArrayList<Future<BlobKey>> allFutures = new ArrayList(concurrentPutOperations);

		ExecutorService executor = Executors.newFixedThreadPool(concurrentPutOperations);

		try (
			final BlobServer blobServer = new BlobServer(configuration, blobStore)) {

			for (int i = 0; i < concurrentPutOperations; i++) {
				Future<BlobKey> putFuture = FlinkCompletableFuture.supplyAsync(new Callable<BlobKey>() {
					@Override
					public BlobKey call() throws Exception {
						try (BlobClient blobClient = blobServer.createClient()) {
							return blobClient.put(new BlockingInputStream(countDownLatch, data));
						}
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

			// check that we only uploaded the file once to the blob store
			verify(blobStore, times(1)).put(any(File.class), eq(blobKey));
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
