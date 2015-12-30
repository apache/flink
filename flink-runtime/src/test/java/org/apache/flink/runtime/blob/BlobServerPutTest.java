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
import org.apache.flink.util.OperatingSystem;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Random;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for successful and failing PUT operations against the BLOB server,
 * and successful GET operations.
 */
public class BlobServerPutTest {

	private final Random rnd = new Random();

	@Test
	public void testPutBufferSuccessful() {
		BlobServer server = null;
		BlobClient client = null;

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress);

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
			client = new BlobClient(serverAddress);

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
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (client != null) {
				try {
					client.close();
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
			if (server != null) {
				server.shutdown();
			}
		}
	}


	@Test
	public void testPutStreamSuccessful() {
		BlobServer server = null;
		BlobClient client = null;

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress);

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
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (client != null) {
				try {
					client.close();
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
			if (server != null) {
				server.shutdown();
			}
		}
	}

	@Test
	public void testPutChunkedStreamSuccessful() {
		BlobServer server = null;
		BlobClient client = null;

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress);

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
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			if (client != null) {
				try {
					client.close();
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
			if (server != null) {
				server.shutdown();
			}
		}
	}

	@Test
	public void testPutBufferFails() {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		BlobServer server = null;
		BlobClient client = null;

		File tempFileDir = null;
		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			// make sure the blob server cannot create any files in its storage dir
			tempFileDir = server.createTemporaryFilename().getParentFile().getParentFile();
			assertTrue(tempFileDir.setExecutable(true, false));
			assertTrue(tempFileDir.setReadable(true, false));
			assertTrue(tempFileDir.setWritable(false, false));

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress);

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

		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			// set writable again to make sure we can remove the directory
			if (tempFileDir != null) {
				tempFileDir.setWritable(true, false);
			}
			if (client != null) {
				try {
					client.close();
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
			if (server != null) {
				server.shutdown();
			}
		}
	}

	@Test
	public void testPutNamedBufferFails() {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		BlobServer server = null;
		BlobClient client = null;

		File tempFileDir = null;
		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			// make sure the blob server cannot create any files in its storage dir
			tempFileDir = server.createTemporaryFilename().getParentFile().getParentFile();
			assertTrue(tempFileDir.setExecutable(true, false));
			assertTrue(tempFileDir.setReadable(true, false));
			assertTrue(tempFileDir.setWritable(false, false));

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress);

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
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			// set writable again to make sure we can remove the directory
			if (tempFileDir != null) {
				tempFileDir.setWritable(true, false);
			}
			if (client != null) {
				try {
					client.close();
				} catch (Throwable t) {
					t.printStackTrace();
				}
			}
			if (server != null) {
				server.shutdown();
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
