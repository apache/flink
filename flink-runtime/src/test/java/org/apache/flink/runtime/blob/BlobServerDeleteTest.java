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

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Tests how DELETE requests behave.
 */
public class BlobServerDeleteTest {

	private final Random rnd = new Random();

	@Test
	public void testDeleteSingleByBlobKey() {
		BlobServer server = null;
		BlobClient client = null;

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = client.put(data);
			assertNotNull(key);

			// second item
			data[0] ^= 1;
			BlobKey key2 = client.put(data);
			assertNotNull(key2);
			assertNotEquals(key, key2);

			// issue a DELETE request via the client
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

			// delete a file directly on the server
			server.delete(key2);
			try {
				server.getURL(key2);
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
	public void testDeleteSingleByName() {
		BlobServer server = null;
		BlobClient client = null;

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			JobID jobID = new JobID();
			String name1 = "random name";
			String name2 = "any nyme";

			client.put(jobID, name1, data);
			client.put(jobID, name2, data);

			// issue a DELETE request via the client
			client.delete(jobID, name1);
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

			// delete a file directly on the server
			server.delete(jobID, name2);
			try {
				server.getURL(jobID, name2);
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
	public void testDeleteAll() {
		BlobServer server = null;
		BlobClient client = null;

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

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
			// items for a second (different!) job ID
			final byte[] jobIdBytes = jobID.getBytes();
			jobIdBytes[0] ^= 1;
			JobID jobID2 = JobID.fromByteArray(jobIdBytes);
			client.put(jobID2, name1, data);
			client.put(jobID2, name2, new byte[712]);


			// issue a DELETE ALL request via the client
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

			// delete the second set of files directly on the server
			server.deleteAll(jobID2);
			try {
				server.getURL(jobID2, name1);
				fail("BLOB should have been deleted");
			}
			catch (IOException e) {
				// expected
			}
			try {
				server.getURL(jobID2, name2);
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

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = client.put(data);
			assertNotNull(key);

			File blobFile = server.getStorageLocation(key);
			assertTrue(blobFile.delete());

			// issue a DELETE request via the client
			try {
				client.delete(key);
			}
			catch (IOException e) {
				fail("DELETE operation should not fail if file is already deleted");
			}

			// issue a DELETE request on the server
			server.delete(key);
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

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			JobID jid = new JobID();
			String name = "------------fdghljEgRJHF+##4U789Q345";

			client.put(jid, name, data);

			File blobFile = server.getStorageLocation(jid, name);
			assertTrue(blobFile.delete());

			// issue a DELETE request via the client
			try {
				client.delete(jid, name);
			}
			catch (IOException e) {
				fail("DELETE operation should not fail if file is already deleted");
			}

			// issue a DELETE request on the server
			server.delete(jid, name);
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
	public void testDeleteFailsByBlobKey() {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		BlobServer server = null;
		BlobClient client = null;

		File blobFile = null;
		File directory = null;
		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = client.put(data);
			assertNotNull(key);

			blobFile = server.getStorageLocation(key);
			directory = blobFile.getParentFile();

			assertTrue(blobFile.setWritable(false, false));
			assertTrue(directory.setWritable(false, false));

			// issue a DELETE request via the client
			client.delete(key);

			// issue a DELETE request on the server
			server.delete(key);

			// the file should still be there
			server.getURL(key);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			if (blobFile != null && directory != null) {
				blobFile.setWritable(true, false);
				directory.setWritable(true, false);
			}
			cleanup(server, client);
		}
	}

	@Test
	public void testDeleteByNameFails() {
		assumeTrue(!OperatingSystem.isWindows()); //setWritable doesn't work on Windows.

		BlobServer server = null;
		BlobClient client = null;

		File blobFile = null;
		File directory = null;
		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress, config);

			byte[] data = new byte[2000000];
			rnd.nextBytes(data);

			JobID jid = new JobID();
			String name = "------------fdghljEgRJHF+##4U789Q345";

			client.put(jid, name, data);

			blobFile = server.getStorageLocation(jid, name);
			directory = blobFile.getParentFile();

			assertTrue(blobFile.setWritable(false, false));
			assertTrue(directory.setWritable(false, false));

			// issue a DELETE request via the client
			client.delete(jid, name);

			// issue a DELETE request on the server
			server.delete(jid, name);

			// the file should still be there
			server.getURL(jid, name);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			blobFile.setWritable(true, false);
			directory.setWritable(true, false);
			cleanup(server, client);
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
			server.shutdown();
		}
	}
}
