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
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * Tests how failing GET requests behave in the presence of failures.
 * Successful GET requests are tested in conjunction wit the PUT
 * requests.
 */
public class BlobServerGetTest {

	private final Random rnd = new Random();

	@Test
	public void testGetFailsDuringLookup() {
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
			BlobKey key = client.put(data);
			assertNotNull(key);

			// delete all files to make sure that GET requests fail
			File blobFile = server.getStorageLocation(key);
			assertTrue(blobFile.delete());

			// issue a GET request that fails
			try {
				client.get(key);
				fail("This should not succeed.");
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
	public void testGetFailsDuringStreaming() {
		BlobServer server = null;
		BlobClient client = null;

		try {
			Configuration config = new Configuration();
			server = new BlobServer(config);

			InetSocketAddress serverAddress = new InetSocketAddress("localhost", server.getPort());
			client = new BlobClient(serverAddress);

			byte[] data = new byte[5000000];
			rnd.nextBytes(data);

			// put content addressable (like libraries)
			BlobKey key = client.put(data);
			assertNotNull(key);

			// issue a GET request that succeeds
			InputStream is = client.get(key);

			byte[] receiveBuffer = new byte[50000];
			BlobUtils.readFully(is, receiveBuffer, 0, receiveBuffer.length, null);
			BlobUtils.readFully(is, receiveBuffer, 0, receiveBuffer.length, null);

			// shut down the server
			for (BlobServerConnection conn : server.getCurrentActiveConnections()) {
				conn.close();
			}

			try {
				byte[] remainder = new byte[data.length - 2*receiveBuffer.length];
				BlobUtils.readFully(is, remainder, 0, remainder.length, null);
				// we tolerate that this succeeds, as the receiver socket may have buffered
				// everything already
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
}
