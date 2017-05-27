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

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Tests to ensure that the BlobServer properly starts on a specified range of available ports.
 */
public class BlobServerRangeTest extends TestLogger {
	/**
	 * Start blob server on 0 = pick an ephemeral port
	 */
	@Test
	public void testOnEphemeralPort() throws IOException {
		Configuration conf = new Configuration();
		conf.setString(BlobServerOptions.PORT, "0");
		BlobServer srv = new BlobServer(conf, new VoidBlobStore());
		srv.close();
	}

	/**
	 * Try allocating on an unavailable port
	 * @throws IOException
	 */
	@Test(expected = IOException.class)
	public void testPortUnavailable() throws IOException {
		// allocate on an ephemeral port
		ServerSocket socket = null;
		try {
			socket = new ServerSocket(0);
		} catch (IOException e) {
			e.printStackTrace();
			Assert.fail("An exception was thrown while preparing the test " + e.getMessage());
		}

		Configuration conf = new Configuration();
		conf.setString(BlobServerOptions.PORT, String.valueOf(socket.getLocalPort()));

		// this thing is going to throw an exception
		try {
			BlobServer srv = new BlobServer(conf, new VoidBlobStore());
		} finally {
			socket.close();
		}
	}

	/**
	 * Give the BlobServer a choice of three ports, where two of them
	 * are allocated
	 */
	@Test
	public void testOnePortAvailable() throws IOException {
		int numAllocated = 2;
		ServerSocket[] sockets = new ServerSocket[numAllocated];
		for(int i = 0; i < numAllocated; i++) {
			ServerSocket socket = null;
			try {
				sockets[i] = new ServerSocket(0);
			} catch (IOException e) {
				e.printStackTrace();
				Assert.fail("An exception was thrown while preparing the test " + e.getMessage());
			}
		}
		int availablePort = NetUtils.getAvailablePort();
		Configuration conf = new Configuration();
		conf.setString(BlobServerOptions.PORT, sockets[0].getLocalPort() + "," + sockets[1].getLocalPort() + "," + availablePort);

		// this thing is going to throw an exception
		try {
			BlobServer srv = new BlobServer(conf, new VoidBlobStore());
			Assert.assertEquals(availablePort, srv.getPort());
			srv.close();
		} finally {
			sockets[0].close();
			sockets[1].close();
		}
	}
}
