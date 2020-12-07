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
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.ServerSocket;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests to ensure that the BlobServer properly starts on a specified range of available ports.
 */
public class BlobServerRangeTest extends TestLogger {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Start blob server on 0 = pick an ephemeral port.
	 */
	@Test
	public void testOnEphemeralPort() throws IOException {
		Configuration conf = new Configuration();
		conf.setString(BlobServerOptions.PORT, "0");
		conf.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		BlobServer server = new BlobServer(conf, new VoidBlobStore());
		server.start();
		server.close();
	}

	/**
	 * Try allocating on an unavailable port.
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
		conf.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		// this thing is going to throw an exception
		try {
			BlobServer server = new BlobServer(conf, new VoidBlobStore());
			server.start();
		} finally {
			socket.close();
		}
	}

	/**
	 * Give the BlobServer a choice of three ports, where two of them
	 * are allocated.
	 */
	@Test
	public void testOnePortAvailable() throws IOException {
		int numAllocated = 2;
		ServerSocket[] sockets = new ServerSocket[numAllocated];
		for (int i = 0; i < numAllocated; i++) {
			try {
				sockets[i] = new ServerSocket(0);
			} catch (IOException e) {
				e.printStackTrace();
				Assert.fail("An exception was thrown while preparing the test " + e.getMessage());
			}
		}
		Configuration conf = new Configuration();
		conf.setString(BlobServerOptions.PORT, sockets[0].getLocalPort() + "," + sockets[1].getLocalPort() + ",50000-50050");
		conf.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		// this thing is going to throw an exception
		try {
			BlobServer server = new BlobServer(conf, new VoidBlobStore());
			server.start();
			assertThat(server.getPort(), allOf(greaterThanOrEqualTo(50000), lessThanOrEqualTo(50050)));
			server.close();
		} finally {
			for (int i = 0; i < numAllocated; ++i) {
				sockets[i].close();
			}
		}
	}
}
