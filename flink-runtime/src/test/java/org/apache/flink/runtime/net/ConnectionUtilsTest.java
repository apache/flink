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
package org.apache.flink.runtime.net;

import static org.junit.Assert.*;

import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * Tests for the network utilities.
 */
public class ConnectionUtilsTest {

	@Test
	public void testReturnLocalHostAddressUsingHeuristics() {
		try (ServerSocket blocker = new ServerSocket(0, 1, InetAddress.getLocalHost())) {
			// the "blocker" server socket simply does not accept connections
			// this address is consequently "unreachable"
			InetSocketAddress unreachable = new InetSocketAddress("localhost", blocker.getLocalPort());
			
			final long start = System.currentTimeMillis();
			InetAddress add = ConnectionUtils.findConnectingAddress(unreachable, 2000, 400);

			// check that it did not take forever
			// this check can unfortunately not be too tight, or it will be flaky on some CI infrastructure
			assertTrue(System.currentTimeMillis() - start < 30000);

			// we should have found a heuristic address
			assertNotNull(add);

			// make sure that we returned the InetAddress.getLocalHost as a heuristic
			assertEquals(InetAddress.getLocalHost(), add);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
