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

import org.apache.flink.util.OperatingSystem;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Tests for the network utilities.
 */
public class ConnectionUtilsTest {

	@Test
	public void testFindConnectableAddress() {
		int unusedPort;
		try {
			unusedPort = org.apache.flink.util.NetUtils.getAvailablePort();
		}
		catch (Throwable t) {
			// if this system cannot find an available port,
			// skip this test
			return;
		}

		try {
			// create an unreachable target address
			InetSocketAddress unreachable = new InetSocketAddress("localhost", unusedPort);

			final long start = System.currentTimeMillis();
			InetAddress add = ConnectionUtils.findConnectingAddress(unreachable, 2000, 400);

			// check that it did not take forever
			assertTrue(System.currentTimeMillis() - start < (OperatingSystem.isWindows() ? 30000 : 8000));

			// we should have found a heuristic address
			assertNotNull(add);

			// these checks are desirable, but will not work on every machine
			// such as machines with no connected network media, which may
			// default to a link local address
			// assertFalse(add.isLinkLocalAddress());
			// assertFalse(add.isLoopbackAddress());
			// assertFalse(add.isAnyLocalAddress());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
