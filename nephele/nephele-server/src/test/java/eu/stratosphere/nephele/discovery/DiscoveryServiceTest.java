/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.discovery;

import java.net.Inet4Address;
import java.net.InetAddress;

import org.junit.Test;

import static org.junit.Assert.*;

public class DiscoveryServiceTest {

	@Test
	public void testToNetmask() {
		/*
		 * assertArrayEquals(
		 * new byte[] {0, 0, 0, 0},
		 * DiscoveryService.toNetmask(0) );
		 * assertArrayEquals(
		 * new byte[] {(byte)255, 0, 0, 0},
		 * DiscoveryService.toNetmask(8) );
		 * assertArrayEquals(
		 * new byte[] {(byte)255, (byte)255, 0, 0},
		 * DiscoveryService.toNetmask(16) );
		 * assertArrayEquals(
		 * new byte[] {(byte)255, (byte)255, (byte)255, 0},
		 * DiscoveryService.toNetmask(24) );
		 * assertArrayEquals(
		 * new byte[] {(byte)255, (byte)255, (byte)255, (byte)255},
		 * DiscoveryService.toNetmask(32) );
		 * assertArrayEquals(
		 * new byte[] {(byte)255, (byte)224, 0, 0},
		 * DiscoveryService.toNetmask(8+3) );
		 */
	}

	@Test
	public void testOnSameNetwork() throws Exception {
		/*
		 * Inet4Address a = (Inet4Address)InetAddress.getByAddress(new byte[] {(byte)192, (byte)168, (byte)0, (byte)1});
		 * Inet4Address b = (Inet4Address)InetAddress.getByAddress(new byte[] {(byte)192, (byte)168, (byte)1, (byte)1});
		 * assertTrue( DiscoveryService.onSameNetwork(a, b, 16));
		 * assertFalse( DiscoveryService.onSameNetwork(a, b, 24));
		 */
	}

}
