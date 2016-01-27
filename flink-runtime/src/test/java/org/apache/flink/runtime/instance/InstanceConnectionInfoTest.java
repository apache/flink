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

package org.apache.flink.runtime.instance;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the InstanceConnectionInfo, which identifies the location and connection
 * information of a TaskManager.
 */
public class InstanceConnectionInfoTest {

	@Test
	public void testEqualsHashAndCompareTo() {
		try {
			// we mock the addresses to save the times of the reverse name lookups
			InetAddress address1 = mock(InetAddress.class);
			when(address1.getCanonicalHostName()).thenReturn("localhost");
			when(address1.getHostName()).thenReturn("localhost");
			when(address1.getHostAddress()).thenReturn("127.0.0.1");
			when(address1.getAddress()).thenReturn(new byte[] {127, 0, 0, 1} );

			InetAddress address2 = mock(InetAddress.class);
			when(address2.getCanonicalHostName()).thenReturn("testhost1");
			when(address2.getHostName()).thenReturn("testhost1");
			when(address2.getHostAddress()).thenReturn("0.0.0.0");
			when(address2.getAddress()).thenReturn(new byte[] {0, 0, 0, 0} );

			InetAddress address3 = mock(InetAddress.class);
			when(address3.getCanonicalHostName()).thenReturn("testhost2");
			when(address3.getHostName()).thenReturn("testhost2");
			when(address3.getHostAddress()).thenReturn("192.168.0.1");
			when(address3.getAddress()).thenReturn(new byte[] {(byte) 192, (byte) 168, 0, 1} );

			// one == four != two != three
			InstanceConnectionInfo one = new InstanceConnectionInfo(address1, 19871);
			InstanceConnectionInfo two = new InstanceConnectionInfo(address2, 19871);
			InstanceConnectionInfo three = new InstanceConnectionInfo(address3, 10871);
			InstanceConnectionInfo four = new InstanceConnectionInfo(address1, 19871);
			
			assertTrue(one.equals(four));
			assertTrue(!one.equals(two));
			assertTrue(!one.equals(three));
			assertTrue(!two.equals(three));
			assertTrue(!three.equals(four));
			
			assertTrue(one.compareTo(four) == 0);
			assertTrue(four.compareTo(one) == 0);
			assertTrue(one.compareTo(two) != 0);
			assertTrue(one.compareTo(three) != 0);
			assertTrue(two.compareTo(three) != 0);
			assertTrue(three.compareTo(four) != 0);
			
			{
				int val = one.compareTo(two);
				assertTrue(two.compareTo(one) == -val);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSerialization() {
		try {
			// without resolved hostname
			{
				InstanceConnectionInfo original = new InstanceConnectionInfo(InetAddress.getByName("1.2.3.4"), 8888);
				
				InstanceConnectionInfo copy = CommonTestUtils.createCopyWritable(original);
				assertEquals(original, copy);
				
				InstanceConnectionInfo serCopy = CommonTestUtils.createCopySerializable(original);
				assertEquals(original, serCopy);
			}
						
			// with resolved hostname
			{
				InstanceConnectionInfo original = new InstanceConnectionInfo(InetAddress.getByName("127.0.0.1"), 19871);
				original.getFQDNHostname();
				
				InstanceConnectionInfo copy = CommonTestUtils.createCopyWritable(original);
				assertEquals(original, copy);
				
				InstanceConnectionInfo serCopy = CommonTestUtils.createCopySerializable(original);
				assertEquals(original, serCopy);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testGetFQDNHostname() {
		try {
			InstanceConnectionInfo info1 = new InstanceConnectionInfo(InetAddress.getByName("127.0.0.1"), 19871);
			assertNotNull(info1.getFQDNHostname());
			
			InstanceConnectionInfo info2 = new InstanceConnectionInfo(InetAddress.getByName("1.2.3.4"), 8888);
			assertNotNull(info2.getFQDNHostname());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testGetHostname0() {
		try {
			InetAddress address = mock(InetAddress.class);
			when(address.getCanonicalHostName()).thenReturn("worker2.cluster.mycompany.com");
			when(address.getHostName()).thenReturn("worker2.cluster.mycompany.com");
			when(address.getHostAddress()).thenReturn("127.0.0.1");

			final InstanceConnectionInfo info = new InstanceConnectionInfo(address, 19871);
			Assert.assertEquals("worker2", info.getHostname());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testGetHostname1() {
		try {
			InetAddress address = mock(InetAddress.class);
			when(address.getCanonicalHostName()).thenReturn("worker10");
			when(address.getHostName()).thenReturn("worker10");
			when(address.getHostAddress()).thenReturn("127.0.0.1");

			InstanceConnectionInfo info = new InstanceConnectionInfo(address, 19871);
			Assert.assertEquals("worker10", info.getHostname());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testGetHostname2() {
		try {
			final String addressString = "192.168.254.254";

			// we mock the addresses to save the times of the reverse name lookups
			InetAddress address = mock(InetAddress.class);
			when(address.getCanonicalHostName()).thenReturn("192.168.254.254");
			when(address.getHostName()).thenReturn("192.168.254.254");
			when(address.getHostAddress()).thenReturn("192.168.254.254");
			when(address.getAddress()).thenReturn(new byte[] {(byte) 192, (byte) 168, (byte) 254, (byte) 254} );

			InstanceConnectionInfo info = new InstanceConnectionInfo(address, 54152);

			assertNotNull(info.getFQDNHostname());
			assertTrue(info.getFQDNHostname().equals(addressString));

			assertNotNull(info.getHostname());
			assertTrue(info.getHostname().equals(addressString));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
