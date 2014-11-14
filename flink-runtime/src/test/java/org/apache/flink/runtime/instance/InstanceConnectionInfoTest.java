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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;

import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.reflect.Whitebox;

public class InstanceConnectionInfoTest {

	@Test
	public void testEqualsHashAndCompareTo() {
		try {
			// one == four != two != three
			InstanceConnectionInfo one = new InstanceConnectionInfo(InetAddress.getByName("127.0.0.1"), 19871);
			InstanceConnectionInfo two = new InstanceConnectionInfo(InetAddress.getByName("0.0.0.0"), 19871);
			InstanceConnectionInfo three = new InstanceConnectionInfo(InetAddress.getByName("192.168.0.1"), 10871);
			InstanceConnectionInfo four = new InstanceConnectionInfo(InetAddress.getByName("127.0.0.1"), 19871);
			
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
			assertTrue(info1.getFQDNHostname() != null);
			
			InstanceConnectionInfo info2 = new InstanceConnectionInfo(InetAddress.getByName("1.2.3.4"), 8888);
			assertTrue(info2.getFQDNHostname() != null);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testGetHostname0() {
		try {
			final InstanceConnectionInfo info1 = PowerMockito.spy(new InstanceConnectionInfo(InetAddress.getByName("127.0.0.1"), 19871));
			Whitebox.setInternalState(info1, "fqdnHostName", "worker2.cluster.mycompany.com");
			Assert.assertEquals("worker2", info1.getHostname());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testGetHostname1() {
		try {
			final InstanceConnectionInfo info1 = PowerMockito.spy(new InstanceConnectionInfo(InetAddress.getByName("127.0.0.1"), 19871));
			Whitebox.setInternalState(info1, "fqdnHostName", "worker10");
			Assert.assertEquals("worker10", info1.getHostname());
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	
}
