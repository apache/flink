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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.InetAddress;

import org.apache.flink.runtime.testutils.CommonTestUtils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link org.apache.flink.runtime.instance.InstanceManager}.
 */
public class InstanceManagerTest {
	
	
	@Test
	public void testInstanceRegistering() {
		try {
			InstanceManager cm = new InstanceManager();
			
			final int ipcPort = 10000;
			final int dataPort = 20000;

			HardwareDescription hardwareDescription = HardwareDescription.extractFromSystem(4096);

			InetAddress address = InetAddress.getByName("127.0.0.1");
			
			// register three instances
			InstanceConnectionInfo ici1 = new InstanceConnectionInfo(address, ipcPort + 0, dataPort + 0);
			InstanceConnectionInfo ici2 = new InstanceConnectionInfo(address, ipcPort + 15, dataPort + 15);
			InstanceConnectionInfo ici3 = new InstanceConnectionInfo(address, ipcPort + 30, dataPort + 30);
			
			InstanceID i1 = cm.registerTaskManager(ici1, hardwareDescription, 1);
			InstanceID i2 = cm.registerTaskManager(ici2, hardwareDescription, 2);
			InstanceID i3 = cm.registerTaskManager(ici3, hardwareDescription, 5);
			
			assertEquals(3, cm.getNumberOfRegisteredTaskManagers());
			assertEquals(8, cm.getTotalNumberOfSlots());
			
			assertEquals(ici1, cm.getAllRegisteredInstances().get(i1).getInstanceConnectionInfo());
			assertEquals(ici2, cm.getAllRegisteredInstances().get(i2).getInstanceConnectionInfo());
			assertEquals(ici3, cm.getAllRegisteredInstances().get(i3).getInstanceConnectionInfo());

			cm.shutdown();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test erroneous: " + e.getMessage());
		}
	}
	
	@Test
	public void testRegisteringAlreadyRegistered() {
		try {
			InstanceManager cm = new InstanceManager();
			
			final int ipcPort = 10000;
			final int dataPort = 20000;

			HardwareDescription resources = HardwareDescription.extractFromSystem(4096);
			InetAddress address = InetAddress.getByName("127.0.0.1");
			InstanceConnectionInfo ici = new InstanceConnectionInfo(address, ipcPort + 0, dataPort + 0);
			
			InstanceID i = cm.registerTaskManager(ici, resources, 1);

			assertNotNull(i);
			assertEquals(1, cm.getNumberOfRegisteredTaskManagers());
			assertEquals(1, cm.getTotalNumberOfSlots());
			
			InstanceID next = cm.registerTaskManager(ici, resources, 1);
			assertNull(next);
			
			assertEquals(1, cm.getNumberOfRegisteredTaskManagers());
			assertEquals(1, cm.getTotalNumberOfSlots());

			cm.shutdown();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test erroneous: " + e.getMessage());
		}
	}
	
	@Test
	public void testReportHeartbeat() {
		try {
			InstanceManager cm = new InstanceManager();
			
			final int ipcPort = 10000;
			final int dataPort = 20000;

			HardwareDescription hardwareDescription = HardwareDescription.extractFromSystem(4096);

			InetAddress address = InetAddress.getByName("127.0.0.1");
			
			// register three instances
			InstanceConnectionInfo ici1 = new InstanceConnectionInfo(address, ipcPort + 0, dataPort + 0);
			InstanceConnectionInfo ici2 = new InstanceConnectionInfo(address, ipcPort + 1, dataPort + 1);
			InstanceConnectionInfo ici3 = new InstanceConnectionInfo(address, ipcPort + 2, dataPort + 2);
			
			InstanceID i1 = cm.registerTaskManager(ici1, hardwareDescription, 1);
			InstanceID i2 = cm.registerTaskManager(ici2, hardwareDescription, 1);
			InstanceID i3 = cm.registerTaskManager(ici3, hardwareDescription, 1);

			// report some immediate heart beats
			assertTrue(cm.reportHeartBeat(i1));
			assertTrue(cm.reportHeartBeat(i2));
			assertTrue(cm.reportHeartBeat(i3));
			
			// report heart beat for non-existing instance
			assertFalse(cm.reportHeartBeat(new InstanceID()));
			
			final long WAIT = 200;
			CommonTestUtils.sleepUninterruptibly(WAIT);
			
			long h1 = cm.getAllRegisteredInstances().get(i1).getLastHeartBeat();
			long h2 = cm.getAllRegisteredInstances().get(i2).getLastHeartBeat();
			long h3 = cm.getAllRegisteredInstances().get(i3).getLastHeartBeat();

			// send one heart beat again and verify that the
			assertTrue(cm.reportHeartBeat(i1));
			long newH1 = cm.getAllRegisteredInstances().get(i1).getLastHeartBeat();
			
			long now = System.currentTimeMillis();
			
			assertTrue(now - h1 >= WAIT);
			assertTrue(now - h2 >= WAIT);
			assertTrue(now - h3 >= WAIT);
			assertTrue(now - newH1 <= WAIT);
			
			cm.shutdown();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test erroneous: " + e.getMessage());
		}
	}
	
	@Test
	public void testShutdown() {
		try {
			InstanceManager cm = new InstanceManager();
			cm.shutdown();
			
			try {
				HardwareDescription resources = HardwareDescription.extractFromSystem(4096);
				InetAddress address = InetAddress.getByName("127.0.0.1");
				InstanceConnectionInfo ici = new InstanceConnectionInfo(address, 10000, 20000);
		
				cm.registerTaskManager(ici, resources, 1);
				fail("Should raise exception in shutdown state");
			}
			catch (IllegalStateException e) {
				// expected
			}
			
			try {
				cm.reportHeartBeat(new InstanceID());
				fail("Should raise exception in shutdown state");
			}
			catch (IllegalStateException e) {
				// expected
			}
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test erroneous: " + e.getMessage());
		}
	}

	/**
	 * This test checks the clean-up routines of the cluster manager.
	 */
	@Test
	public void testCleanUp() {
		try {
			InstanceManager cm = new InstanceManager(200, 100);

			HardwareDescription resources = HardwareDescription.extractFromSystem(4096);
			InetAddress address = InetAddress.getByName("127.0.0.1");
			InstanceConnectionInfo ici1 = new InstanceConnectionInfo(address, 10000, 20000);
			InstanceConnectionInfo ici2 = new InstanceConnectionInfo(address, 10001, 20001);

			// register three instances
			InstanceID i1 = cm.registerTaskManager(ici1, resources, 1);
			InstanceID i2 = cm.registerTaskManager(ici2, resources, 1);

			assertNotNull(i1);
			assertNotNull(i2);
			
			assertEquals(2, cm.getNumberOfRegisteredTaskManagers());
			assertEquals(2, cm.getTotalNumberOfSlots());

			// report a few heatbeats for both of the machines (each 50 msecs)...
			for (int i = 0; i < 8; i++) {
				CommonTestUtils.sleepUninterruptibly(50);
				
				assertTrue(cm.reportHeartBeat(i1));
				assertTrue(cm.reportHeartBeat(i2));
			}
			
			// all should be alive
			assertEquals(2, cm.getNumberOfRegisteredTaskManagers());
			assertEquals(2, cm.getTotalNumberOfSlots());

			// report a few heatbeats for both only one machine
			for (int i = 0; i < 8; i++) {
				CommonTestUtils.sleepUninterruptibly(50);
				
				assertTrue(cm.reportHeartBeat(i1));
			}
			
			// we should have lost one TM by now
			assertEquals(1, cm.getNumberOfRegisteredTaskManagers());
			assertEquals(1, cm.getTotalNumberOfSlots());
			
			// if the lost TM reports, it should not be accepted
			assertFalse(cm.reportHeartBeat(i2));
			
			// allow the lost TM to re-register itself
			i2 = cm.registerTaskManager(ici2, resources, 1);
			assertEquals(2, cm.getNumberOfRegisteredTaskManagers());
			assertEquals(2, cm.getTotalNumberOfSlots());
			
			// report a few heatbeats for both of the machines (each 50 msecs)...
			for (int i = 0; i < 8; i++) {
				CommonTestUtils.sleepUninterruptibly(50);
				
				assertTrue(cm.reportHeartBeat(i1));
				assertTrue(cm.reportHeartBeat(i2));
			}
			
			// all should be alive
			assertEquals(2, cm.getNumberOfRegisteredTaskManagers());
			assertEquals(2, cm.getTotalNumberOfSlots());

			
			cm.shutdown();
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test erroneous: " + e.getMessage());
		}
	}

}
