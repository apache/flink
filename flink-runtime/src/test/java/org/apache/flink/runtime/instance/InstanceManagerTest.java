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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.CommonTestUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link org.apache.flink.runtime.instance.InstanceManager}.
 */
public class InstanceManagerTest{

	static ActorSystem system;

	static UUID leaderSessionID = UUID.randomUUID();

	@BeforeClass
	public static void setup(){
		system = ActorSystem.create("TestingActorSystem", TestingUtils.testConfig());
	}

	@AfterClass
	public static void teardown(){
		JavaTestKit.shutdownActorSystem(system);
		system = null;
	}

	@Test
	public void testInstanceRegistering() {
		try {
			InstanceManager cm = new InstanceManager();
			
			final int dataPort = 20000;

			HardwareDescription hardwareDescription = HardwareDescription.extractFromSystem(4096);

			InetAddress address = InetAddress.getByName("127.0.0.1");
			
			// register three instances
			InstanceConnectionInfo ici1 = new InstanceConnectionInfo(address, dataPort);
			InstanceConnectionInfo ici2 = new InstanceConnectionInfo(address, dataPort + 15);
			InstanceConnectionInfo ici3 = new InstanceConnectionInfo(address, dataPort + 30);

			final JavaTestKit probe1 = new JavaTestKit(system);
			final JavaTestKit probe2 = new JavaTestKit(system);
			final JavaTestKit probe3 = new JavaTestKit(system);

			cm.registerTaskManager(probe1.getRef(), ici1, hardwareDescription, 1, leaderSessionID);
			cm.registerTaskManager(probe2.getRef(), ici2, hardwareDescription, 2, leaderSessionID);
			cm.registerTaskManager(probe3.getRef(), ici3, hardwareDescription, 5, leaderSessionID);
			
			assertEquals(3, cm.getNumberOfRegisteredTaskManagers());
			assertEquals(8, cm.getTotalNumberOfSlots());

			Collection<Instance> instances = cm.getAllRegisteredInstances();
			Set<InstanceConnectionInfo> instanceConnectionInfos = new
					HashSet<InstanceConnectionInfo>();

			for(Instance instance: instances){
				instanceConnectionInfos.add(instance.getInstanceConnectionInfo());
			}

			assertTrue(instanceConnectionInfos.contains(ici1));
			assertTrue(instanceConnectionInfos.contains(ici2));
			assertTrue(instanceConnectionInfos.contains(ici3));
			
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
			
			final int dataPort = 20000;

			HardwareDescription resources = HardwareDescription.extractFromSystem(4096);
			InetAddress address = InetAddress.getByName("127.0.0.1");
			InstanceConnectionInfo ici = new InstanceConnectionInfo(address, dataPort);

			JavaTestKit probe = new JavaTestKit(system);
			InstanceID i = cm.registerTaskManager(probe.getRef(), ici, resources, 1, leaderSessionID);

			assertNotNull(i);
			assertEquals(1, cm.getNumberOfRegisteredTaskManagers());
			assertEquals(1, cm.getTotalNumberOfSlots());
			
			InstanceID next = cm.registerTaskManager(probe.getRef(), ici, resources, 1, leaderSessionID);
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
			
			final int dataPort = 20000;

			HardwareDescription hardwareDescription = HardwareDescription.extractFromSystem(4096);

			InetAddress address = InetAddress.getByName("127.0.0.1");
			
			// register three instances
			InstanceConnectionInfo ici1 = new InstanceConnectionInfo(address, dataPort);
			InstanceConnectionInfo ici2 = new InstanceConnectionInfo(address, dataPort + 1);
			InstanceConnectionInfo ici3 = new InstanceConnectionInfo(address, dataPort + 2);

			JavaTestKit probe1 = new JavaTestKit(system);
			JavaTestKit probe2 = new JavaTestKit(system);
			JavaTestKit probe3 = new JavaTestKit(system);
			
			InstanceID i1 = cm.registerTaskManager(probe1.getRef(), ici1, hardwareDescription, 1, leaderSessionID);
			InstanceID i2 = cm.registerTaskManager(probe2.getRef(), ici2, hardwareDescription, 1, leaderSessionID);
			InstanceID i3 = cm.registerTaskManager(probe3.getRef(), ici3, hardwareDescription, 1, leaderSessionID);

			// report some immediate heart beats
			assertTrue(cm.reportHeartBeat(i1, new byte[] {}));
			assertTrue(cm.reportHeartBeat(i2, new byte[] {}));
			assertTrue(cm.reportHeartBeat(i3, new byte[] {}));
			
			// report heart beat for non-existing instance
			assertFalse(cm.reportHeartBeat(new InstanceID(), new byte[] {}));
			
			final long WAIT = 200;
			CommonTestUtils.sleepUninterruptibly(WAIT);

			Iterator<Instance> it = cm.getAllRegisteredInstances().iterator();

			Instance instance1 = it.next();

			long h1 = instance1.getLastHeartBeat();
			long h2 = it.next().getLastHeartBeat();
			long h3 = it.next().getLastHeartBeat();

			// send one heart beat again and verify that the
			assertTrue(cm.reportHeartBeat(instance1.getId(), new byte[] {}));
			long newH1 = instance1.getLastHeartBeat();
			
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
				InstanceConnectionInfo ici = new InstanceConnectionInfo(address, 20000);

				JavaTestKit probe = new JavaTestKit(system);
				cm.registerTaskManager(probe.getRef(), ici, resources, 1, leaderSessionID);
				fail("Should raise exception in shutdown state");
			}
			catch (IllegalStateException e) {
				// expected
			}
			
			assertFalse(cm.reportHeartBeat(new InstanceID(), new byte[] {}));
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			Assert.fail("Test erroneous: " + e.getMessage());
		}
	}
}
