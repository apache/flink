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

import static org.junit.Assert.*;

import java.lang.reflect.Method;
import java.net.InetAddress;

import akka.actor.ActorRef;
import org.apache.flink.api.common.JobID;
import org.junit.Test;

/**
 * Tests for the {@link Instance} class.
 */
public class InstanceTest {

	@Test
	public void testAllocatingAndCancellingSlots() {
		try {
			HardwareDescription hardwareDescription = new HardwareDescription(4, 2L*1024*1024*1024, 1024*1024*1024, 512*1024*1024);
			InetAddress address = InetAddress.getByName("127.0.0.1");
			InstanceConnectionInfo connection = new InstanceConnectionInfo(address, 10001);

			Instance instance = new Instance(ActorRef.noSender(), connection, new InstanceID(), hardwareDescription, 4);

			assertEquals(4, instance.getTotalNumberOfSlots());
			assertEquals(4, instance.getNumberOfAvailableSlots());
			assertEquals(0, instance.getNumberOfAllocatedSlots());

			SimpleSlot slot1 = instance.allocateSimpleSlot(new JobID());
			SimpleSlot slot2 = instance.allocateSimpleSlot(new JobID());
			SimpleSlot slot3 = instance.allocateSimpleSlot(new JobID());
			SimpleSlot slot4 = instance.allocateSimpleSlot(new JobID());

			assertNotNull(slot1);
			assertNotNull(slot2);
			assertNotNull(slot3);
			assertNotNull(slot4);

			assertEquals(0, instance.getNumberOfAvailableSlots());
			assertEquals(4, instance.getNumberOfAllocatedSlots());
			assertEquals(6, slot1.getSlotNumber() + slot2.getSlotNumber() +
					slot3.getSlotNumber() + slot4.getSlotNumber());

			// no more slots
			assertNull(instance.allocateSimpleSlot(new JobID()));
			try {
				instance.returnAllocatedSlot(slot2);
				fail("instance accepted a non-cancelled slot.");
			}
			catch (IllegalArgumentException e) {
				// good
			}

			// release the slots. this returns them to the instance
			slot1.releaseSlot();
			slot2.releaseSlot();
			slot3.releaseSlot();
			slot4.releaseSlot();

			assertEquals(4, instance.getNumberOfAvailableSlots());
			assertEquals(0, instance.getNumberOfAllocatedSlots());

			assertFalse(instance.returnAllocatedSlot(slot1));
			assertFalse(instance.returnAllocatedSlot(slot2));
			assertFalse(instance.returnAllocatedSlot(slot3));
			assertFalse(instance.returnAllocatedSlot(slot4));

			assertEquals(4, instance.getNumberOfAvailableSlots());
			assertEquals(0, instance.getNumberOfAllocatedSlots());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testInstanceDies() {
		try {
			HardwareDescription hardwareDescription = new HardwareDescription(4, 2L*1024*1024*1024, 1024*1024*1024, 512*1024*1024);
			InetAddress address = InetAddress.getByName("127.0.0.1");
			InstanceConnectionInfo connection = new InstanceConnectionInfo(address, 10001);

			Instance instance = new Instance(ActorRef.noSender(), connection, new InstanceID(), hardwareDescription, 3);

			assertEquals(3, instance.getNumberOfAvailableSlots());

			SimpleSlot slot1 = instance.allocateSimpleSlot(new JobID());
			SimpleSlot slot2 = instance.allocateSimpleSlot(new JobID());
			SimpleSlot slot3 = instance.allocateSimpleSlot(new JobID());

			instance.markDead();

			assertEquals(0, instance.getNumberOfAllocatedSlots());
			assertEquals(0, instance.getNumberOfAvailableSlots());

			assertTrue(slot1.isCanceled());
			assertTrue(slot2.isCanceled());
			assertTrue(slot3.isCanceled());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCancelAllSlots() {
		try {
			HardwareDescription hardwareDescription = new HardwareDescription(4, 2L*1024*1024*1024, 1024*1024*1024, 512*1024*1024);
			InetAddress address = InetAddress.getByName("127.0.0.1");
			InstanceConnectionInfo connection = new InstanceConnectionInfo(address, 10001);

			Instance instance = new Instance(ActorRef.noSender(), connection, new InstanceID(), hardwareDescription, 3);

			assertEquals(3, instance.getNumberOfAvailableSlots());

			SimpleSlot slot1 = instance.allocateSimpleSlot(new JobID());
			SimpleSlot slot2 = instance.allocateSimpleSlot(new JobID());
			SimpleSlot slot3 = instance.allocateSimpleSlot(new JobID());

			instance.cancelAndReleaseAllSlots();

			assertEquals(3, instance.getNumberOfAvailableSlots());

			assertTrue(slot1.isCanceled());
			assertTrue(slot2.isCanceled());
			assertTrue(slot3.isCanceled());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * It is crucial for some portions of the code that instance objects do not override equals and
	 * are only considered equal, if the references are equal.
	 */
	@Test
	public void testInstancesReferenceEqual() {
		try {
			Method m = Instance.class.getMethod("equals", Object.class);
			assertTrue(m.getDeclaringClass() == Object.class);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}