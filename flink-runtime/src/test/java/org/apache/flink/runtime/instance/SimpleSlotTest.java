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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.TestingPayload;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SimpleSlotTest extends  TestLogger {

	@Test
	public void testStateTransitions() {
		try {
			// release immediately
			{
				SimpleSlot slot = getSlot();
				assertTrue(slot.isAlive());

				slot.releaseSlot();
				assertFalse(slot.isAlive());
				assertTrue(slot.isCanceled());
				assertTrue(slot.isReleased());
			}

			// state transitions manually
			{
				SimpleSlot slot = getSlot();
				assertTrue(slot.isAlive());

				slot.markCancelled();
				assertFalse(slot.isAlive());
				assertTrue(slot.isCanceled());
				assertFalse(slot.isReleased());

				slot.markReleased();
				assertFalse(slot.isAlive());
				assertTrue(slot.isCanceled());
				assertTrue(slot.isReleased());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSetExecutionVertex() {
		try {
			TestingPayload payload1 = new TestingPayload();
			TestingPayload payload2 = new TestingPayload();

			// assign to alive slot
			{
				SimpleSlot slot = getSlot();

				assertTrue(slot.tryAssignPayload(payload1));
				assertEquals(payload1, slot.getPayload());

				// try to add another one
				assertFalse(slot.tryAssignPayload(payload2));
				assertEquals(payload1, slot.getPayload());
			}

			// assign to canceled slot
			{
				SimpleSlot slot = getSlot();
				assertTrue(slot.markCancelled());

				assertFalse(slot.tryAssignPayload(payload1));
				assertNull(slot.getPayload());
			}

			// assign to released marked slot
			{
				SimpleSlot slot = getSlot();
				assertTrue(slot.markCancelled());
				assertTrue(slot.markReleased());

				assertFalse(slot.tryAssignPayload(payload1));
				assertNull(slot.getPayload());
			}
			
			// assign to released
			{
				SimpleSlot slot = getSlot();
				slot.releaseSlot();

				assertFalse(slot.tryAssignPayload(payload1));
				assertNull(slot.getPayload());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	public static SimpleSlot getSlot() throws Exception {
		ResourceID resourceID = ResourceID.generate();
		HardwareDescription hardwareDescription = new HardwareDescription(4, 2L*1024*1024*1024, 1024*1024*1024, 512*1024*1024);
		InetAddress address = InetAddress.getByName("127.0.0.1");
		TaskManagerLocation connection = new TaskManagerLocation(resourceID, address, 10001);

		Instance instance = new Instance(
			new ActorTaskManagerGateway(DummyActorGateway.INSTANCE),
			connection,
			new InstanceID(),
			hardwareDescription,
			1);

		return instance.allocateSimpleSlot();
	}
}
