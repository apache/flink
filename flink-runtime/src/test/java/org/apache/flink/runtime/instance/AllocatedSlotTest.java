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

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.net.InetAddress;

import akka.actor.ActorRef;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.jobgraph.JobID;
import org.junit.Test;
import org.mockito.Matchers;

public class AllocatedSlotTest {

	@Test
	public void testStateTransitions() {
		try {
			// cancel, then release
			{
				AllocatedSlot slot = getSlot();
				assertTrue(slot.isAlive());
				
				slot.cancel();
				assertFalse(slot.isAlive());
				assertTrue(slot.isCanceled());
				assertFalse(slot.isReleased());
				
				slot.releaseSlot();
				assertFalse(slot.isAlive());
				assertTrue(slot.isCanceled());
				assertTrue(slot.isReleased());
			}
			
			// release immediately
			{
				AllocatedSlot slot = getSlot();
				assertTrue(slot.isAlive());
				
				slot.releaseSlot();
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
			Execution ev = mock(Execution.class);
			Execution ev_2 = mock(Execution.class);
			
			// assign to alive slot
			{
				AllocatedSlot slot = getSlot();
				
				assertTrue(slot.setExecutedVertex(ev));
				assertEquals(ev, slot.getExecutedVertex());
				
				// try to add another one
				assertFalse(slot.setExecutedVertex(ev_2));
				assertEquals(ev, slot.getExecutedVertex());
			}
			
			// assign to canceled slot
			{
				AllocatedSlot slot = getSlot();
				slot.cancel();
				
				assertFalse(slot.setExecutedVertex(ev));
				assertNull(slot.getExecutedVertex());
			}
			
			// assign to released
			{
				AllocatedSlot slot = getSlot();
				slot.releaseSlot();
				
				assertFalse(slot.setExecutedVertex(ev));
				assertNull(slot.getExecutedVertex());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testReleaseCancelsVertex() {
		try {
			Execution ev = mock(Execution.class);
			
			AllocatedSlot slot = getSlot();
			assertTrue(slot.setExecutedVertex(ev));
			assertEquals(ev, slot.getExecutedVertex());
			
			slot.cancel();
			slot.releaseSlot();
			slot.cancel();
			
			verify(ev, times(1)).fail(Matchers.any(Throwable.class));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	public static AllocatedSlot getSlot() throws Exception {
		HardwareDescription hardwareDescription = new HardwareDescription(4, 2L*1024*1024*1024, 1024*1024*1024, 512*1024*1024);
		InetAddress address = InetAddress.getByName("127.0.0.1");
		InstanceConnectionInfo connection = new InstanceConnectionInfo(address, 10001);
		
		Instance instance = new Instance(ActorRef.noSender(), connection, new InstanceID(), hardwareDescription, 1);
		return instance.allocateSlot(new JobID());
	}
}
