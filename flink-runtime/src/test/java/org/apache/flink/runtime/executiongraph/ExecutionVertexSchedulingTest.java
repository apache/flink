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

package org.apache.flink.runtime.executiongraph;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getExecutionVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getInstance;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SlotAllocationFuture;

import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.mockito.Matchers;

public class ExecutionVertexSchedulingTest {
	private static ActorSystem system;

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
	public void testSlotReleasedWhenScheduledImmediately() {
		
		try {
			// a slot than cannot be deployed to
			final Instance instance = getInstance(ActorRef.noSender());
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			slot.cancel();
			assertFalse(slot.isReleased());
			
			final ExecutionJobVertex ejv = getExecutionVertex(new JobVertexID());
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.DEFAULT_TIMEOUT());
			
			Scheduler scheduler = mock(Scheduler.class);
			when(scheduler.scheduleImmediately(Matchers.any(ScheduledUnit.class))).thenReturn(slot);
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			// try to deploy to the slot
			vertex.scheduleForExecution(scheduler, false);
			
			// will have failed
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertTrue(slot.isReleased());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSlotReleasedWhenScheduledQueued() {

		try {
			// a slot than cannot be deployed to
			final Instance instance = getInstance(ActorRef.noSender());
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			slot.cancel();
			assertFalse(slot.isReleased());
			
			final SlotAllocationFuture future = new SlotAllocationFuture();
			
			final ExecutionJobVertex ejv = getExecutionVertex(new JobVertexID());
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.DEFAULT_TIMEOUT());
			
			Scheduler scheduler = mock(Scheduler.class);
			when(scheduler.scheduleQueued(Matchers.any(ScheduledUnit.class))).thenReturn(future);
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			// try to deploy to the slot
			vertex.scheduleForExecution(scheduler, true);
			
			// future has not yet a slot
			assertEquals(ExecutionState.SCHEDULED, vertex.getExecutionState());
			
			future.setSlot(slot);
			
			// will have failed
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertTrue(slot.isReleased());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testScheduleToRunning() {
		try {
			TestingUtils.setCallingThreadDispatcher(system);
			ActorRef tm = TestActorRef.create(system, Props.create(ExecutionGraphTestUtils
					.SimpleAcknowledgingTaskManager.class));

			final Instance instance = getInstance(tm);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getExecutionVertex(new JobVertexID());
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.DEFAULT_TIMEOUT());
			
			Scheduler scheduler = mock(Scheduler.class);
			when(scheduler.scheduleImmediately(Matchers.any(ScheduledUnit.class))).thenReturn(slot);
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			// try to deploy to the slot
			vertex.scheduleForExecution(scheduler, false);
			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			TestingUtils.setGlobalExecutionContext();
		}
	}
}
