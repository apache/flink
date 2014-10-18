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

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getInstance;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getJobVertexNotExecuting;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.SlotAllocationFuture;
import org.apache.flink.runtime.protocols.TaskOperationProtocol;

import org.junit.Test;

import org.mockito.Matchers;

public class ExecutionVertexSchedulingTest {
	
	@Test
	public void testSlotReleasedWhenScheduledImmediately() {
		
		try {
			// a slot than cannot be deployed to
			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			slot.cancel();
			assertFalse(slot.isReleased());
			
			final ExecutionJobVertex ejv = getJobVertexNotExecuting(new JobVertexID());
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			
			Scheduler scheduler = mock(Scheduler.class);
			when(scheduler.scheduleImmediately(Matchers.any(ScheduledUnit.class))).thenReturn(slot);
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			// try to deploy to the slot
			vertex.scheduleForExecution(scheduler, false);
			
			// will have failed
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertTrue(slot.isReleased());
			
			verify(taskManager, times(0)).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
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
			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			slot.cancel();
			assertFalse(slot.isReleased());
			
			final SlotAllocationFuture future = new SlotAllocationFuture();
			
			final ExecutionJobVertex ejv = getJobVertexNotExecuting(new JobVertexID());
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			
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
			
			verify(taskManager, times(0)).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testScheduleToDeploy() {
		try {
			// a slot than cannot be deployed to
			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexNotExecuting(new JobVertexID());
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			
			Scheduler scheduler = mock(Scheduler.class);
			when(scheduler.scheduleImmediately(Matchers.any(ScheduledUnit.class))).thenReturn(slot);
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			// try to deploy to the slot
			vertex.scheduleForExecution(scheduler, false);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
