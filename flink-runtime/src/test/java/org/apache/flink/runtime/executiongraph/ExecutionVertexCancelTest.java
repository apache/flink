/**
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

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState2;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler;
import org.apache.flink.runtime.protocols.TaskOperationProtocol;
import org.apache.flink.runtime.taskmanager.TaskOperationResult;
import org.apache.flink.util.LogUtils;
import org.apache.log4j.Level;
import org.junit.Test;
import org.mockito.Matchers;

public class ExecutionVertexCancelTest {

	// --------------------------------------------------------------------------------------------
	//  Canceling in different states
	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testCancelFromCreated() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getJobVertexNotExecuting(jid);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
			
			assertEquals(ExecutionState2.CREATED, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState2.CANCELED, vertex.getExecutionState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCancelFromScheduled() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getJobVertexNotExecuting(jid);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
			
			setVertexState(vertex, ExecutionState2.SCHEDULED);
			assertEquals(ExecutionState2.SCHEDULED, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState2.CANCELED, vertex.getExecutionState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCancelConcurrentlyToDeploying_CallsNotOvertaking() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ActionQueue actions = new ActionQueue();
			
			final ExecutionJobVertex ejv = getJobVertexExecutingTriggered(jid, actions);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
			
			setVertexState(vertex, ExecutionState2.SCHEDULED);
			assertEquals(ExecutionState2.SCHEDULED, vertex.getExecutionState());
			
			// task manager mock
			TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(jid, 0, true));
			when(taskManager.cancelTask(jid, 0)).thenReturn(new TaskOperationResult(jid, 0, true), new TaskOperationResult(jid, 0, false));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			vertex.deployToSlot(slot);
			
			assertEquals(ExecutionState2.DEPLOYING, vertex.getExecutionState());
			
			vertex.cancel();
			 
			assertEquals(ExecutionState2.CANCELING, vertex.getExecutionState());
			
			verify(taskManager, times(0)).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
			verify(taskManager, times(0)).cancelTask(jid, 0);

			// first action happens (deploy)
			actions.triggerNextAction();
			verify(taskManager, times(1)).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
			
			// the deploy call found itself in canceling after it returned and needs to send a cancel call
			// the call did not yet execute, so it is still in canceling
			assertEquals(ExecutionState2.CANCELING, vertex.getExecutionState());
			
			// second action happens (cancel call from cancel function)
			actions.triggerNextAction();
			
			// should properly set state to cancelled
			assertEquals(ExecutionState2.CANCELED, vertex.getExecutionState());
			
			// trigger the correction canceling call
			actions.triggerNextAction();
			assertEquals(ExecutionState2.CANCELED, vertex.getExecutionState());
			
			verify(taskManager, times(2)).cancelTask(jid, 0);
			
			assertTrue(slot.isReleased());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCancelConcurrentlyToDeploying_CallsOvertaking() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ActionQueue actions = new ActionQueue();
			
			final ExecutionJobVertex ejv = getJobVertexExecutingTriggered(jid, actions);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
			
			setVertexState(vertex, ExecutionState2.SCHEDULED);
			assertEquals(ExecutionState2.SCHEDULED, vertex.getExecutionState());
			
			// task manager mock
			TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(jid, 0, true));
			
			// first return NOT SUCCESS (task not found, cancel call overtook deploy call), then success (cancel call after deploy call)
			when(taskManager.cancelTask(jid, 0)).thenReturn(new TaskOperationResult(jid, 0, false), new TaskOperationResult(jid, 0, true));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			vertex.deployToSlot(slot);
			
			assertEquals(ExecutionState2.DEPLOYING, vertex.getExecutionState());
			
			vertex.cancel();
			 
			assertEquals(ExecutionState2.CANCELING, vertex.getExecutionState());
			
			verify(taskManager, times(0)).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
			verify(taskManager, times(0)).cancelTask(jid, 0);

			// first action happens (deploy)
			Runnable deployAction = actions.popNextAction();
			Runnable cancelAction = actions.popNextAction();
			
			// cancel call first
			cancelAction.run();
			
			// did not find the task, not properly cancelled, stay in canceling
			assertEquals(ExecutionState2.CANCELING, vertex.getExecutionState());
			
			// deploy action next
			deployAction.run();
			
			verify(taskManager, times(1)).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
			
			// the deploy call found itself in canceling after it returned and needs to send a cancel call
			// the call did not yet execute, so it is still in canceling
			assertEquals(ExecutionState2.CANCELING, vertex.getExecutionState());
			
			// trigger the correcting cancel call, should properly set state to cancelled
			actions.triggerNextAction();
			assertEquals(ExecutionState2.CANCELED, vertex.getExecutionState());
			
			verify(taskManager, times(2)).cancelTask(jid, 0);
			
			assertTrue(slot.isReleased());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCancelFromRunning() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getJobVertexExecutingSynchronously(jid);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);

			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.cancelTask(jid, 0)).thenReturn(new TaskOperationResult(jid, 0, true));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());

			setVertexState(vertex, ExecutionState2.RUNNING);
			setVertexResource(vertex, slot);
			
			assertEquals(ExecutionState2.RUNNING, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState2.CANCELED, vertex.getExecutionState());
			
			verify(taskManager).cancelTask(jid, 0);
			
			assertTrue(slot.isReleased());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testRepeatedCancelFromRunning() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ActionQueue actions = new ActionQueue();
			final ExecutionJobVertex ejv = getJobVertexExecutingTriggered(jid, actions);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);

			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.cancelTask(jid, 0)).thenReturn(new TaskOperationResult(jid, 0, true));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());

			setVertexState(vertex, ExecutionState2.RUNNING);
			setVertexResource(vertex, slot);
			
			assertEquals(ExecutionState2.RUNNING, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState2.CANCELING, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState2.CANCELING, vertex.getExecutionState());
			
			actions.triggerNextAction();
			
			assertEquals(ExecutionState2.CANCELED, vertex.getExecutionState());
			
			try {
				actions.triggerNextAction();
				fail("Too many calls sent.");
			} catch (NoSuchElementException e) {}
			
			assertTrue(slot.isReleased());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCancelFromRunningDidNotFindTask() {
		// this may happen when the task finished or failed while the call was in progress
		
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getJobVertexExecutingSynchronously(jid);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);

			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.cancelTask(jid, 0)).thenReturn(new TaskOperationResult(jid, 0, false));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());

			setVertexState(vertex, ExecutionState2.RUNNING);
			setVertexResource(vertex, slot);
			
			assertEquals(ExecutionState2.RUNNING, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState2.CANCELING, vertex.getExecutionState());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCancelCallFails() {
		// this may happen when the task finished or failed while the call was in progress
		LogUtils.initializeDefaultConsoleLogger(Level.OFF);
		
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getJobVertexExecutingSynchronously(jid);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);

			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.cancelTask(jid, 0)).thenThrow(new IOException("RPC call failed"));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());

			setVertexState(vertex, ExecutionState2.RUNNING);
			setVertexResource(vertex, slot);
			
			assertEquals(ExecutionState2.RUNNING, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState2.FAILED, vertex.getExecutionState());
			
			assertTrue(slot.isReleased());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Actions after a vertex has been canceled or while canceling
	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testScheduleOrDeployAfterCancel() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getJobVertexNotExecuting(jid);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
			setVertexState(vertex, ExecutionState2.CANCELED);
			
			assertEquals(ExecutionState2.CANCELED, vertex.getExecutionState());
			
			// 1)
			// scheduling after being created should be tolerated (no exception) because
			// it can occur as the result of races
			{
				DefaultScheduler scheduler = mock(DefaultScheduler.class);
				vertex.scheduleForExecution(scheduler);
				
				assertEquals(ExecutionState2.CANCELED, vertex.getExecutionState());
			}
			
			// 2)
			// deploying after canceling from CREATED needs to raise an exception, because
			// the scheduler (or any caller) needs to know that the slot should be released
			try {
				TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
				Instance instance = getInstance(taskManager);
				AllocatedSlot slot = instance.allocateSlot(new JobID());
				
				vertex.deployToSlot(slot);
				fail("Method should throw an exception");
			}
			catch (IllegalStateException e) {
				assertEquals(ExecutionState2.CANCELED, vertex.getExecutionState());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testActionsWhileCancelling() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getJobVertexNotExecuting(jid);
			
			// scheduling while canceling is an illegal state transition
			try {
				ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
				setVertexState(vertex, ExecutionState2.CANCELING);
				
				DefaultScheduler scheduler = mock(DefaultScheduler.class);
				vertex.scheduleForExecution(scheduler);
				fail("Method should throw an exception");
			}
			catch (IllegalStateException e) {}
			
			
			// deploying while in canceling state is illegal (should immediately go to canceled)
			try {
				ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
				setVertexState(vertex, ExecutionState2.CANCELING);
				
				TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
				Instance instance = getInstance(taskManager);
				AllocatedSlot slot = instance.allocateSlot(new JobID());
				
				vertex.deployToSlot(slot);
				fail("Method should throw an exception");
			}
			catch (IllegalStateException e) {}
			
			
			// fail while canceling
			{
				ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
				
				TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
				Instance instance = getInstance(taskManager);
				AllocatedSlot slot = instance.allocateSlot(new JobID());
				
				setVertexResource(vertex, slot);
				setVertexState(vertex, ExecutionState2.CANCELING);
				
				Exception failureCause = new Exception("test exception");
				
				vertex.fail(failureCause);
				assertEquals(ExecutionState2.FAILED, vertex.getExecutionState());
				assertEquals(failureCause, vertex.getFailureCause());
				
				assertTrue(slot.isReleased());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
