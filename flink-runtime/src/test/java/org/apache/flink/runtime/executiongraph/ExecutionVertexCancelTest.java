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

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.protocols.TaskOperationProtocol;
import org.apache.flink.runtime.taskmanager.TaskOperationResult;

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
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());
			
			assertNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELED) > 0);
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
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			
			setVertexState(vertex, ExecutionState.SCHEDULED);
			assertEquals(ExecutionState.SCHEDULED, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());
			
			assertNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELED) > 0);
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
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			final ExecutionAttemptID execId = vertex.getCurrentExecutionAttempt().getAttemptId();
			
			setVertexState(vertex, ExecutionState.SCHEDULED);
			assertEquals(ExecutionState.SCHEDULED, vertex.getExecutionState());
			
			// task manager mock
			TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(execId, true));
			when(taskManager.cancelTask(execId)).thenReturn(new TaskOperationResult(execId, true), new TaskOperationResult(execId, false));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			vertex.deployToSlot(slot);
			
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());
			
			vertex.cancel();
			 
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());
			
			verify(taskManager, times(0)).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
			verify(taskManager, times(0)).cancelTask(execId);

			// first action happens (deploy)
			actions.triggerNextAction();
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());
			verify(taskManager, times(1)).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
			
			// the deploy call found itself in canceling after it returned and needs to send a cancel call
			// the call did not yet execute, so it is still in canceling
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());
			
			// second action happens (cancel call from cancel function)
			actions.triggerNextAction();
			
			// TaskManager reports back (canceling done)
			vertex.getCurrentExecutionAttempt().cancelingComplete();
			
			// should properly set state to cancelled
			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());
			
			// trigger the correction canceling call
			actions.triggerNextAction();
			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());
			
			verify(taskManager, times(2)).cancelTask(execId);
			
			assertTrue(slot.isReleased());
			
			assertNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELED) > 0);
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
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			final ExecutionAttemptID execId = vertex.getCurrentExecutionAttempt().getAttemptId();
			
			setVertexState(vertex, ExecutionState.SCHEDULED);
			assertEquals(ExecutionState.SCHEDULED, vertex.getExecutionState());
			
			// task manager mock
			TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(execId, true));
			
			// first return NOT SUCCESS (task not found, cancel call overtook deploy call), then success (cancel call after deploy call)
			when(taskManager.cancelTask(execId)).thenReturn(new TaskOperationResult(execId, false), new TaskOperationResult(execId, true));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			vertex.deployToSlot(slot);
			
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());
			
			vertex.cancel();
			 
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());
			
			verify(taskManager, times(0)).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
			verify(taskManager, times(0)).cancelTask(execId);

			// first action happens (deploy)
			Runnable deployAction = actions.popNextAction();
			Runnable cancelAction = actions.popNextAction();
			
			// cancel call first
			cancelAction.run();
			
			// did not find the task, not properly cancelled, stay in canceling
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());
			
			// deploy action next
			deployAction.run();
			
			verify(taskManager, times(1)).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
			
			// the deploy call found itself in canceling after it returned and needs to send a cancel call
			// the call did not yet execute, so it is still in canceling
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());
			
			// trigger the correcting cancel call, should properly set state to cancelled
			actions.triggerNextAction();
			vertex.getCurrentExecutionAttempt().cancelingComplete();
			
			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());
			
			verify(taskManager, times(2)).cancelTask(execId);
			
			assertTrue(slot.isReleased());
			
			assertNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELED) > 0);
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
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			final ExecutionAttemptID execId = vertex.getCurrentExecutionAttempt().getAttemptId();

			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.cancelTask(execId)).thenReturn(new TaskOperationResult(execId, true));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());

			setVertexState(vertex, ExecutionState.RUNNING);
			setVertexResource(vertex, slot);
			
			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());
			
			vertex.cancel();
			vertex.getCurrentExecutionAttempt().cancelingComplete(); // responce by task manager once actially canceled
			
			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());
			
			verify(taskManager).cancelTask(execId);
			
			assertTrue(slot.isReleased());
			
			assertNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELED) > 0);
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
			final ExecutionJobVertex ejv = getJobVertexExecutingSynchronously(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			final ExecutionAttemptID execId = vertex.getCurrentExecutionAttempt().getAttemptId();

			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.cancelTask(execId)).thenReturn(new TaskOperationResult(execId, true));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());

			setVertexState(vertex, ExecutionState.RUNNING);
			setVertexResource(vertex, slot);
			
			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());
			
			// callback by TaskManager after canceling completes
			vertex.getCurrentExecutionAttempt().cancelingComplete();
			
			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());
			
			// check that we did not overdo our cancel calls
			verify(taskManager, times(1)).cancelTask(execId);
			
			assertTrue(slot.isReleased());
			
			assertNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELED) > 0);
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
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			final ExecutionAttemptID execId = vertex.getCurrentExecutionAttempt().getAttemptId();

			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.cancelTask(execId)).thenReturn(new TaskOperationResult(execId, false));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());

			setVertexState(vertex, ExecutionState.RUNNING);
			setVertexResource(vertex, slot);
			
			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());
			
			assertNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCancelCallFails() {
		
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getJobVertexExecutingSynchronously(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			final ExecutionAttemptID execId = vertex.getCurrentExecutionAttempt().getAttemptId();

			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.cancelTask(execId)).thenThrow(new IOException("RPC call failed"));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());

			setVertexState(vertex, ExecutionState.RUNNING);
			setVertexResource(vertex, slot);
			
			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			
			assertTrue(slot.isReleased());
			
			assertNotNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSendCancelAndReceiveFail() {
		
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getJobVertexNotExecuting(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			final ExecutionAttemptID execId = vertex.getCurrentExecutionAttempt().getAttemptId();

			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.cancelTask(execId)).thenThrow(new IOException("RPC call failed"));
			
			Instance instance = getInstance(taskManager);
			AllocatedSlot slot = instance.allocateSlot(new JobID());

			setVertexState(vertex, ExecutionState.RUNNING);
			setVertexResource(vertex, slot);
			
			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());
			
			vertex.cancel();
			
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());
			
			vertex.getCurrentExecutionAttempt().markFailed(new Throwable("test"));
			
			assertTrue(vertex.getExecutionState() == ExecutionState.CANCELED || vertex.getExecutionState() == ExecutionState.FAILED);
			
			assertTrue(slot.isReleased());
			
			assertEquals(0, vertex.getExecutionGraph().getRegisteredExecutions().size());
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
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			setVertexState(vertex, ExecutionState.CANCELED);
			
			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());
			
			// 1)
			// scheduling after being created should be tolerated (no exception) because
			// it can occur as the result of races
			{
				Scheduler scheduler = mock(Scheduler.class);
				vertex.scheduleForExecution(scheduler, false);
				
				assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());
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
				assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());
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
				ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
				setVertexState(vertex, ExecutionState.CANCELING);
				
				Scheduler scheduler = mock(Scheduler.class);
				vertex.scheduleForExecution(scheduler, false);
			}
			catch (Exception e) {
				fail("should not throw an exception");
			}
			
			
			// deploying while in canceling state is illegal (should immediately go to canceled)
			try {
				ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
				setVertexState(vertex, ExecutionState.CANCELING);
				
				TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
				Instance instance = getInstance(taskManager);
				AllocatedSlot slot = instance.allocateSlot(new JobID());
				
				vertex.deployToSlot(slot);
				fail("Method should throw an exception");
			}
			catch (IllegalStateException e) {}
			
			
			// fail while canceling
			{
				ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
				
				TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
				Instance instance = getInstance(taskManager);
				AllocatedSlot slot = instance.allocateSlot(new JobID());
				
				setVertexResource(vertex, slot);
				setVertexState(vertex, ExecutionState.CANCELING);
				
				Exception failureCause = new Exception("test exception");
				
				vertex.fail(failureCause);
				assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
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
