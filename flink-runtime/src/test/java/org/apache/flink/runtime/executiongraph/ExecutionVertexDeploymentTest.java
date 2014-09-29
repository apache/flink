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
import static org.mockito.Matchers.any;

import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.protocols.TaskOperationProtocol;
import org.apache.flink.runtime.taskmanager.TaskOperationResult;

import org.junit.Test;

import org.mockito.Matchers;

import java.util.ArrayList;

public class ExecutionVertexDeploymentTest {
	
	@Test
	public void testDeployCall() {
		try {
			final JobVertexID jid = new JobVertexID();
			
			// mock taskmanager to simply accept the call
			TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexNotExecuting(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
			
			assertNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testDeployWithSynchronousAnswer() {
		try {
			final JobVertexID jid = new JobVertexID();
			
			// mock taskmanager to simply accept the call
			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexExecutingSynchronously(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(vertex.getCurrentExecutionAttempt().getAttemptId(), true));
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			
			vertex.deployToSlot(slot);
			
			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
			
			verify(taskManager).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
			
			assertNull(vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.RUNNING) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testDeployWithAsynchronousAnswer() {
		try {
			final JobVertexID jid = new JobVertexID();
			
			// mock taskmanager to simply accept the call
			TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			
			
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexExecutingAsynchronously(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(vertex.getCurrentExecutionAttempt().getAttemptId(), true));
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			
			vertex.deployToSlot(slot);
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
			
			// wait until the state transition must be done
			for (int i = 0; i < 100; i++) {
				if (vertex.getExecutionState() != ExecutionState.RUNNING) {
					Thread.sleep(10);
				}
			}
			
			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
			
			verify(taskManager).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.RUNNING) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testDeployFailedSynchronous() {
		final String ERROR_MESSAGE = "test_failure_error_message";
		
		try {
			final JobVertexID jid = new JobVertexID();
			
			// mock taskmanager to simply accept the call
			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexExecutingSynchronously(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(vertex.getCurrentExecutionAttempt().getAttemptId(), false, ERROR_MESSAGE));
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			
			vertex.deployToSlot(slot);
			
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertNotNull(vertex.getFailureCause());
			assertTrue(vertex.getFailureCause().getMessage().contains(ERROR_MESSAGE));
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testDeployFailedAsynchronously() {
		final String ERROR_MESSAGE = "test_failure_error_message";
		
		try {
			final JobVertexID jid = new JobVertexID();
			
			// mock taskmanager to simply accept the call
			final TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexExecutingAsynchronously(jid);
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(vertex.getCurrentExecutionAttempt().getAttemptId(), false, ERROR_MESSAGE));
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			
			vertex.deployToSlot(slot);
			
			// wait until the state transition must be done
			for (int i = 0; i < 100; i++) {
				if (vertex.getExecutionState() == ExecutionState.FAILED && vertex.getFailureCause() != null) {
					break;
				} else {
					Thread.sleep(10);
				}
			}
			
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertNotNull(vertex.getFailureCause());
			assertTrue(vertex.getFailureCause().getMessage().contains(ERROR_MESSAGE));
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testFailExternallyDuringDeploy() {
		
		try {
			final JobVertexID jid = new JobVertexID();
			
			// mock taskmanager to simply accept the call
			TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexNotExecuting(jid);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());
			
			Exception testError = new Exception("test error");
			vertex.fail(testError);
			
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertEquals(testError, vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testFailCallOvertakesDeploymentAnswer() {
		
		try {
			final ActionQueue queue = new ActionQueue();
			final JobVertexID jid = new JobVertexID();
			
			// mock taskmanager to simply accept the call
			TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexExecutingTriggered(jid, queue);
			
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0]);
			final ExecutionAttemptID eid = vertex.getCurrentExecutionAttempt().getAttemptId();
			
			// the deployment call succeeds regularly
			when(taskManager.submitTask(any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(eid, true));
			
			// first cancel call does not find a task, second one finds it
			when(taskManager.cancelTask(any(ExecutionAttemptID.class))).thenReturn(
					new TaskOperationResult(eid, false), new TaskOperationResult(eid, true));
			
			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			
			vertex.deployToSlot(slot);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());
			
			Exception testError = new Exception("test error");
			vertex.fail(testError);
			
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			
			// now the deploy call returns
			Runnable deploy = queue.popNextAction();
			Runnable cancel1 = queue.popNextAction();
			
			// cancel call overtakes
			cancel1.run();
			deploy.run();
			
			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			
			// should have sent another cancel call
			queue.triggerNextAction();
			
			assertEquals(testError, vertex.getFailureCause());
			
			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);
			
			// should have received two cancel calls
			verify(taskManager, times(2)).cancelTask(eid);
			verify(taskManager, times(1)).submitTask(any(TaskDeploymentDescriptor.class));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
