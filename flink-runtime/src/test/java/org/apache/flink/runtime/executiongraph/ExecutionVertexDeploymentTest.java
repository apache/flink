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

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState2;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.protocols.TaskOperationProtocol;
import org.apache.flink.runtime.taskmanager.TaskOperationResult;
import org.junit.Test;
import org.mockito.Matchers;

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
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
			
			assertEquals(ExecutionState2.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			assertEquals(ExecutionState2.DEPLOYING, vertex.getExecutionState());
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
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
			TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(jid, 0, true));
			
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexExecutingSynchronously(jid);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
			
			assertEquals(ExecutionState2.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			assertEquals(ExecutionState2.RUNNING, vertex.getExecutionState());
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
			
			verify(taskManager).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
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
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(jid, 0, true));
			
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexExecutingAsynchronously(jid);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
			
			assertEquals(ExecutionState2.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
			
			// wait until the state transition must be done
			for (int i = 0; i < 100; i++) {
				if (vertex.getExecutionState() != ExecutionState2.RUNNING) {
					Thread.sleep(10);
				}
			}
			
			assertEquals(ExecutionState2.RUNNING, vertex.getExecutionState());
			
			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {}
			
			verify(taskManager).submitTask(Matchers.any(TaskDeploymentDescriptor.class));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testDeployFailedSynchronous() {
		try {
			final JobVertexID jid = new JobVertexID();
			
			// mock taskmanager to simply accept the call
			TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(jid, 0, false, "failed"));
			
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexExecutingSynchronously(jid);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
			
			assertEquals(ExecutionState2.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			
			assertEquals(ExecutionState2.FAILED, vertex.getExecutionState());
			assertNotNull(vertex.getFailureCause());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testDeployFailedAsynchronously() {
		try {
			final JobVertexID jid = new JobVertexID();
			
			// mock taskmanager to simply accept the call
			TaskOperationProtocol taskManager = mock(TaskOperationProtocol.class);
			when(taskManager.submitTask(Matchers.any(TaskDeploymentDescriptor.class))).thenReturn(new TaskOperationResult(jid, 0, false, "failed"));
			
			final Instance instance = getInstance(taskManager);
			final AllocatedSlot slot = instance.allocateSlot(new JobID());
			
			final ExecutionJobVertex ejv = getJobVertexExecutingAsynchronously(jid);
			
			final ExecutionVertex2 vertex = new ExecutionVertex2(ejv, 0, new IntermediateResult[0]);
			
			assertEquals(ExecutionState2.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			
			// wait until the state transition must be done
			for (int i = 0; i < 100; i++) {
				if (vertex.getExecutionState() != ExecutionState2.FAILED) {
					Thread.sleep(10);
				}
			}
			
			assertEquals(ExecutionState2.FAILED, vertex.getExecutionState());
			assertNotNull(vertex.getFailureCause());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
