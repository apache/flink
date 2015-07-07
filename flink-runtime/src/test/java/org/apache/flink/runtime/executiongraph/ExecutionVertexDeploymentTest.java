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

import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.TaskMessages.TaskOperationResult;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.junit.Test;

public class ExecutionVertexDeploymentTest {

	@Test
	public void testDeployCall() {
		try {
			final JobVertexID jid = new JobVertexID();

			final ExecutionJobVertex ejv = getExecutionVertex(jid);

			// mock taskmanager to simply accept the call
			Instance instance = getInstance(
					new SimpleInstanceGateway(TestingUtils.directExecutionContext()));
			final SimpleSlot slot = instance.allocateSimpleSlot(ejv.getJobId());

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {
				// as expected
			}

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

			final ExecutionJobVertex ejv = getExecutionVertex(jid, TestingUtils.directExecutionContext());

			final Instance instance = getInstance(
					new SimpleInstanceGateway(TestingUtils.directExecutionContext()));
			final SimpleSlot slot = instance.allocateSimpleSlot(ejv.getJobId());

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			vertex.deployToSlot(slot);

			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {
				// as expected
			}

			assertNull(vertex.getFailureCause());

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.RUNNING) == 0);
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
			final ExecutionJobVertex ejv = getExecutionVertex(jid);

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			final Instance instance = getInstance(
					new SimpleInstanceGateway(TestingUtils.defaultExecutionContext()));
			final SimpleSlot slot = instance.allocateSimpleSlot(ejv.getJobId());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			vertex.deployToSlot(slot);

			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {
				// as expected
			}

			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			// no repeated scheduling
			try {
				vertex.deployToSlot(slot);
				fail("Scheduled from wrong state");
			}
			catch (IllegalStateException e) {
				// as expected
			}

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.RUNNING) == 0);
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
			final ExecutionJobVertex ejv = getExecutionVertex(jid, TestingUtils.directExecutionContext());

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			final Instance instance = getInstance(
					new SimpleFailingInstanceGateway(TestingUtils.directExecutionContext()));
			final SimpleSlot slot = instance.allocateSimpleSlot(ejv.getJobId());

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
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid);
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			final Instance instance = getInstance(
					new SimpleFailingInstanceGateway(TestingUtils.directExecutionContext()));
			final SimpleSlot slot = instance.allocateSimpleSlot(ejv.getJobId());

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

			final TestingUtils.QueuedActionExecutionContext ec = TestingUtils.queuedActionExecutionContext();
			final TestingUtils.ActionQueue queue = ec.actionQueue();

			final ExecutionJobVertex ejv = getExecutionVertex(jid, ec);

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			final Instance instance = getInstance(new SimpleInstanceGateway(TestingUtils.directExecutionContext()));
			final SimpleSlot slot = instance.allocateSimpleSlot(ejv.getJobId());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());
			vertex.deployToSlot(slot);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			Exception testError = new Exception("test error");
			vertex.fail(testError);

			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
			assertEquals(testError, vertex.getFailureCause());

			queue.triggerNextAction();
			queue.triggerNextAction();

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
			final TestingUtils.QueuedActionExecutionContext context = TestingUtils.queuedActionExecutionContext();
			final TestingUtils.ActionQueue queue = context.actionQueue();

			final JobVertexID jid = new JobVertexID();

			final ExecutionJobVertex ejv = getExecutionVertex(jid, context);
			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			final ExecutionAttemptID eid = vertex.getCurrentExecutionAttempt().getAttemptId();

			final Instance instance = getInstance(
					new ExecutionVertexCancelTest.CancelSequenceInstanceGateway(
							context,
							new TaskOperationResult(eid, false),
							new TaskOperationResult(eid, true)));

			final SimpleSlot slot = instance.allocateSimpleSlot(ejv.getJobId());

			assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

			vertex.deployToSlot(slot);
			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			Exception testError = new Exception("test error");
			vertex.fail(testError);

			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());

			// cancel call overtakes deploy call
			Runnable deploy = queue.popNextAction();
			Runnable cancel1 = queue.popNextAction();

			cancel1.run();
			// execute onComplete callback of cancel
			queue.triggerNextAction();

			deploy.run();

			// execute onComplete callback of deploy
			queue.triggerNextAction();

			assertEquals(ExecutionState.FAILED, vertex.getExecutionState());

			assertEquals(testError, vertex.getFailureCause());

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.DEPLOYING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.FAILED) > 0);

			assertTrue(queue.isEmpty());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}