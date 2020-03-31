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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.getExecutionVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.setVertexResource;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.setVertexState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for cancelling {@link ExecutionVertex ExecutionVertices}.
 */
public class ExecutionVertexCancelTest extends TestLogger {

	// --------------------------------------------------------------------------------------------
	//  Canceling in different states
	// --------------------------------------------------------------------------------------------

	@Test
	public void testCancelFromCreated() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid);

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

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
			final ExecutionJobVertex ejv = getExecutionVertex(jid);

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

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
	public void testCancelFromRunning() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid, new DirectScheduledExecutorService());

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			LogicalSlot slot = new TestingLogicalSlotBuilder().setTaskManagerGateway(new CancelSequenceSimpleAckingTaskManagerGateway(1)).createTestingLogicalSlot();

			setVertexResource(vertex, slot);
			setVertexState(vertex, ExecutionState.RUNNING);

			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());

			vertex.cancel();
			vertex.getCurrentExecutionAttempt().completeCancelling(); // response by task manager once actually canceled

			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());

			assertFalse(slot.isAlive());

			assertNull(vertex.getFailureCause());

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELED) > 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRepeatedCancelFromRunning() {
		try {

			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid, new DirectScheduledExecutorService());

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			LogicalSlot slot = new TestingLogicalSlotBuilder().setTaskManagerGateway(new CancelSequenceSimpleAckingTaskManagerGateway(1)).createTestingLogicalSlot();

			setVertexResource(vertex, slot);
			setVertexState(vertex, ExecutionState.RUNNING);

			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());

			vertex.cancel();

			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());

			vertex.cancel();

			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());

			// callback by TaskManager after canceling completes
			vertex.getCurrentExecutionAttempt().completeCancelling();

			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());

			assertFalse(slot.isAlive());

			assertNull(vertex.getFailureCause());

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELED) > 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCancelFromRunningDidNotFindTask() {
		// this may happen when the task finished or failed while the call was in progress
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid, new DirectScheduledExecutorService());

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			LogicalSlot slot = new TestingLogicalSlotBuilder().setTaskManagerGateway(new CancelSequenceSimpleAckingTaskManagerGateway(1)).createTestingLogicalSlot();

			setVertexResource(vertex, slot);
			setVertexState(vertex, ExecutionState.RUNNING);

			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());

			vertex.cancel();

			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());

			assertNull(vertex.getFailureCause());

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCancelCallFails() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid, new DirectScheduledExecutorService());

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			LogicalSlot slot = new TestingLogicalSlotBuilder().setTaskManagerGateway(new CancelSequenceSimpleAckingTaskManagerGateway(0)).createTestingLogicalSlot();

			setVertexResource(vertex, slot);
			setVertexState(vertex, ExecutionState.RUNNING);

			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());

			vertex.cancel();

			// Callback fails, leading to CANCELED
			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());

			assertFalse(slot.isAlive());

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSendCancelAndReceiveFail() throws Exception {
		final ExecutionGraph graph = ExecutionGraphTestUtils.createSimpleTestGraph();

		graph.scheduleForExecution();
		ExecutionGraphTestUtils.switchAllVerticesToRunning(graph);
		assertEquals(JobStatus.RUNNING, graph.getState());

		final ExecutionVertex[] vertices = graph.getVerticesTopologically().iterator().next().getTaskVertices();
		assertEquals(vertices.length, graph.getRegisteredExecutions().size());

		final Execution exec = vertices[3].getCurrentExecutionAttempt();
		exec.cancel();
		assertEquals(ExecutionState.CANCELING, exec.getState());

		exec.markFailed(new Exception("test"));
		assertTrue(exec.getState() == ExecutionState.FAILED || exec.getState() == ExecutionState.CANCELED);

		assertFalse(exec.getAssignedResource().isAlive());
		assertEquals(vertices.length - 1, exec.getVertex().getExecutionGraph().getRegisteredExecutions().size());
	}

	// --------------------------------------------------------------------------------------------
	//  Actions after a vertex has been canceled or while canceling
	// --------------------------------------------------------------------------------------------

	@Test
	public void testScheduleOrDeployAfterCancel() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid);

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());
			setVertexState(vertex, ExecutionState.CANCELED);

			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());

			// 1)
			// scheduling after being canceled should be tolerated (no exception) because
			// it can occur as the result of races
			{
				vertex.scheduleForExecution(
					TestingSlotProviderStrategy.from(new ProgrammedSlotProvider(1)),
					LocationPreferenceConstraint.ALL,
					Collections.emptySet());

				assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());
			}

			// 2)
			// deploying after canceling from CREATED needs to raise an exception, because
			// the scheduler (or any caller) needs to know that the slot should be released
			try {

				final LogicalSlot slot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();

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
			final ExecutionJobVertex ejv = getExecutionVertex(jid);

			// scheduling while canceling is an illegal state transition
			try {
				ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
						AkkaUtils.getDefaultTimeout());
				setVertexState(vertex, ExecutionState.CANCELING);
				vertex.scheduleForExecution(
					TestingSlotProviderStrategy.from(new ProgrammedSlotProvider(1)),
					LocationPreferenceConstraint.ALL,
					Collections.emptySet());
			}
			catch (Exception e) {
				fail("should not throw an exception");
			}

			// deploying while in canceling state is illegal (should immediately go to canceled)
			try {
				ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
						AkkaUtils.getDefaultTimeout());
				setVertexState(vertex, ExecutionState.CANCELING);

				final LogicalSlot slot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();

				vertex.deployToSlot(slot);
				fail("Method should throw an exception");
			}
			catch (IllegalStateException e) {
				// that is what we expect
			}

			// fail while canceling
			{
				ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
						AkkaUtils.getDefaultTimeout());

				final LogicalSlot slot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();

				setVertexResource(vertex, slot);
				setVertexState(vertex, ExecutionState.CANCELING);

				Exception failureCause = new Exception("test exception");

				vertex.fail(failureCause);
				assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());

				assertFalse(slot.isAlive());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static class CancelSequenceSimpleAckingTaskManagerGateway extends SimpleAckingTaskManagerGateway {
		private final int successfulOperations;
		private int index = -1;

		public CancelSequenceSimpleAckingTaskManagerGateway(int successfulOperations) {
			super();
			this.successfulOperations = successfulOperations;
		}

		@Override
		public CompletableFuture<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptID, Time timeout) {
			index++;

			if (index >= successfulOperations) {
				return FutureUtils.completedExceptionally(new IOException("Rpc call fails"));
			} else {
				return CompletableFuture.completedFuture(Acknowledge.get());
			}
		}
	}
}
