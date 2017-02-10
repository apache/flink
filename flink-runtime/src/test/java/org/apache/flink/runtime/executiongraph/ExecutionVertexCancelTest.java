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

import java.io.IOException;

import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.BaseTestingActorGateway;
import org.apache.flink.runtime.instance.DummyActorGateway;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskMessages.SubmitTask;
import org.apache.flink.runtime.messages.TaskMessages.CancelTask;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;

import org.junit.Test;

import scala.concurrent.ExecutionContext;

@SuppressWarnings("serial")
public class ExecutionVertexCancelTest {

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
	public void testCancelConcurrentlyToDeploying_CallsNotOvertaking() {
		try {
			final JobVertexID jid = new JobVertexID();

			final TestingUtils.QueuedActionExecutionContext executionContext = TestingUtils.queuedActionExecutionContext();
			final TestingUtils.ActionQueue actions = executionContext.actionQueue();


			final ExecutionJobVertex ejv = getExecutionVertex(
				jid,
				executionContext
			);

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			setVertexState(vertex, ExecutionState.SCHEDULED);
			assertEquals(ExecutionState.SCHEDULED, vertex.getExecutionState());

			ActorGateway actorGateway = new CancelSequenceActorGateway(
					executionContext, 2);

			Instance instance = getInstance(new ActorTaskManagerGateway(actorGateway));
			SimpleSlot slot = instance.allocateSimpleSlot(new JobID());

			vertex.deployToSlot(slot);

			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			vertex.cancel();

			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());

			// first action happens (deploy)
			actions.triggerNextAction();
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());

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

			assertTrue(slot.isReleased());

			assertNull(vertex.getFailureCause());

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELED) > 0);
		}
		catch(Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testCancelConcurrentlyToDeploying_CallsOvertaking() {
		try {
			final JobVertexID jid = new JobVertexID();

			final TestingUtils.QueuedActionExecutionContext executionContext = TestingUtils.queuedActionExecutionContext();
			final TestingUtils.ActionQueue actions = executionContext.actionQueue();

			final ExecutionJobVertex ejv = getExecutionVertex(jid, executionContext);

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			setVertexState(vertex, ExecutionState.SCHEDULED);
			assertEquals(ExecutionState.SCHEDULED, vertex.getExecutionState());

			// task manager cancel sequence mock actor
			// first return NOT SUCCESS (task not found, cancel call overtook deploy call), then success (cancel call after deploy call)
			ActorGateway actorGateway = new CancelSequenceActorGateway(
					executionContext,
					2);

			Instance instance = getInstance(new ActorTaskManagerGateway(actorGateway));
			SimpleSlot slot = instance.allocateSimpleSlot(new JobID());

			vertex.deployToSlot(slot);

			assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());

			vertex.cancel();

			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());

			// first action happens (deploy)
			Runnable deployAction = actions.popNextAction();
			Runnable cancelAction = actions.popNextAction();

			// cancel call first
			cancelAction.run();
			// process onComplete callback
			actions.triggerNextAction();

			// did not find the task, not properly cancelled, stay in canceling
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());

			// deploy action next
			deployAction.run();

			// the deploy call found itself in canceling after it returned and needs to send a cancel call
			// the call did not yet execute, so it is still in canceling
			assertEquals(ExecutionState.CANCELING, vertex.getExecutionState());

			vertex.getCurrentExecutionAttempt().cancelingComplete();

			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());

			assertTrue(slot.isReleased());

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
	public void testCancelFromRunning() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid, new DirectScheduledExecutorService());

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			ActorGateway actorGateway = new CancelSequenceActorGateway(
					TestingUtils.directExecutionContext(),
					1);

			Instance instance = getInstance(new ActorTaskManagerGateway(actorGateway));
			SimpleSlot slot = instance.allocateSimpleSlot(new JobID());

			setVertexState(vertex, ExecutionState.RUNNING);
			setVertexResource(vertex, slot);

			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());

			vertex.cancel();
			vertex.getCurrentExecutionAttempt().cancelingComplete(); // response by task manager once actually canceled

			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());

			assertTrue(slot.isReleased());

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

			final ActorGateway actorGateway = new CancelSequenceActorGateway(
					TestingUtils.directExecutionContext(),
					1);

			Instance instance = getInstance(new ActorTaskManagerGateway(actorGateway));
			SimpleSlot slot = instance.allocateSimpleSlot(new JobID());

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

			assertTrue(slot.isReleased());

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

			final ActorGateway actorGateway = new CancelSequenceActorGateway(
					TestingUtils.directExecutionContext(),
					1);

			Instance instance = getInstance(new ActorTaskManagerGateway(actorGateway));
			SimpleSlot slot = instance.allocateSimpleSlot(new JobID());

			setVertexState(vertex, ExecutionState.RUNNING);
			setVertexResource(vertex, slot);

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

			final ActorGateway gateway = new CancelSequenceActorGateway(TestingUtils.directExecutionContext(), 0);

			Instance instance = getInstance(new ActorTaskManagerGateway(gateway));
			SimpleSlot slot = instance.allocateSimpleSlot(new JobID());

			setVertexState(vertex, ExecutionState.RUNNING);
			setVertexResource(vertex, slot);

			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());

			vertex.cancel();

			// Callback fails, leading to CANCELED
			assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());

			assertTrue(slot.isReleased());

			assertTrue(vertex.getStateTimestamp(ExecutionState.CREATED) > 0);
			assertTrue(vertex.getStateTimestamp(ExecutionState.CANCELING) > 0);
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSendCancelAndReceiveFail() {
		try {
			final JobVertexID jid = new JobVertexID();
			final ExecutionJobVertex ejv = getExecutionVertex(jid);

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());

			final ActorGateway gateway = new CancelSequenceActorGateway(
					TestingUtils.defaultExecutionContext(),
					1);

			Instance instance = getInstance(new ActorTaskManagerGateway(gateway));
			SimpleSlot slot = instance.allocateSimpleSlot(new JobID());

			setVertexState(vertex, ExecutionState.RUNNING);
			setVertexResource(vertex, slot);

			assertEquals(ExecutionState.RUNNING, vertex.getExecutionState());

			vertex.cancel();

			assertTrue(vertex.getExecutionState() == ExecutionState.CANCELING || vertex.getExecutionState() == ExecutionState.FAILED);

			vertex.getCurrentExecutionAttempt().markFailed(new Throwable("test"));

			assertTrue(vertex.getExecutionState() == ExecutionState.CANCELED || vertex.getExecutionState() == ExecutionState.FAILED);

			assertTrue(slot.isReleased());

			assertEquals(0, vertex.getExecutionGraph().getRegisteredExecutions().size());
		} catch (Exception e) {
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
			final ExecutionJobVertex ejv = getExecutionVertex(jid);

			final ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
					AkkaUtils.getDefaultTimeout());
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
				Instance instance = getInstance(new ActorTaskManagerGateway(DummyActorGateway.INSTANCE));
				SimpleSlot slot = instance.allocateSimpleSlot(new JobID());

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

				Scheduler scheduler = mock(Scheduler.class);
				vertex.scheduleForExecution(scheduler, false);
			}
			catch (Exception e) {
				fail("should not throw an exception");
			}


			// deploying while in canceling state is illegal (should immediately go to canceled)
			try {
				ExecutionVertex vertex = new ExecutionVertex(ejv, 0, new IntermediateResult[0],
						AkkaUtils.getDefaultTimeout());
				setVertexState(vertex, ExecutionState.CANCELING);

				Instance instance = getInstance(new ActorTaskManagerGateway(DummyActorGateway.INSTANCE));
				SimpleSlot slot = instance.allocateSimpleSlot(new JobID());

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

				Instance instance = getInstance(new ActorTaskManagerGateway(DummyActorGateway.INSTANCE));
				SimpleSlot slot = instance.allocateSimpleSlot(new JobID());

				setVertexResource(vertex, slot);
				setVertexState(vertex, ExecutionState.CANCELING);

				Exception failureCause = new Exception("test exception");

				vertex.fail(failureCause);
				assertEquals(ExecutionState.CANCELED, vertex.getExecutionState());

				assertTrue(slot.isReleased());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	public static class CancelSequenceActorGateway extends BaseTestingActorGateway {
		private final int successfulOperations;
		private int index = -1;

		public CancelSequenceActorGateway(ExecutionContext executionContext, int successfulOperations) {
			super(executionContext);
			this.successfulOperations = successfulOperations;
		}

		@Override
		public Object handleMessage(Object message) throws Exception {
			Object result;
			if(message instanceof SubmitTask) {
				result = Acknowledge.get();
			} else if(message instanceof CancelTask) {
				index++;

				if(index >= successfulOperations){
					throw new IOException("RPC call failed.");
				} else {
					result = Acknowledge.get();
				}
			} else {
				result = null;
			}

			return result;
		}
	}
}
