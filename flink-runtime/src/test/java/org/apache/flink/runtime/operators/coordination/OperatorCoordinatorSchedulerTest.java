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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskexecutor.TaskExecutorOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.core.testutils.FlinkMatchers.futureFailedWith;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the integration of the {@link OperatorCoordinator} with the scheduler, to ensure the relevant
 * actions are leading to the right method invocations on the coordinator.
 */
@SuppressWarnings("serial")
public class OperatorCoordinatorSchedulerTest extends TestLogger {

	private final JobVertexID testVertexId = new JobVertexID();
	private final OperatorID testOperatorId = new OperatorID();

	private final ManuallyTriggeredScheduledExecutorService executor = new ManuallyTriggeredScheduledExecutorService();

	// ------------------------------------------------------------------------
	//  tests
	// ------------------------------------------------------------------------

	@Test
	public void testCoordinatorStartedWhenSchedulerStarts() throws Exception {
		final DefaultScheduler scheduler = createAndStartScheduler();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		assertTrue(coordinator.isStarted());
	}

	@Test
	public void testCoordinatorDisposedWhenSchedulerStops() throws Exception {
		final DefaultScheduler scheduler = createAndStartScheduler();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		scheduler.suspend(new Exception("test suspend"));

		assertTrue(coordinator.isClosed());
	}

	@Test
	public void testFailureToStartPropagatesExceptions() throws Exception {
		final OperatorCoordinator.Provider failingCoordinatorProvider =
			new TestingOperatorCoordinator.Provider(testOperatorId, CoordinatorThatFailsInStart::new);
		final DefaultScheduler scheduler = createScheduler(failingCoordinatorProvider);

		try {
			scheduler.startScheduling();
			fail("expected an exception");
		} catch (Exception ignored) {
			// expected
		}
	}

	@Test
	public void testFailureToStartClosesCoordinator() throws Exception {
		final OperatorCoordinator.Provider failingCoordinatorProvider =
				new TestingOperatorCoordinator.Provider(testOperatorId, CoordinatorThatFailsInStart::new);
		final DefaultScheduler scheduler = createScheduler(failingCoordinatorProvider);
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		try {
			scheduler.startScheduling();
		} catch (Exception ignored) {}

		assertTrue(coordinator.isClosed());
	}

	@Test
	public void taskFailureNotifiesCoordinator() throws Exception {
		final DefaultScheduler scheduler = createAndStartScheduler();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failTask(scheduler, 1);
		executor.triggerScheduledTasks();

		assertEquals(1, coordinator.getFailedTasks().size());
		assertThat(coordinator.getFailedTasks(), contains(1));
		assertThat(coordinator.getFailedTasks(), not(contains(0)));
	}

	@Test
	public void taskRepeatedFailureNotifyCoordinator() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
		final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

		failTask(scheduler, 0);
		executor.triggerScheduledTasks();
		failTask(scheduler, 0);
		executor.triggerScheduledTasks();

		assertEquals(2, coordinator.getFailedTasks().size());
		assertThat(coordinator.getFailedTasks(), contains(0, 0));
	}

	@Test
	public void taskExceptionWhenTasksNotRunning() throws Exception {
		final DefaultScheduler scheduler = createAndStartScheduler();
		final OperatorCoordinator.Context context = getCoordinator(scheduler).getContext();

		final CompletableFuture<?> result = context.sendEvent(new TestOperatorEvent(), 0);

		assertThat(result, futureFailedWith(TaskNotRunningException.class));
	}

	@Test
	public void taskTaskManagerFailuresAreReportedBack() throws Exception {
		final DefaultScheduler scheduler = createSchedulerAndDeployTasks(new FailingTaskExecutorOperatorEventGateway());

		final OperatorCoordinator.Context context = getCoordinator(scheduler).getContext();
		final CompletableFuture<?> result = context.sendEvent(new TestOperatorEvent(), 0);

		assertThat(result, futureFailedWith(TestException.class));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testDeliveringClientRequestToRequestHandler() throws Exception {
		final OperatorCoordinator.Provider provider = new TestingCoordinationRequestHandler.Provider(testOperatorId);
		final DefaultScheduler scheduler = createScheduler(provider);

		final String payload = "testing payload";
		final TestingCoordinationRequestHandler.Request<String> request =
			new TestingCoordinationRequestHandler.Request<>(payload);
		final TestingCoordinationRequestHandler.Response<String> response =
			(TestingCoordinationRequestHandler.Response<String>)
				scheduler.deliverCoordinationRequestToCoordinator(testOperatorId, request).get();

		assertEquals(payload, response.getPayload());
	}

	@Test
	public void testDeliveringClientRequestToNonRequestHandler() throws Exception {
		final OperatorCoordinator.Provider provider = new TestingOperatorCoordinator.Provider(testOperatorId);
		final DefaultScheduler scheduler = createScheduler(provider);

		final String payload = "testing payload";
		final TestingCoordinationRequestHandler.Request<String> request =
			new TestingCoordinationRequestHandler.Request<>(payload);

		CommonTestUtils.assertThrows(
			"cannot handle client event",
			FlinkException.class,
			() -> scheduler.deliverCoordinationRequestToCoordinator(testOperatorId, request));
	}

	@Test
	public void testDeliveringClientRequestToNonExistingCoordinator() throws Exception {
		final OperatorCoordinator.Provider provider = new TestingOperatorCoordinator.Provider(testOperatorId);
		final DefaultScheduler scheduler = createScheduler(provider);

		final String payload = "testing payload";
		final TestingCoordinationRequestHandler.Request<String> request =
			new TestingCoordinationRequestHandler.Request<>(payload);

		CommonTestUtils.assertThrows(
			"does not exist",
			FlinkException.class,
			() -> scheduler.deliverCoordinationRequestToCoordinator(new OperatorID(), request));
	}

	// ------------------------------------------------------------------------
	//  test setups
	// ------------------------------------------------------------------------

	private DefaultScheduler createScheduler(OperatorCoordinator.Provider provider) throws Exception {
		return setupTestJobAndScheduler(provider, null, false);
	}

	private DefaultScheduler createAndStartScheduler() throws Exception {
		final DefaultScheduler scheduler = setupTestJobAndScheduler(new TestingOperatorCoordinator.Provider(testOperatorId), null, false);
		scheduler.startScheduling();
		return scheduler;
	}

	private DefaultScheduler createSchedulerAndDeployTasks() throws Exception {
		final DefaultScheduler scheduler = createAndStartScheduler();
		SchedulerTestingUtils.setAllExecutionsToRunning(scheduler);
		return scheduler;
	}

	private DefaultScheduler createSchedulerAndDeployTasks(TaskExecutorOperatorEventGateway gateway) throws Exception {
		final DefaultScheduler scheduler = setupTestJobAndScheduler(new TestingOperatorCoordinator.Provider(testOperatorId), gateway, false);
		scheduler.startScheduling();
		SchedulerTestingUtils.setAllExecutionsToRunning(scheduler);
		return scheduler;
	}

	private DefaultScheduler createSchedulerWithCheckpointing() throws Exception {
		final DefaultScheduler scheduler = setupTestJobAndScheduler(new TestingOperatorCoordinator.Provider(testOperatorId), null, true);
		scheduler.startScheduling();
		SchedulerTestingUtils.setAllExecutionsToRunning(scheduler);
		return scheduler;
	}

	private DefaultScheduler setupTestJobAndScheduler(
			OperatorCoordinator.Provider provider,
			@Nullable TaskExecutorOperatorEventGateway taskExecutorOperatorEventGateway,
			boolean enableCheckpoints) throws Exception {

		final JobVertex vertex = new JobVertex("Vertex with OperatorCoordinator", testVertexId);
		vertex.setInvokableClass(NoOpInvokable.class);
		vertex.addOperatorCoordinator(new SerializedValue<>(provider));
		vertex.setParallelism(2);

		final JobGraph jobGraph = new JobGraph("test job with OperatorCoordinator", vertex);
		if (enableCheckpoints) {
			SchedulerTestingUtils.enableCheckpointing(jobGraph);
		}

		final DefaultScheduler scheduler = taskExecutorOperatorEventGateway == null
				? SchedulerTestingUtils.createScheduler(jobGraph, executor)
				: SchedulerTestingUtils.createScheduler(jobGraph, executor, taskExecutorOperatorEventGateway);
		scheduler.setMainThreadExecutor(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		return scheduler;
	}

	private TestingOperatorCoordinator getCoordinator(DefaultScheduler scheduler) {
		final ExecutionJobVertex vertexWithCoordinator = getJobVertex(scheduler, testVertexId);
		assertNotNull("vertex for coordinator not found", vertexWithCoordinator);

		final OperatorCoordinator coordinator = vertexWithCoordinator.getOperatorCoordinator(testOperatorId);
		assertNotNull("vertex does not contain coordinator", coordinator);
		assertThat(coordinator, instanceOf(TestingOperatorCoordinator.class));

		return (TestingOperatorCoordinator) coordinator;
	}

	// ------------------------------------------------------------------------
	//  test actions
	// ------------------------------------------------------------------------

	private void failTask(DefaultScheduler scheduler, int subtask) {
		final ExecutionJobVertex ejv = getJobVertex(scheduler, testVertexId);
		assert ejv != null;
		final ExecutionAttemptID attemptID = ejv.getTaskVertices()[subtask].getCurrentExecutionAttempt().getAttemptId();

		scheduler.updateTaskExecutionState(new TaskExecutionState(
				ejv.getJobId(), attemptID, ExecutionState.FAILED, new Exception("test task failure")));
	}

	// ------------------------------------------------------------------------
	//  miscellaneous utilities
	// ------------------------------------------------------------------------

	private static ExecutionJobVertex getJobVertex(DefaultScheduler scheduler, JobVertexID jobVertexId) {
		final ExecutionVertexID id = new ExecutionVertexID(jobVertexId, 0);
		return scheduler.getExecutionVertex(id).getJobVertex();
	}

	// ------------------------------------------------------------------------
	//  test mocks
	// ------------------------------------------------------------------------

	private static final class TestOperatorEvent implements OperatorEvent {}

	private static final class TestException extends Exception {}

	private static final class CoordinatorThatFailsInStart extends TestingOperatorCoordinator {

		public CoordinatorThatFailsInStart(Context context) {
			super(context);
		}

		@Override
		public void start() throws Exception {
			throw new Exception("test failure");
		}
	}

	private static final class FailingTaskExecutorOperatorEventGateway implements TaskExecutorOperatorEventGateway {

		@Override
		public CompletableFuture<Acknowledge> sendOperatorEventToTask(
				ExecutionAttemptID task,
				OperatorID operator,
				SerializedValue<OperatorEvent> evt) {
			return FutureUtils.completedExceptionally(new TestException());
		}
	}
}
