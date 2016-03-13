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

import akka.dispatch.Futures;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.FailureRateRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;
import scala.concurrent.impl.Promise;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.SimpleActorGateway;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class ExecutionGraphRestartTest extends TestLogger {

	private final static int NUM_TASKS = 31;

	@Test
	public void testNoManualRestart() throws Exception {
		NoRestartStrategy restartStrategy = new NoRestartStrategy();
		Tuple2<ExecutionGraph, Instance> executionGraphInstanceTuple = createExecutionGraph(restartStrategy);
		ExecutionGraph eg = executionGraphInstanceTuple.f0;

		eg.getAllExecutionVertices().iterator().next().fail(new Exception("Test Exception"));

		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().cancelingComplete();
		}

		assertEquals(JobStatus.FAILED, eg.getState());

		// This should not restart the graph.
		eg.restart();

		assertEquals(JobStatus.FAILED, eg.getState());
	}

	@Test
	public void testConstraintsAfterRestart() throws Exception {
		
		//setting up
		Instance instance = ExecutionGraphTestUtils.getInstance(
			new SimpleActorGateway(TestingUtils.directExecutionContext()),
			NUM_TASKS);
		
		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex groupVertex = newJobVertex("Task1", NUM_TASKS, Tasks.NoOpInvokable.class);
		JobVertex groupVertex2 = newJobVertex("Task2", NUM_TASKS, Tasks.NoOpInvokable.class);

		SlotSharingGroup sharingGroup = new SlotSharingGroup();
		groupVertex.setSlotSharingGroup(sharingGroup);
		groupVertex2.setSlotSharingGroup(sharingGroup);
		groupVertex.setStrictlyCoLocatedWith(groupVertex2);
		
		//initiate and schedule job
		JobGraph jobGraph = new JobGraph("Pointwise job", groupVertex, groupVertex2);
		ExecutionGraph eg = newExecutionGraph(new FixedDelayRestartStrategy(1, 0L));
		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());
		
		eg.scheduleForExecution(scheduler);
		assertEquals(JobStatus.RUNNING, eg.getState());
		
		//sanity checks
		validateConstraints(eg);

		//restart automatically
		restartAfterFailure(eg, new FiniteDuration(2, TimeUnit.MINUTES), false);
		
		//checking execution vertex properties
		validateConstraints(eg);

		haltExecution(eg);
	}

	private void validateConstraints(ExecutionGraph eg) {
		
		ExecutionJobVertex[] tasks = eg.getAllVertices().values().toArray(new ExecutionJobVertex[2]);
		
		for(int i=0; i<NUM_TASKS; i++){
			CoLocationConstraint constr1 = tasks[0].getTaskVertices()[i].getLocationConstraint();
			CoLocationConstraint constr2 = tasks[1].getTaskVertices()[i].getLocationConstraint();
			assertNotNull(constr1.getSharedSlot());
			assertTrue(constr1.isAssigned());
			assertEquals(constr1, constr2);
		}
		
	}

	@Test
	public void testRestartAutomatically() throws Exception {
		RestartStrategy restartStrategy = new FixedDelayRestartStrategy(1, 1000);
		Tuple2<ExecutionGraph, Instance> executionGraphInstanceTuple = createExecutionGraph(restartStrategy);
		ExecutionGraph eg = executionGraphInstanceTuple.f0;

		restartAfterFailure(eg, new FiniteDuration(2, TimeUnit.MINUTES), true);
	}

	@Test
	public void taskShouldFailWhenFailureRateLimitExceeded() throws Exception {
		FailureRateRestartStrategy restartStrategy = new FailureRateRestartStrategy(2, Time.of(10, TimeUnit.SECONDS), Time.of(0, TimeUnit.SECONDS));
		FiniteDuration timeout = new FiniteDuration(2, TimeUnit.SECONDS);
		Tuple2<ExecutionGraph, Instance> executionGraphInstanceTuple = createExecutionGraph(restartStrategy);
		ExecutionGraph eg = executionGraphInstanceTuple.f0;

		restartAfterFailure(eg, timeout, false);
		restartAfterFailure(eg, timeout, false);
		makeAFailureAndWait(eg, timeout);
		//failure rate limit exceeded, so task should be failed
		assertEquals(JobStatus.FAILED, eg.getState());
	}

	@Test
	public void taskShouldNotFailWhenFailureRateLimitWasNotExceeded() throws Exception {
		FailureRateRestartStrategy restartStrategy = new FailureRateRestartStrategy(1, Time.of(1, TimeUnit.MILLISECONDS), Time.of(0, TimeUnit.SECONDS));
		FiniteDuration timeout = new FiniteDuration(2, TimeUnit.SECONDS);
		Tuple2<ExecutionGraph, Instance> executionGraphInstanceTuple = createExecutionGraph(restartStrategy);
		ExecutionGraph eg = executionGraphInstanceTuple.f0;

		//task restarted many times, but after all job is still running, because rate limit was not exceeded
		restartAfterFailure(eg, timeout, false);
		restartAfterFailure(eg, timeout, false);
		restartAfterFailure(eg, timeout, false);
		assertEquals(JobStatus.RUNNING, eg.getState());
	}

	@Test
	public void testCancelWhileRestarting() throws Exception {
		// We want to manually control the restart and delay
		FixedDelayRestartStrategy restartStrategy = new FixedDelayRestartStrategy(Integer.MAX_VALUE, Long.MAX_VALUE);
		Tuple2<ExecutionGraph, Instance> executionGraphInstanceTuple = createExecutionGraph(restartStrategy);
		ExecutionGraph executionGraph = executionGraphInstanceTuple.f0;
		Instance instance = executionGraphInstanceTuple.f1;

		// Kill the instance and wait for the job to restart
		instance.markDead();

		Deadline deadline = TestingUtils.TESTING_DURATION().fromNow();

		while (deadline.hasTimeLeft() &&
				executionGraph.getState() != JobStatus.RESTARTING) {

			Thread.sleep(100);
		}

		assertEquals(JobStatus.RESTARTING, executionGraph.getState());

		// Canceling needs to abort the restart
		executionGraph.cancel();

		assertEquals(JobStatus.CANCELED, executionGraph.getState());

		// The restart has been aborted
		executionGraph.restart();

		assertEquals(JobStatus.CANCELED, executionGraph.getState());
	}

	@Test
	public void testFailWhileRestarting() throws Exception {
		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

		Instance instance = ExecutionGraphTestUtils.getInstance(
			new SimpleActorGateway(TestingUtils.directExecutionContext()),
			NUM_TASKS);

		scheduler.newInstanceAvailable(instance);

		// Blocking program
		ExecutionGraph executionGraph = new ExecutionGraph(
			TestingUtils.defaultExecutionContext(),
			new JobID(),
			"TestJob",
			new Configuration(),
			new SerializedValue<>(new ExecutionConfig()),
			AkkaUtils.getDefaultTimeout(),
			// We want to manually control the restart and delay
			new FixedDelayRestartStrategy(Integer.MAX_VALUE, Long.MAX_VALUE));

		JobVertex jobVertex = new JobVertex("NoOpInvokable");
		jobVertex.setInvokableClass(Tasks.NoOpInvokable.class);
		jobVertex.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("TestJob", jobVertex);

		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, executionGraph.getState());

		executionGraph.scheduleForExecution(scheduler);

		assertEquals(JobStatus.RUNNING, executionGraph.getState());

		// Kill the instance and wait for the job to restart
		instance.markDead();

		Deadline deadline = TestingUtils.TESTING_DURATION().fromNow();

		while (deadline.hasTimeLeft() &&
			executionGraph.getState() != JobStatus.RESTARTING) {

			Thread.sleep(100);
		}

		assertEquals(JobStatus.RESTARTING, executionGraph.getState());

		// Canceling needs to abort the restart
		executionGraph.fail(new Exception("Test exception"));

		assertEquals(JobStatus.FAILED, executionGraph.getState());

		// The restart has been aborted
		executionGraph.restart();

		assertEquals(JobStatus.FAILED, executionGraph.getState());
	}

	@Test
	public void testCancelWhileFailing() throws Exception {
		// We want to manually control the restart and delay
		FixedDelayRestartStrategy restartStrategy = new FixedDelayRestartStrategy(Integer.MAX_VALUE, Long.MAX_VALUE);
		Tuple2<ExecutionGraph, Instance> executionGraphInstanceTuple = createSpyExecutionGraph(restartStrategy);
		ExecutionGraph executionGraph = executionGraphInstanceTuple.f0;
		Instance instance = executionGraphInstanceTuple.f1;
		doNothing().when(executionGraph).jobVertexInFinalState();

		// Kill the instance...
		instance.markDead();

		Deadline deadline = TestingUtils.TESTING_DURATION().fromNow();

		// ...and wait for all vertices to be in state FAILED. The
		// jobVertexInFinalState does nothing, that's why we don't wait on the
		// job status.
		boolean success = false;
		while (deadline.hasTimeLeft() && !success) {
			success = true;
			for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
				ExecutionState state = vertex.getExecutionState();
				if (state != ExecutionState.FAILED && state != ExecutionState.CANCELED) {
					success = false;
					Thread.sleep(100);
					break;
				}
			}
		}

		// Still in failing
		assertEquals(JobStatus.FAILING, executionGraph.getState());

		// The cancel call needs to change the state to CANCELLING
		executionGraph.cancel();

		assertEquals(JobStatus.CANCELLING, executionGraph.getState());

		// Unspy and finalize the job state
		doCallRealMethod().when(executionGraph).jobVertexInFinalState();

		executionGraph.jobVertexInFinalState();

		assertEquals(JobStatus.CANCELED, executionGraph.getState());
	}

	@Test
	public void testNoRestartOnSuppressException() throws Exception {
		Tuple2<ExecutionGraph, Instance> executionGraphInstanceTuple = createSpyExecutionGraph(new FixedDelayRestartStrategy(1, 1000));
		ExecutionGraph eg = executionGraphInstanceTuple.f0;

		// Fail with unrecoverable Exception
		eg.getAllExecutionVertices().iterator().next().fail(
				new SuppressRestartsException(new Exception("Test Exception")));

		assertEquals(JobStatus.FAILING, eg.getState());

		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().cancelingComplete();
		}

		FiniteDuration timeout = new FiniteDuration(2, TimeUnit.MINUTES);

		// Wait for async restart
		Deadline deadline = timeout.fromNow();
		while (deadline.hasTimeLeft() && eg.getState() != JobStatus.FAILED) {
			Thread.sleep(100);
		}

		assertEquals(JobStatus.FAILED, eg.getState());

		// No restart
		verify(eg, never()).restart();

		RestartStrategy restartStrategy = eg.getRestartStrategy();

		assertTrue(restartStrategy instanceof FixedDelayRestartStrategy);

		assertEquals(0, ((FixedDelayRestartStrategy) restartStrategy).getCurrentRestartAttempt());
	}

	/**
	 * Tests that a failing execution does not affect a restarted job. This is important if a
	 * callback handler fails an execution after it has already reached a final state and the job
	 * has been restarted.
	 */
	@Test
	public void testFailingExecutionAfterRestart() throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
			new SimpleActorGateway(TestingUtils.directExecutionContext()),
			2);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex sender = newJobVertex("Task1", 1, Tasks.NoOpInvokable.class);
		JobVertex receiver = newJobVertex("Task2", 1, Tasks.NoOpInvokable.class);
		JobGraph jobGraph = new JobGraph("Pointwise job", sender, receiver);
		ExecutionGraph eg = newExecutionGraph(new FixedDelayRestartStrategy(1, 1000));
		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution(scheduler);
		assertEquals(JobStatus.RUNNING, eg.getState());

		Iterator<ExecutionVertex> executionVertices = eg.getAllExecutionVertices().iterator();

		Execution finishedExecution = executionVertices.next().getCurrentExecutionAttempt();
		Execution failedExecution = executionVertices.next().getCurrentExecutionAttempt();

		finishedExecution.markFinished();

		failedExecution.fail(new Exception("Test Exception"));
		failedExecution.cancelingComplete();

		FiniteDuration timeout = new FiniteDuration(2, TimeUnit.MINUTES);

		waitForAsyncRestart(eg, timeout);

		assertEquals(JobStatus.RUNNING, eg.getState());

		// Wait for all resources to be assigned after async restart
		waitForAllResourcesToBeAssignedAfterAsyncRestart(eg, timeout.fromNow());

		// At this point all resources have been assigned
		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			assertNotNull("No assigned resource (test instability).", vertex.getCurrentAssignedResource());
			vertex.getCurrentExecutionAttempt().switchToRunning();
		}

		// fail old finished execution, this should not affect the execution
		finishedExecution.fail(new Exception("This should have no effect"));

		for (ExecutionVertex vertex: eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().markFinished();
		}

		// the state of the finished execution should have not changed since it is terminal
		assertEquals(ExecutionState.FINISHED, finishedExecution.getState());

		assertEquals(JobStatus.FINISHED, eg.getState());
	}

	/**
	 * Tests that a graph is not restarted after cancellation via a call to
	 * {@link ExecutionGraph#fail(Throwable)}. This can happen when a slot is
	 * released concurrently with cancellation.
	 */
	@Test
	public void testFailExecutionAfterCancel() throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
				new SimpleActorGateway(TestingUtils.directExecutionContext()),
				2);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex vertex = newJobVertex("Test Vertex", 1, Tasks.NoOpInvokable.class);

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			Integer.MAX_VALUE, Integer.MAX_VALUE));
		JobGraph jobGraph = new JobGraph("Test Job", vertex);
		jobGraph.setExecutionConfig(executionConfig);

		ExecutionGraph eg = newExecutionGraph(new FixedDelayRestartStrategy(1, 1000000));

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution(scheduler);
		assertEquals(JobStatus.RUNNING, eg.getState());

		// Fail right after cancel (for example with concurrent slot release)
		eg.cancel();

		for (ExecutionVertex v : eg.getAllExecutionVertices()) {
			v.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
		}

		assertEquals(JobStatus.CANCELED, eg.getState());

		Execution execution = eg.getAllExecutionVertices().iterator().next().getCurrentExecutionAttempt();

		execution.cancelingComplete();
		assertEquals(JobStatus.CANCELED, eg.getState());
	}

	/**
	 * Tests that it is possible to fail a graph via a call to
	 * {@link ExecutionGraph#fail(Throwable)} after cancellation.
	 */
	@Test
	public void testFailExecutionGraphAfterCancel() throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
				new SimpleActorGateway(TestingUtils.directExecutionContext()),
				2);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex vertex = newJobVertex("Test Vertex", 1, Tasks.NoOpInvokable.class);

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			Integer.MAX_VALUE, Integer.MAX_VALUE));
		JobGraph jobGraph = new JobGraph("Test Job", vertex);
		jobGraph.setExecutionConfig(executionConfig);

		ExecutionGraph eg = newExecutionGraph(new FixedDelayRestartStrategy(1, 1000000));

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution(scheduler);
		assertEquals(JobStatus.RUNNING, eg.getState());

		// Fail right after cancel (for example with concurrent slot release)
		eg.cancel();
		assertEquals(JobStatus.CANCELLING, eg.getState());

		eg.fail(new Exception("Test Exception"));
		assertEquals(JobStatus.FAILING, eg.getState());

		Execution execution = eg.getAllExecutionVertices().iterator().next().getCurrentExecutionAttempt();

		execution.cancelingComplete();
		assertEquals(JobStatus.RESTARTING, eg.getState());
	}

	/**
	 * Tests that a suspend call while restarting a job, will abort the restarting.
	 *
	 * @throws Exception
	 */
	@Test
	public void testSuspendWhileRestarting() throws Exception {
		FiniteDuration timeout = new FiniteDuration(1, TimeUnit.MINUTES);
		Deadline deadline = timeout.fromNow();

		Instance instance = ExecutionGraphTestUtils.getInstance(
			new SimpleActorGateway(TestingUtils.directExecutionContext()),
			NUM_TASKS);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex sender = new JobVertex("Task");
		sender.setInvokableClass(Tasks.NoOpInvokable.class);
		sender.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("Pointwise job", sender);

		ControllableRestartStrategy controllableRestartStrategy = new ControllableRestartStrategy(timeout);

		ExecutionGraph eg = new ExecutionGraph(
			TestingUtils.defaultExecutionContext(),
			new JobID(),
			"Test job",
			new Configuration(),
			new SerializedValue<>(new ExecutionConfig()),
			AkkaUtils.getDefaultTimeout(),
			controllableRestartStrategy);

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution(scheduler);

		assertEquals(JobStatus.RUNNING, eg.getState());

		instance.markDead();

		Await.ready(controllableRestartStrategy.getReachedCanRestart(), deadline.timeLeft());

		assertEquals(JobStatus.RESTARTING, eg.getState());

		eg.suspend(new Exception("Test exception"));

		assertEquals(JobStatus.SUSPENDED, eg.getState());

		controllableRestartStrategy.unlockRestart();

		Await.ready(controllableRestartStrategy.getRestartDone(), deadline.timeLeft());

		assertEquals(JobStatus.SUSPENDED, eg.getState());
	}

	private static class ControllableRestartStrategy implements RestartStrategy {

		private Promise<Boolean> reachedCanRestart = new Promise.DefaultPromise<>();
		private Promise<Boolean> doRestart = new Promise.DefaultPromise<>();
		private Promise<Boolean> restartDone = new Promise.DefaultPromise<>();

		private volatile Exception exception = null;

		private FiniteDuration timeout;

		public ControllableRestartStrategy(FiniteDuration timeout) {
			this.timeout = timeout;
		}

		public void unlockRestart() {
			doRestart.success(true);
		}

		public Exception getException() {
			return exception;
		}

		public Future<Boolean> getReachedCanRestart() {
			return reachedCanRestart.future();
		}

		public Future<Boolean> getRestartDone() {
			return restartDone.future();
		}

		@Override
		public boolean canRestart() {
			reachedCanRestart.success(true);
			return true;
		}

		@Override
		public void restart(final ExecutionGraph executionGraph) {
			Futures.future(new Callable<Object>() {
				@Override
				public Object call() throws Exception {
					try {

						Await.ready(doRestart.future(), timeout);
						executionGraph.restart();
					} catch (Exception e) {
						exception = e;
					}

					restartDone.success(true);

					return null;
				}
			}, TestingUtils.defaultExecutionContext());
		}
	}

	private static Tuple2<ExecutionGraph, Instance> createExecutionGraph(RestartStrategy restartStrategy) throws Exception {
		return createExecutionGraph(restartStrategy, false);
	}

	private static Tuple2<ExecutionGraph, Instance> createSpyExecutionGraph(RestartStrategy restartStrategy) throws Exception {
		return createExecutionGraph(restartStrategy, true);
	}

	private static Tuple2<ExecutionGraph, Instance> createExecutionGraph(RestartStrategy restartStrategy, boolean isSpy) throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
				new SimpleActorGateway(TestingUtils.directExecutionContext()),
				NUM_TASKS);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex sender = newJobVertex("Task", NUM_TASKS, Tasks.NoOpInvokable.class);

		JobGraph jobGraph = new JobGraph("Pointwise job", sender);

		ExecutionGraph eg = newExecutionGraph(restartStrategy);
		if (isSpy) {
			eg = spy(eg);
		}
		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution(scheduler);
		assertEquals(JobStatus.RUNNING, eg.getState());
		return new Tuple2<>(eg, instance);
	}

	private static JobVertex newJobVertex(String task1, int numTasks, Class<Tasks.NoOpInvokable> invokable) {
		JobVertex groupVertex = new JobVertex(task1);
		groupVertex.setInvokableClass(invokable);
		groupVertex.setParallelism(numTasks);
		return groupVertex;
	}

	private static ExecutionGraph newExecutionGraph(RestartStrategy restartStrategy) throws IOException {
		return new ExecutionGraph(
				TestingUtils.defaultExecutionContext(),
				new JobID(),
				"Test job",
				new Configuration(),
				new SerializedValue<>(new ExecutionConfig()),
				AkkaUtils.getDefaultTimeout(),
				restartStrategy);
	}

	private static void restartAfterFailure(ExecutionGraph eg, FiniteDuration timeout, boolean haltAfterRestart) throws InterruptedException {
		makeAFailureAndWait(eg, timeout);

		assertEquals(JobStatus.RUNNING, eg.getState());

		// Wait for deploying after async restart
		Deadline deadline = timeout.fromNow();
		waitForAllResourcesToBeAssignedAfterAsyncRestart(eg, deadline);

		if (haltAfterRestart) {
			if (deadline.hasTimeLeft()) {
				haltExecution(eg);
			} else {
				fail("Failed to wait until all execution attempts left the state DEPLOYING.");
			}
		}
	}

	private static void waitForAllResourcesToBeAssignedAfterAsyncRestart(ExecutionGraph eg, Deadline deadline) throws InterruptedException {
		boolean success = false;

		while (deadline.hasTimeLeft() && !success) {
			success = true;

			for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
				if (vertex.getCurrentExecutionAttempt().getAssignedResource() == null) {
					success = false;
					Thread.sleep(100);
					break;
				}
			}
		}
	}

	private static void makeAFailureAndWait(ExecutionGraph eg, FiniteDuration timeout) throws InterruptedException {
		eg.getAllExecutionVertices().iterator().next().fail(new Exception("Test Exception"));
		assertEquals(JobStatus.FAILING, eg.getState());

		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().cancelingComplete();
		}

		// Wait for async restart
		waitForAsyncRestart(eg, timeout);
	}

	private static void waitForAsyncRestart(ExecutionGraph eg, FiniteDuration timeout) throws InterruptedException {
		Deadline deadline = timeout.fromNow();
		while (deadline.hasTimeLeft() && eg.getState() != JobStatus.RUNNING) {
			Thread.sleep(100);
		}
	}

	private static void haltExecution(ExecutionGraph eg) {
		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().markFinished();
		}

		assertEquals(JobStatus.FINISHED, eg.getState());
	}
}
