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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartCallback;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.NotCancelAckingTaskGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.SimpleActorGateway;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.completeCancellingForAllVertices;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createSimpleTestGraph;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.finishAllVertices;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.switchToRunning;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilJobStatus;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the restart behaviour of the {@link ExecutionGraph}.
 */
public class ExecutionGraphRestartTest extends TestLogger {

	private static final int NUM_TASKS = 31;

	private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);

	@After
	public void shutdown() {
		executor.shutdownNow();
	}

	// ------------------------------------------------------------------------

	@Test
	public void testNoManualRestart() throws Exception {
		NoRestartStrategy restartStrategy = new NoRestartStrategy();
		Tuple2<ExecutionGraph, Instance> executionGraphInstanceTuple = createExecutionGraph(restartStrategy);
		ExecutionGraph eg = executionGraphInstanceTuple.f0;

		eg.getAllExecutionVertices().iterator().next().fail(new Exception("Test Exception"));

		completeCanceling(eg);

		assertEquals(JobStatus.FAILED, eg.getState());

		// This should not restart the graph.
		eg.restart(eg.getGlobalModVersion());

		assertEquals(JobStatus.FAILED, eg.getState());
	}

	private void completeCanceling(ExecutionGraph eg) {
		executeOperationForAllExecutions(eg, Execution::cancelingComplete);
	}

	private void executeOperationForAllExecutions(ExecutionGraph eg, Consumer<Execution> operation) {
		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			operation.accept(vertex.getCurrentExecutionAttempt());
		}
	}

	@Test
	public void testRestartAutomatically() throws Exception {
		RestartStrategy restartStrategy = new FixedDelayRestartStrategy(1, 0L);
		Tuple2<ExecutionGraph, Instance> executionGraphInstanceTuple = createExecutionGraph(restartStrategy);
		ExecutionGraph eg = executionGraphInstanceTuple.f0;

		restartAfterFailure(eg, new FiniteDuration(2, TimeUnit.MINUTES), true);
	}

	@Test
	public void testCancelWhileRestarting() throws Exception {
		// We want to manually control the restart and delay
		RestartStrategy restartStrategy = new InfiniteDelayRestartStrategy();
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
		executionGraph.restart(executionGraph.getGlobalModVersion());

		assertEquals(JobStatus.CANCELED, executionGraph.getState());
	}

	@Test
	public void testFailWhileRestarting() throws Exception {
		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());

		Instance instance = ExecutionGraphTestUtils.getInstance(
			new ActorTaskManagerGateway(
				new SimpleActorGateway(TestingUtils.directExecutionContext())),
			NUM_TASKS);

		scheduler.newInstanceAvailable(instance);

		// Blocking program
		ExecutionGraph executionGraph = new ExecutionGraph(
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			new JobID(),
			"TestJob",
			new Configuration(),
			new SerializedValue<>(new ExecutionConfig()),
			AkkaUtils.getDefaultTimeout(),
			// We want to manually control the restart and delay
			new InfiniteDelayRestartStrategy(),
			scheduler);

		JobVertex jobVertex = new JobVertex("NoOpInvokable");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobVertex.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("TestJob", jobVertex);

		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, executionGraph.getState());

		executionGraph.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, executionGraph.getState());

		// Kill the instance and wait for the job to restart
		instance.markDead();

		waitUntilJobStatus(executionGraph, JobStatus.RESTARTING, TestingUtils.TESTING_DURATION().toMillis());

		assertEquals(JobStatus.RESTARTING, executionGraph.getState());

		// If we fail when being in RESTARTING, then we should try to restart again
		final long globalModVersion = executionGraph.getGlobalModVersion();
		final Exception testException = new Exception("Test exception");
		executionGraph.failGlobal(testException);

		assertNotEquals(globalModVersion, executionGraph.getGlobalModVersion());
		assertEquals(JobStatus.RESTARTING, executionGraph.getState());
		assertEquals(testException, executionGraph.getFailureCause()); // we should have updated the failure cause

		// but it should fail when sending a SuppressRestartsException
		executionGraph.failGlobal(new SuppressRestartsException(new Exception("Suppress restart exception")));

		assertEquals(JobStatus.FAILED, executionGraph.getState());

		// The restart has been aborted
		executionGraph.restart(executionGraph.getGlobalModVersion());

		assertEquals(JobStatus.FAILED, executionGraph.getState());
	}

	@Test
	public void testCancelWhileFailing() throws Exception {
		final RestartStrategy restartStrategy = new InfiniteDelayRestartStrategy();
		final ExecutionGraph graph = createExecutionGraph(restartStrategy).f0;

		assertEquals(JobStatus.RUNNING, graph.getState());

		// switch all tasks to running
		for (ExecutionVertex vertex : graph.getVerticesTopologically().iterator().next().getTaskVertices()) {
			vertex.getCurrentExecutionAttempt().switchToRunning();
		}

		graph.failGlobal(new Exception("test"));

		assertEquals(JobStatus.FAILING, graph.getState());

		graph.cancel();

		assertEquals(JobStatus.CANCELLING, graph.getState());

		// let all tasks finish cancelling
		completeCanceling(graph);

		assertEquals(JobStatus.CANCELED, graph.getState());
	}

	@Test
	public void testFailWhileCanceling() throws Exception {
		final RestartStrategy restartStrategy = new NoRestartStrategy();
		final ExecutionGraph graph = createExecutionGraph(restartStrategy).f0;

		assertEquals(JobStatus.RUNNING, graph.getState());
		switchAllTasksToRunning(graph);

		graph.cancel();

		assertEquals(JobStatus.CANCELLING, graph.getState());

		graph.failGlobal(new Exception("test"));

		assertEquals(JobStatus.FAILING, graph.getState());

		// let all tasks finish cancelling
		completeCanceling(graph);

		assertEquals(JobStatus.FAILED, graph.getState());
	}

	private void switchAllTasksToRunning(ExecutionGraph graph) {
		executeOperationForAllExecutions(graph, Execution::switchToRunning);
	}

	@Test
	public void testNoRestartOnSuppressException() throws Exception {
		final ExecutionGraph eg = createExecutionGraph(new FixedDelayRestartStrategy(Integer.MAX_VALUE, 0)).f0;

		// Fail with unrecoverable Exception
		eg.getAllExecutionVertices().iterator().next().fail(
				new SuppressRestartsException(new Exception("Test Exception")));

		assertEquals(JobStatus.FAILING, eg.getState());

		completeCanceling(eg);

		eg.waitUntilTerminal();
		assertEquals(JobStatus.FAILED, eg.getState());

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
			new ActorTaskManagerGateway(
				new SimpleActorGateway(TestingUtils.directExecutionContext())),
			2);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex sender = ExecutionGraphTestUtils.createJobVertex("Task1", 1, NoOpInvokable.class);
		JobVertex receiver = ExecutionGraphTestUtils.createJobVertex("Task2", 1, NoOpInvokable.class);
		JobGraph jobGraph = new JobGraph("Pointwise job", sender, receiver);
		ExecutionGraph eg = newExecutionGraph(new FixedDelayRestartStrategy(1, 0L), scheduler);
		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution();
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
	 * {@link ExecutionGraph#failGlobal(Throwable)}. This can happen when a slot is
	 * released concurrently with cancellation.
	 */
	@Test
	public void testFailExecutionAfterCancel() throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
			new ActorTaskManagerGateway(
				new SimpleActorGateway(TestingUtils.directExecutionContext())),
			2);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex vertex = ExecutionGraphTestUtils.createJobVertex("Test Vertex", 1, NoOpInvokable.class);

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			Integer.MAX_VALUE, Integer.MAX_VALUE));
		JobGraph jobGraph = new JobGraph("Test Job", vertex);
		jobGraph.setExecutionConfig(executionConfig);

		ExecutionGraph eg = newExecutionGraph(new InfiniteDelayRestartStrategy(), scheduler);

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, eg.getState());

		// Fail right after cancel (for example with concurrent slot release)
		eg.cancel();

		for (ExecutionVertex v : eg.getAllExecutionVertices()) {
			v.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
		}

		assertEquals(JobStatus.CANCELED, eg.getTerminationFuture().get());

		Execution execution = eg.getAllExecutionVertices().iterator().next().getCurrentExecutionAttempt();

		execution.cancelingComplete();
		assertEquals(JobStatus.CANCELED, eg.getState());
	}

	/**
	 * Tests that it is possible to fail a graph via a call to
	 * {@link ExecutionGraph#failGlobal(Throwable)} after cancellation.
	 */
	@Test
	public void testFailExecutionGraphAfterCancel() throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
			new ActorTaskManagerGateway(
				new SimpleActorGateway(TestingUtils.directExecutionContext())),
			2);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex vertex = ExecutionGraphTestUtils.createJobVertex("Test Vertex", 1, NoOpInvokable.class);

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(
			Integer.MAX_VALUE, Integer.MAX_VALUE));
		JobGraph jobGraph = new JobGraph("Test Job", vertex);
		jobGraph.setExecutionConfig(executionConfig);

		ExecutionGraph eg = newExecutionGraph(new InfiniteDelayRestartStrategy(), scheduler);

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, eg.getState());

		// Fail right after cancel (for example with concurrent slot release)
		eg.cancel();
		assertEquals(JobStatus.CANCELLING, eg.getState());

		eg.failGlobal(new Exception("Test Exception"));
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
		final Time timeout = Time.of(1, TimeUnit.MINUTES);

		Instance instance = ExecutionGraphTestUtils.getInstance(
			new ActorTaskManagerGateway(
				new SimpleActorGateway(TestingUtils.directExecutionContext())),
			NUM_TASKS);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex sender = new JobVertex("Task");
		sender.setInvokableClass(NoOpInvokable.class);
		sender.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("Pointwise job", sender);

		ControllableRestartStrategy controllableRestartStrategy = new ControllableRestartStrategy(timeout);

		ExecutionGraph eg = new ExecutionGraph(
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			new JobID(),
			"Test job",
			new Configuration(),
			new SerializedValue<>(new ExecutionConfig()),
			AkkaUtils.getDefaultTimeout(),
			controllableRestartStrategy,
			scheduler);

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, eg.getState());

		instance.markDead();

		controllableRestartStrategy.getReachedCanRestart().await(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

		assertEquals(JobStatus.RESTARTING, eg.getState());

		eg.suspend(new Exception("Test exception"));

		assertEquals(JobStatus.SUSPENDED, eg.getState());

		controllableRestartStrategy.unlockRestart();

		controllableRestartStrategy.getRestartDone().await(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

		assertEquals(JobStatus.SUSPENDED, eg.getState());
	}

	@Test
	public void testConcurrentLocalFailAndRestart() throws Exception {
		final int parallelism = 10;
		SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		final OneShotLatch restartLatch = new OneShotLatch();
		final TriggeredRestartStrategy triggeredRestartStrategy = new TriggeredRestartStrategy(restartLatch);

		final ExecutionGraph eg = createSimpleTestGraph(
			new JobID(),
			taskManagerGateway,
			triggeredRestartStrategy,
			createNoOpVertex(parallelism));

		WaitForTasks waitForTasks = new WaitForTasks(parallelism);
		WaitForTasks waitForTasksCancelled = new WaitForTasks(parallelism);
		taskManagerGateway.setSubmitConsumer(waitForTasks);
		taskManagerGateway.setCancelConsumer(waitForTasksCancelled);

		eg.setScheduleMode(ScheduleMode.EAGER);
		eg.scheduleForExecution();

		waitForTasks.getFuture().get(1000, TimeUnit.MILLISECONDS);

		switchToRunning(eg);

		final ExecutionJobVertex vertex = eg.getVerticesTopologically().iterator().next();
		final Execution first = vertex.getTaskVertices()[0].getCurrentExecutionAttempt();
		final Execution last = vertex.getTaskVertices()[vertex.getParallelism() - 1].getCurrentExecutionAttempt();

		final OneShotLatch failTrigger = new OneShotLatch();
		final CountDownLatch readyLatch = new CountDownLatch(2);

		Thread failure1 = new Thread() {
			@Override
			public void run() {
				readyLatch.countDown();
				try {
					failTrigger.await();
				} catch (InterruptedException ignored) {}

				first.fail(new Exception("intended test failure 1"));
			}
		};

		Thread failure2 = new Thread() {
			@Override
			public void run() {
				readyLatch.countDown();
				try {
					failTrigger.await();
				} catch (InterruptedException ignored) {}

				last.fail(new Exception("intended test failure 2"));
			}
		};

		// make sure both threads start simultaneously
		failure1.start();
		failure2.start();
		readyLatch.await();
		failTrigger.trigger();

		waitUntilJobStatus(eg, JobStatus.FAILING, 1000);

		WaitForTasks waitForTasksAfterRestart = new WaitForTasks(parallelism);
		taskManagerGateway.setSubmitConsumer(waitForTasksAfterRestart);

		waitForTasksCancelled.getFuture().get(1000L, TimeUnit.MILLISECONDS);

		completeCancellingForAllVertices(eg);

		// block the restart until we have completed for all vertices the cancellation
		// otherwise it might happen that the last vertex which failed will have a new
		// execution set due to restart which is wrongly canceled
		restartLatch.trigger();

		waitUntilJobStatus(eg, JobStatus.RUNNING, 1000);

		waitForTasksAfterRestart.getFuture().get(1000, TimeUnit.MILLISECONDS);

		switchToRunning(eg);
		finishAllVertices(eg);

		eg.waitUntilTerminal();
		assertEquals(JobStatus.FINISHED, eg.getState());
	}

	@Test
	public void testConcurrentGlobalFailAndRestarts() throws Exception {
		final OneShotLatch restartTrigger = new OneShotLatch();

		final int parallelism = 10;
		final JobID jid = new JobID();
		final JobVertex vertex = createNoOpVertex(parallelism);
		final NotCancelAckingTaskGateway taskManagerGateway = new NotCancelAckingTaskGateway();
		final SlotProvider slots = new SimpleSlotProvider(jid, parallelism, taskManagerGateway);
		final TriggeredRestartStrategy restartStrategy = new TriggeredRestartStrategy(restartTrigger);

		final ExecutionGraph eg = createSimpleTestGraph(jid, slots, restartStrategy, vertex);

		WaitForTasks waitForTasks = new WaitForTasks(parallelism);
		taskManagerGateway.setSubmitConsumer(waitForTasks);

		eg.setScheduleMode(ScheduleMode.EAGER);
		eg.scheduleForExecution();

		waitForTasks.getFuture().get(1000, TimeUnit.MILLISECONDS);

		switchToRunning(eg);

		// fail into 'RESTARTING'
		eg.failGlobal(new Exception("intended test failure 1"));
		assertEquals(JobStatus.FAILING, eg.getState());

		WaitForTasks waitForTasksRestart = new WaitForTasks(parallelism);
		taskManagerGateway.setSubmitConsumer(waitForTasksRestart);

		completeCancellingForAllVertices(eg);
		waitUntilJobStatus(eg, JobStatus.RESTARTING, 1000);

		eg.failGlobal(new Exception("intended test failure 2"));
		assertEquals(JobStatus.RESTARTING, eg.getState());

		// trigger both restart strategies to kick in concurrently
		restartTrigger.trigger();

		waitUntilJobStatus(eg, JobStatus.RUNNING, 1000);

		waitForTasksRestart.getFuture().get(1000, TimeUnit.MILLISECONDS);
		switchToRunning(eg);
		finishAllVertices(eg);

		eg.waitUntilTerminal();
		assertEquals(JobStatus.FINISHED, eg.getState());

		if (eg.getNumberOfFullRestarts() > 2) {
			fail("Too many restarts: " + eg.getNumberOfFullRestarts());
		}
	}

	@Test
	public void testRestartWithEagerSchedulingAndSlotSharing() throws Exception {
		// this test is inconclusive if not used with a proper multi-threaded executor
		assertTrue("test assumptions violated", ((ThreadPoolExecutor) executor).getCorePoolSize() > 1);

		SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		final int parallelism = 20;
		final Scheduler scheduler = createSchedulerWithInstances(parallelism, taskManagerGateway);

		final SlotSharingGroup sharingGroup = new SlotSharingGroup();

		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		source.setParallelism(parallelism);
		source.setSlotSharingGroup(sharingGroup);

		final JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(NoOpInvokable.class);
		sink.setParallelism(parallelism);
		sink.setSlotSharingGroup(sharingGroup);
		sink.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED_BOUNDED);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
			new JobID(), scheduler, new FixedDelayRestartStrategy(Integer.MAX_VALUE, 0), executor, source, sink);

		WaitForTasks waitForTasks = new WaitForTasks(parallelism * 2);
		taskManagerGateway.setSubmitConsumer(waitForTasks);

		eg.setScheduleMode(ScheduleMode.EAGER);
		eg.scheduleForExecution();

		waitForTasks.getFuture().get(1000, TimeUnit.MILLISECONDS);

		switchToRunning(eg);

		// fail into 'RESTARTING'
		eg.getAllExecutionVertices().iterator().next().getCurrentExecutionAttempt().fail(
			new Exception("intended test failure"));

		assertEquals(JobStatus.FAILING, eg.getState());

		WaitForTasks waitForTasksAfterRestart = new WaitForTasks(parallelism * 2);
		taskManagerGateway.setSubmitConsumer(waitForTasksAfterRestart);

		completeCancellingForAllVertices(eg);

		// clean termination
		waitUntilJobStatus(eg, JobStatus.RUNNING, 1000);

		waitForTasksAfterRestart.getFuture().get(1000, TimeUnit.MILLISECONDS);
		switchToRunning(eg);
		finishAllVertices(eg);
		waitUntilJobStatus(eg, JobStatus.FINISHED, 1000);
	}

	@Test
	public void testRestartWithSlotSharingAndNotEnoughResources() throws Exception {
		// this test is inconclusive if not used with a proper multi-threaded executor
		assertTrue("test assumptions violated", ((ThreadPoolExecutor) executor).getCorePoolSize() > 1);

		final int numRestarts = 10;
		final int parallelism = 20;

		TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		final Scheduler scheduler = createSchedulerWithInstances(parallelism - 1, taskManagerGateway);

		final SlotSharingGroup sharingGroup = new SlotSharingGroup();

		final JobVertex source = new JobVertex("source");
		source.setInvokableClass(NoOpInvokable.class);
		source.setParallelism(parallelism);
		source.setSlotSharingGroup(sharingGroup);

		final JobVertex sink = new JobVertex("sink");
		sink.setInvokableClass(NoOpInvokable.class);
		sink.setParallelism(parallelism);
		sink.setSlotSharingGroup(sharingGroup);
		sink.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED_BOUNDED);

		final ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraph(
				new JobID(), scheduler, new FixedDelayRestartStrategy(numRestarts, 0), executor, source, sink);

		eg.setScheduleMode(ScheduleMode.EAGER);
		eg.scheduleForExecution();

		// wait until no more changes happen
		while (eg.getNumberOfFullRestarts() < numRestarts) {
			Thread.sleep(1);
		}

		waitUntilJobStatus(eg, JobStatus.FAILED, 1000);

		final Throwable t = eg.getFailureCause();
		if (!(t instanceof NoResourceAvailableException)) {
			ExceptionUtils.rethrowException(t, t.getMessage());
		}
	}

	/**
	 * Tests that the {@link ExecutionGraph} can handle concurrent failures while
	 * being in the RESTARTING state.
	 */
	@Test
	public void testConcurrentFailureWhileRestarting() throws Exception {
		final long timeout = 5000L;

		final CountDownLatch countDownLatch = new CountDownLatch(2);
		final CountDownLatchRestartStrategy restartStrategy = new CountDownLatchRestartStrategy(countDownLatch);
		final ExecutionGraph executionGraph = createSimpleExecutionGraph(restartStrategy, new TestingSlotProvider(ignored -> new CompletableFuture<>()));

		executionGraph.setQueuedSchedulingAllowed(true);
		executionGraph.scheduleForExecution();

		assertThat(executionGraph.getState(), is(JobStatus.RUNNING));

		executionGraph.failGlobal(new FlinkException("Test exception"));

		executor.execute(() -> {
			countDownLatch.countDown();
			try {
				countDownLatch.await();
			} catch (InterruptedException e) {
				ExceptionUtils.rethrow(e);
			}

			executionGraph.failGlobal(new FlinkException("Concurrent exception"));
		});

		waitUntilJobStatus(executionGraph, JobStatus.RUNNING, timeout);
	}

	private static final class CountDownLatchRestartStrategy implements RestartStrategy {

		private final CountDownLatch countDownLatch;

		private CountDownLatchRestartStrategy(CountDownLatch countDownLatch) {
			this.countDownLatch = countDownLatch;
		}

		@Override
		public boolean canRestart() {
			return true;
		}

		@Override
		public void restart(RestartCallback restarter, ScheduledExecutor executor) {
			executor.execute(() -> {
				countDownLatch.countDown();

				try {
					countDownLatch.await();
				} catch (InterruptedException e) {
					ExceptionUtils.rethrow(e);
				}

				restarter.triggerFullRecovery();
			});
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private Scheduler createSchedulerWithInstances(int num, TaskManagerGateway taskManagerGateway) {
		final Scheduler scheduler = new Scheduler(executor);
		final Instance[] instances = new Instance[num];

		for (int i = 0; i < instances.length; i++) {
			instances[i] = createInstance(taskManagerGateway, 55443 + i);
			scheduler.newInstanceAvailable(instances[i]);
		}

		return scheduler;
	}

	private static Instance createInstance(TaskManagerGateway taskManagerGateway, int port) {
		final HardwareDescription resources = new HardwareDescription(4, 1_000_000_000, 500_000_000, 400_000_000);
		final TaskManagerLocation location = new TaskManagerLocation(
				ResourceID.generate(), InetAddress.getLoopbackAddress(), port);
		return new Instance(taskManagerGateway, location, new InstanceID(), resources, 1);
	}

	// ------------------------------------------------------------------------

	private static class ControllableRestartStrategy implements RestartStrategy {

		private final OneShotLatch reachedCanRestart = new OneShotLatch();
		private final OneShotLatch doRestart = new OneShotLatch();
		private final OneShotLatch restartDone = new OneShotLatch();

		private final Time timeout;

		private volatile Exception exception;

		public ControllableRestartStrategy(Time timeout) {
			this.timeout = timeout;
		}

		public void unlockRestart() {
			doRestart.trigger();
		}

		public Exception getException() {
			return exception;
		}

		public OneShotLatch getReachedCanRestart() {
			return reachedCanRestart;
		}

		public OneShotLatch getRestartDone() {
			return restartDone;
		}

		@Override
		public boolean canRestart() {
			reachedCanRestart.trigger();
			return true;
		}

		@Override
		public void restart(final RestartCallback restarter, ScheduledExecutor executor) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						doRestart.await(timeout.getSize(), timeout.getUnit());
						restarter.triggerFullRecovery();
					} catch (Exception e) {
						exception = e;
					}

					restartDone.trigger();
				}
			});
		}
	}

	private static Tuple2<ExecutionGraph, Instance> createExecutionGraph(RestartStrategy restartStrategy) throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
			new ActorTaskManagerGateway(
				new SimpleActorGateway(TestingUtils.directExecutionContext())),
			NUM_TASKS);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		ExecutionGraph eg = createSimpleExecutionGraph(restartStrategy, scheduler);

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, eg.getState());
		return new Tuple2<>(eg, instance);
	}

	private static ExecutionGraph createSimpleExecutionGraph(RestartStrategy restartStrategy, SlotProvider slotProvider) throws IOException, JobException {
		JobGraph jobGraph = createJobGraph(NUM_TASKS);

		ExecutionGraph eg = newExecutionGraph(restartStrategy, slotProvider);
		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		return eg;
	}

	@Nonnull
	private static JobGraph createJobGraph(int parallelism) {
		JobVertex sender = ExecutionGraphTestUtils.createJobVertex("Task", parallelism, NoOpInvokable.class);

		return new JobGraph("Pointwise job", sender);
	}

	private static ExecutionGraph newExecutionGraph(RestartStrategy restartStrategy, SlotProvider slotProvider) throws IOException {
		return new ExecutionGraph(
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			new JobID(),
			"Test job",
			new Configuration(),
			new SerializedValue<>(new ExecutionConfig()),
			AkkaUtils.getDefaultTimeout(),
			restartStrategy,
			slotProvider);
	}

	private static void restartAfterFailure(ExecutionGraph eg, FiniteDuration timeout, boolean haltAfterRestart) throws InterruptedException, TimeoutException {
		ExecutionGraphTestUtils.failExecutionGraph(eg, new Exception("Test Exception"));

		// Wait for async restart
		waitForAsyncRestart(eg, timeout);

		assertEquals(JobStatus.RUNNING, eg.getState());

		// Wait for deploying after async restart
		Deadline deadline = timeout.fromNow();
		waitUntilAllExecutionsReachDeploying(eg, deadline);

		if (haltAfterRestart) {
			if (deadline.hasTimeLeft()) {
				haltExecution(eg);
			} else {
				fail("Failed to wait until all execution attempts left the state DEPLOYING.");
			}
		}
	}

	private static void waitUntilAllExecutionsReachDeploying(ExecutionGraph eg, Deadline deadline) throws TimeoutException {
		ExecutionGraphTestUtils.waitForAllExecutionsPredicate(
			eg,
			ExecutionGraphTestUtils.isInExecutionState(ExecutionState.DEPLOYING),
			deadline.timeLeft().toMillis());
	}

	private static void waitForAllResourcesToBeAssignedAfterAsyncRestart(ExecutionGraph eg, Deadline deadline) throws TimeoutException {
		ExecutionGraphTestUtils.waitForAllExecutionsPredicate(
			eg,
			ExecutionGraphTestUtils.hasResourceAssigned,
			deadline.timeLeft().toMillis());
	}

	private static void waitForAsyncRestart(ExecutionGraph eg, FiniteDuration timeout) throws InterruptedException {
		Deadline deadline = timeout.fromNow();
		long waitingTime = 10L;
		while (deadline.hasTimeLeft() && eg.getState() != JobStatus.RUNNING) {
			Thread.sleep(waitingTime);
			waitingTime = Math.min(waitingTime << 1, 100L);
		}
	}

	private static void haltExecution(ExecutionGraph eg) {
		finishAllVertices(eg);

		assertEquals(JobStatus.FINISHED, eg.getState());
	}

	// ------------------------------------------------------------------------

	/**
	 * A RestartStrategy that blocks restarting on a given {@link OneShotLatch}.
	 */
	private static final class TriggeredRestartStrategy implements RestartStrategy {

		private final OneShotLatch latch;

		TriggeredRestartStrategy(OneShotLatch latch) {
			this.latch = latch;
		}

		@Override
		public boolean canRestart() {
			return true;
		}

		@Override
		public void restart(final RestartCallback restarter, ScheduledExecutor executor) {
			executor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						latch.await();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
					restarter.triggerFullRecovery();
				}
			});
		}
	}

	/**
	 * A consumer which counts the number of tasks for which it has been called and completes a future
	 * upon reaching the number of tasks to wait for.
	 */
	public static class WaitForTasks implements Consumer<ExecutionAttemptID> {

		private final int tasksToWaitFor;
		private final CompletableFuture<Boolean> allTasksReceived;
		private final AtomicInteger counter;

		public WaitForTasks(int tasksToWaitFor) {
			this.tasksToWaitFor = tasksToWaitFor;
			this.allTasksReceived = new CompletableFuture<>();
			this.counter = new AtomicInteger();
		}

		public CompletableFuture<Boolean> getFuture() {
			return allTasksReceived;
		}

		@Override
		public void accept(ExecutionAttemptID executionAttemptID) {
			if (counter.incrementAndGet() >= tasksToWaitFor) {
				allTasksReceived.complete(true);
			}
		}
	}
}
