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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.UnrecoverableException;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
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
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.Iterator;
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
		Instance instance = ExecutionGraphTestUtils.getInstance(
				new SimpleActorGateway(TestingUtils.directExecutionContext()),
				NUM_TASKS);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex sender = new JobVertex("Task");
		sender.setInvokableClass(Tasks.NoOpInvokable.class);
		sender.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("Pointwise job", sender);

		ExecutionGraph eg = new ExecutionGraph(
				TestingUtils.defaultExecutionContext(),
				new JobID(),
				"test job",
				new Configuration(),
				AkkaUtils.getDefaultTimeout(),
				new NoRestartStrategy());
		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution(scheduler);
		assertEquals(JobStatus.RUNNING, eg.getState());

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
		
		JobVertex groupVertex = new JobVertex("Task1");
		groupVertex.setInvokableClass(Tasks.NoOpInvokable.class);
		groupVertex.setParallelism(NUM_TASKS);

		JobVertex groupVertex2 = new JobVertex("Task2");
		groupVertex2.setInvokableClass(Tasks.NoOpInvokable.class);
		groupVertex2.setParallelism(NUM_TASKS);

		SlotSharingGroup sharingGroup = new SlotSharingGroup();
		groupVertex.setSlotSharingGroup(sharingGroup);
		groupVertex2.setSlotSharingGroup(sharingGroup);
		groupVertex.setStrictlyCoLocatedWith(groupVertex2);
		
		//initiate and schedule job
		JobGraph jobGraph = new JobGraph("Pointwise job", groupVertex, groupVertex2);
		ExecutionGraph eg = new ExecutionGraph(
			TestingUtils.defaultExecutionContext(),
			new JobID(),
			"test job",
			new Configuration(),
				AkkaUtils.getDefaultTimeout(),
				new FixedDelayRestartStrategy(1, 0L));
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
		Instance instance = ExecutionGraphTestUtils.getInstance(
				new SimpleActorGateway(TestingUtils.directExecutionContext()),
				NUM_TASKS);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex sender = new JobVertex("Task");
		sender.setInvokableClass(Tasks.NoOpInvokable.class);
		sender.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("Pointwise job", sender);

		ExecutionGraph eg = new ExecutionGraph(
				TestingUtils.defaultExecutionContext(),
				new JobID(),
				"Test job",
				new Configuration(),
				AkkaUtils.getDefaultTimeout(),
				new FixedDelayRestartStrategy(1, 1000));
		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution(scheduler);

		assertEquals(JobStatus.RUNNING, eg.getState());
		restartAfterFailure(eg, new FiniteDuration(2, TimeUnit.MINUTES), true);
	}

	@Test
	public void testCancelWhileRestarting() throws Exception {
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
		executionGraph.cancel();

		assertEquals(JobStatus.CANCELED, executionGraph.getState());

		// The restart has been aborted
		executionGraph.restart();

		assertEquals(JobStatus.CANCELED, executionGraph.getState());
	}

	@Test
	public void testCancelWhileFailing() throws Exception {
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
				AkkaUtils.getDefaultTimeout(),
				// We want to manually control the restart and delay
				new FixedDelayRestartStrategy(Integer.MAX_VALUE, Long.MAX_VALUE));

		// Spy on the graph
		executionGraph = spy(executionGraph);

		// Do nothing here, because we don't want to transition out of
		// the FAILING state.
		doNothing().when(executionGraph).jobVertexInFinalState();

		JobVertex jobVertex = new JobVertex("NoOpInvokable");
		jobVertex.setInvokableClass(Tasks.NoOpInvokable.class);
		jobVertex.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("TestJob", jobVertex);

		executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, executionGraph.getState());

		executionGraph.scheduleForExecution(scheduler);

		assertEquals(JobStatus.RUNNING, executionGraph.getState());

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
				if (vertex.getExecutionState() != ExecutionState.FAILED) {
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
	public void testNoRestartOnUnrecoverableException() throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
				new SimpleActorGateway(TestingUtils.directExecutionContext()),
				NUM_TASKS);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		JobVertex sender = new JobVertex("Task");
		sender.setInvokableClass(Tasks.NoOpInvokable.class);
		sender.setParallelism(NUM_TASKS);

		JobGraph jobGraph = new JobGraph("Pointwise job", sender);

		ExecutionGraph eg = spy(new ExecutionGraph(
			TestingUtils.defaultExecutionContext(),
			new JobID(),
			"Test job",
			new Configuration(),
			AkkaUtils.getDefaultTimeout(),
			new FixedDelayRestartStrategy(1, 1000)));

		eg.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());

		assertEquals(JobStatus.CREATED, eg.getState());

		eg.scheduleForExecution(scheduler);

		assertEquals(JobStatus.RUNNING, eg.getState());

		// Fail with unrecoverable Exception
		eg.getAllExecutionVertices().iterator().next().fail(
				new UnrecoverableException(new Exception("Test Exception")));

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

		JobVertex sender = new JobVertex("Task1");
		sender.setInvokableClass(Tasks.NoOpInvokable.class);
		sender.setParallelism(1);

		JobVertex receiver = new JobVertex("Task2");
		receiver.setInvokableClass(Tasks.NoOpInvokable.class);
		receiver.setParallelism(1);

		JobGraph jobGraph = new JobGraph("Pointwise job", sender, receiver);

		ExecutionGraph eg = new ExecutionGraph(
			TestingUtils.defaultExecutionContext(),
			new JobID(),
			"test job",
			new Configuration(),
			AkkaUtils.getDefaultTimeout(),
			new FixedDelayRestartStrategy(1, 1000));

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

		Deadline deadline = timeout.fromNow();

		while (deadline.hasTimeLeft() && eg.getState() != JobStatus.RUNNING) {
			Thread.sleep(100);
		}

		assertEquals(JobStatus.RUNNING, eg.getState());

		// Wait for deploying after async restart
		deadline = timeout.fromNow();
		boolean success = false;

		while (deadline.hasTimeLeft() && !success) {
			success = true;

			for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
				if (vertex.getCurrentExecutionAttempt().getAssignedResource() == null) {
					success = false;
					Thread.sleep(100);
					break;
				} else {
					vertex.getCurrentExecutionAttempt().switchToRunning();
				}
			}
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

	private static void restartAfterFailure(ExecutionGraph eg, FiniteDuration timeout, boolean haltAfterRestart) throws InterruptedException {

		eg.getAllExecutionVertices().iterator().next().fail(new Exception("Test Exception"));
		assertEquals(JobStatus.FAILING, eg.getState());

		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().cancelingComplete();
		}

		// Wait for async restart
		Deadline deadline = timeout.fromNow();
		while (deadline.hasTimeLeft() && eg.getState() != JobStatus.RUNNING) {
			Thread.sleep(100);
		}

		assertEquals(JobStatus.RUNNING, eg.getState());

		// Wait for deploying after async restart
		deadline = timeout.fromNow();
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

		if (haltAfterRestart) {
			if (deadline.hasTimeLeft()) {
				haltExecution(eg);
			} else {
				fail("Failed to wait until all execution attempts left the state DEPLOYING.");
			}
		}
	}

	private static void haltExecution(ExecutionGraph eg) {
		for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
			vertex.getCurrentExecutionAttempt().markFinished();
		}

		assertEquals(JobStatus.FINISHED, eg.getState());
	}
}
