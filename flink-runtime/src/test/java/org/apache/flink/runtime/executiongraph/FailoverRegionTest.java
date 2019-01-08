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
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy.Factory;
import org.apache.flink.runtime.executiongraph.failover.RestartPipelinedRegionStrategy;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.SimpleActorGateway;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilExecutionState;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilFailoverRegionState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FailoverRegionTest extends TestLogger {

	/**
	 * Tests that a job only has one failover region and can recover from task failure successfully
	 * @throws Exception
	 */
	@Test
	public void testSingleRegionFailover() throws Exception {
		RestartStrategy restartStrategy = new InfiniteDelayRestartStrategy(10);
		ExecutionGraph eg = createSingleRegionExecutionGraph(restartStrategy);
		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();

		ExecutionVertex ev = eg.getAllExecutionVertices().iterator().next();

		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev).getState());

		ev.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev).getState());

		for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
			evs.getCurrentExecutionAttempt().cancelingComplete();
		}
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev).getState());
	}

	/**
	 * Tests that a job has server failover regions and one region failover does not influence others
	 * 
	 * <pre>
	 *     (a1) ---> (b1) -+-> (c1) ---+-> (d1) 
	 *                     X          /
	 *     (a2) ---> (b2) -+-> (c2) -+
	 *
	 *           ^         ^         ^
	 *           |         |         |
	 *     (pipelined) (blocking) (pipelined)
	 *
	 * </pre>
	 */
	@Test
	public void testMultiRegionsFailover() throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		final SlotProvider slotProvider = new SimpleSlotProvider(jobId, 20);
				
		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");
		JobVertex v4 = new JobVertex("vertex4");

		v1.setParallelism(2);
		v2.setParallelism(2);
		v3.setParallelism(2);
		v4.setParallelism(1);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		List<JobVertex> ordered = Arrays.asList(v1, v2, v3, v4);

		ExecutionGraph eg = new ExecutionGraph(
			new DummyJobInformation(
				jobId,
				jobName),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			new InfiniteDelayRestartStrategy(10),
			new FailoverPipelinedRegionWithDirectExecutor(),
			slotProvider);

		eg.attachJobGraph(ordered);

		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();

		// the following two vertices are in the same failover region
		ExecutionVertex ev11 = eg.getJobVertex(v1.getID()).getTaskVertices()[0];
		ExecutionVertex ev21 = eg.getJobVertex(v2.getID()).getTaskVertices()[0];

		// the following two vertices are in the same failover region
		ExecutionVertex ev12 = eg.getJobVertex(v1.getID()).getTaskVertices()[1];
		ExecutionVertex ev22 = eg.getJobVertex(v2.getID()).getTaskVertices()[1];

		// the following vertices are in one failover region
		ExecutionVertex ev31 = eg.getJobVertex(v3.getID()).getTaskVertices()[0];
		ExecutionVertex ev32 = eg.getJobVertex(v3.getID()).getTaskVertices()[1];
		ExecutionVertex ev4 = eg.getJobVertex(v3.getID()).getTaskVertices()[0];

		eg.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev11).getState());

		ev21.scheduleForExecution(slotProvider, true, LocationPreferenceConstraint.ALL);
		ev21.getCurrentExecutionAttempt().fail(new Exception("New fail"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev11).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev22).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev31).getState());

		ev11.getCurrentExecutionAttempt().cancelingComplete();
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev11).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev22).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev31).getState());

		ev11.getCurrentExecutionAttempt().markFinished();
		ev21.getCurrentExecutionAttempt().markFinished();
		ev22.scheduleForExecution(slotProvider, true, LocationPreferenceConstraint.ALL);
		ev22.getCurrentExecutionAttempt().markFinished();
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev11).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev22).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev31).getState());

		waitUntilExecutionState(ev31.getCurrentExecutionAttempt(), ExecutionState.DEPLOYING, 2000);
		waitUntilExecutionState(ev32.getCurrentExecutionAttempt(), ExecutionState.DEPLOYING, 2000);

		ev31.getCurrentExecutionAttempt().fail(new Exception("New fail"));
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev11).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev22).getState());
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev31).getState());

		ev32.getCurrentExecutionAttempt().cancelingComplete();
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev11).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev22).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev31).getState());
	}

	/**
	 * Tests that when a task fail, and restart strategy doesn't support restarting, the job will go to failed
	 * @throws Exception
	 */
	@Test
	public void testNoManualRestart() throws Exception {
		NoRestartStrategy restartStrategy = new NoRestartStrategy();
		ExecutionGraph eg = createSingleRegionExecutionGraph(restartStrategy);

		ExecutionVertex ev = eg.getAllExecutionVertices().iterator().next();

		ev.fail(new Exception("Test Exception"));

		for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
			evs.getCurrentExecutionAttempt().cancelingComplete();
		}
		assertEquals(JobStatus.FAILED, eg.getState());
	}

	/**
	 * Tests that two failover regions failover at the same time, they will not influence each other
	 * @throws Exception
	 */
	@Test
	public void testMultiRegionFailoverAtSameTime() throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
				new ActorTaskManagerGateway(
						new SimpleActorGateway(TestingUtils.directExecutionContext())),
				16);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");
		JobVertex v4 = new JobVertex("vertex4");

		v1.setParallelism(2);
		v2.setParallelism(2);
		v3.setParallelism(2);
		v4.setParallelism(2);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);
		v4.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		List<JobVertex> ordered = Arrays.asList(v1, v2, v3, v4);

		ExecutionGraph eg = new ExecutionGraph(
				new DummyJobInformation(
					jobId,
					jobName),
				TestingUtils.defaultExecutor(),
				TestingUtils.defaultExecutor(),
				AkkaUtils.getDefaultTimeout(),
				new InfiniteDelayRestartStrategy(10),
				new RestartPipelinedRegionStrategy.Factory(),
				scheduler);
		try {
			eg.attachJobGraph(ordered);
		}
		catch (JobException e) {
			e.printStackTrace();
			fail("Job failed with exception: " + e.getMessage());
		}
		eg.scheduleForExecution();
		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();

		ExecutionVertex ev11 = eg.getJobVertex(v1.getID()).getTaskVertices()[0];
		ExecutionVertex ev12 = eg.getJobVertex(v1.getID()).getTaskVertices()[1];
		ExecutionVertex ev31 = eg.getJobVertex(v3.getID()).getTaskVertices()[0];
		ExecutionVertex ev32 = eg.getJobVertex(v3.getID()).getTaskVertices()[1];
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev11).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev31).getState());

		ev11.getCurrentExecutionAttempt().fail(new Exception("new fail"));
		ev31.getCurrentExecutionAttempt().fail(new Exception("new fail"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev11).getState());
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev31).getState());

		ev32.getCurrentExecutionAttempt().cancelingComplete();
		waitUntilFailoverRegionState(strategy.getFailoverRegion(ev31), JobStatus.RUNNING, 1000);

		ev12.getCurrentExecutionAttempt().cancelingComplete();
		waitUntilFailoverRegionState(strategy.getFailoverRegion(ev11), JobStatus.RUNNING, 1000);
	}

	/**
	 * Tests that if a task reports the result of its preceding task is failed,
	 * its preceding task will be considered as failed, and start to failover
	 * TODO: as the report part is not finished yet, this case is ignored temporarily
	 * @throws Exception
	 */
	@Ignore
	@Test
	public void testSucceedingNoticePreceding() throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
				new ActorTaskManagerGateway(
						new SimpleActorGateway(TestingUtils.directExecutionContext())),
				14);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");

		v1.setParallelism(1);
		v2.setParallelism(1);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));

		ExecutionGraph eg = new ExecutionGraph(
			new DummyJobInformation(
				jobId,
				jobName),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			new InfiniteDelayRestartStrategy(10),
			new FailoverPipelinedRegionWithDirectExecutor(),
			scheduler);
		try {
			eg.attachJobGraph(ordered);
		}
		catch (JobException e) {
			e.printStackTrace();
			fail("Job failed with exception: " + e.getMessage());
		}
		eg.setScheduleMode(ScheduleMode.EAGER);
		eg.scheduleForExecution();
		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();

		ExecutionVertex ev11 = eg.getJobVertex(v2.getID()).getTaskVertices()[0];
		ExecutionVertex ev21 = eg.getJobVertex(v2.getID()).getTaskVertices()[0];
		ev21.getCurrentExecutionAttempt().fail(new Exception("Fail with v1"));

		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev21).getState());
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev11).getState());
	}

	/**
	 * Tests that a new failure comes while the failover region is in CANCELLING
	 * @throws Exception
	 */
	@Test
	public void testFailWhileCancelling() throws Exception {
		RestartStrategy restartStrategy = new InfiniteDelayRestartStrategy();
		ExecutionGraph eg = createSingleRegionExecutionGraph(restartStrategy);
		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();

		Iterator<ExecutionVertex> iter = eg.getAllExecutionVertices().iterator();
		ExecutionVertex ev1 = iter.next();
		ev1.getCurrentExecutionAttempt().switchToRunning();
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev1).getState());

		ev1.getCurrentExecutionAttempt().fail(new Exception("new fail"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev1).getState());

		ExecutionVertex ev2 = iter.next();
		ev2.getCurrentExecutionAttempt().fail(new Exception("new fail"));
		assertEquals(JobStatus.RUNNING, eg.getState());
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev1).getState());
	}

	/**
	 * Tests that a new failure comes while the failover region is restarting
	 * @throws Exception
	 */
	@Test
	public void testFailWhileRestarting() throws Exception {
		RestartStrategy restartStrategy = new InfiniteDelayRestartStrategy();
		ExecutionGraph eg = createSingleRegionExecutionGraph(restartStrategy);
		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();

		Iterator<ExecutionVertex> iter = eg.getAllExecutionVertices().iterator();
		ExecutionVertex ev1 = iter.next();
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev1).getState());

		ev1.getCurrentExecutionAttempt().fail(new Exception("new fail"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev1).getState());

		for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
			evs.getCurrentExecutionAttempt().cancelingComplete();
		}
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev1).getState());

		ev1.getCurrentExecutionAttempt().fail(new Exception("new fail"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev1).getState());
	}

	private static ExecutionGraph createSingleRegionExecutionGraph(RestartStrategy restartStrategy) throws Exception {
		Instance instance = ExecutionGraphTestUtils.getInstance(
				new ActorTaskManagerGateway(
						new SimpleActorGateway(TestingUtils.directExecutionContext())),
				14);

		Scheduler scheduler = new Scheduler(TestingUtils.defaultExecutionContext());
		scheduler.newInstanceAvailable(instance);

		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");

		v1.setParallelism(3);
		v2.setParallelism(2);
		v3.setParallelism(2);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);
		v3.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2, v3));

		ExecutionGraph eg = new ExecutionGraph(
			new DummyJobInformation(
				jobId,
				jobName),
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			restartStrategy,
			new FailoverPipelinedRegionWithDirectExecutor(),
			scheduler);
		try {
			eg.attachJobGraph(ordered);
		}
		catch (JobException e) {
			e.printStackTrace();
			fail("Job failed with exception: " + e.getMessage());
		}

		eg.scheduleForExecution();
		return eg;
	}

	// ------------------------------------------------------------------------

	/**
	 * A factory to create a RestartPipelinedRegionStrategy that uses a
	 * direct (synchronous) executor for easier testing.
	 */
	private static class FailoverPipelinedRegionWithDirectExecutor implements Factory {

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new RestartPipelinedRegionStrategy(executionGraph, Executors.directExecutor());
		}
	}

}
