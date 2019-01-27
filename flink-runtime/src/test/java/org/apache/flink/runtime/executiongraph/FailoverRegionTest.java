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
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
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
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.SimpleActorGateway;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilExecutionState;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilFailoverRegionState;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;

public class FailoverRegionTest extends TestLogger {

	private CheckpointCoordinator spyCheckpointCoordinator;

	@Before
	public void setup() {
		resetCheckpointCoordinator();
	}

	@After
	public void cleanup() {
		resetCheckpointCoordinator();
	}

	private void resetCheckpointCoordinator() {
		if (spyCheckpointCoordinator != null) {
			spyCheckpointCoordinator = null;
		}
	}

	/**
	 * Tests that a job only has one failover region and can recover from task failure successfully
	 * @throws Exception
	 */
	@Test
	public void testSingleRegionFailover() throws Exception {
		RestartStrategy restartStrategy = new InfiniteDelayRestartStrategy(10);
		ExecutionGraph eg = createSingleRegionExecutionGraph(restartStrategy);
		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();

		ExecutionVertex ev = eg.getAllExecutionVertices().iterator().next();

		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev).getState());

		ev.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev).getState());

		for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
			evs.getCurrentExecutionAttempt().cancelingComplete();
		}
		// #startCheckpointScheduler would trigger stop again
		verify(spyCheckpointCoordinator, times(2)).stopCheckpointScheduler();
		verify(spyCheckpointCoordinator, times(1)).restoreLatestCheckpointedState(any(List.class), any(Boolean.class), any(Boolean.class));
		verify(spyCheckpointCoordinator, times(1)).startCheckpointScheduler();
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

		ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraphDirectly(
			new DummyJobInformation(
				jobId,
				jobName),
			new DirectScheduledExecutorService(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			new InfiniteDelayRestartStrategy(10),
			new FailoverPipelinedRegionWithDirectExecutor(100),
			slotProvider,
			ordered);

		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();

		// the following two vertices are in the same failover region
		ExecutionVertex ev11 = eg.getJobVertex(v1.getID()).getTaskVertices()[0];
		ExecutionVertex ev21 = eg.getJobVertex(v2.getID()).getTaskVertices()[0];

		// the following two vertices are in the same failover region
		ExecutionVertex ev12 = eg.getJobVertex(v1.getID()).getTaskVertices()[1];
		ExecutionVertex ev22 = eg.getJobVertex(v2.getID()).getTaskVertices()[1];

		// the following vertices are in one failover region
		ExecutionVertex ev31 = eg.getJobVertex(v3.getID()).getTaskVertices()[0];
		ExecutionVertex ev32 = eg.getJobVertex(v3.getID()).getTaskVertices()[1];
		ExecutionVertex ev4 = eg.getJobVertex(v4.getID()).getTaskVertices()[0];

		List<ExecutionVertex> region1Vertices = new ArrayList<>();
		region1Vertices.add(ev11);
		region1Vertices.add(ev21);

		List<ExecutionVertex> region2Vertices = new ArrayList<>();
		region2Vertices.add(ev12);
		region2Vertices.add(ev22);

		List<ExecutionVertex> region3Vertices = new ArrayList<>();
		region3Vertices.add(ev32);
		region3Vertices.add(ev31);
		region3Vertices.add(ev4);

		enableCheckpointing(eg);
		spyCheckpointCoordinator = spy(eg.getCheckpointCoordinator());
		Whitebox.setInternalState(eg, "checkpointCoordinator", spyCheckpointCoordinator);

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

		verify(spyCheckpointCoordinator, times(2)).stopCheckpointScheduler();
		verify(spyCheckpointCoordinator, times(1)).restoreLatestCheckpointedState(eq(region1Vertices), any(Boolean.class), any(Boolean.class));
		verify(spyCheckpointCoordinator, times(1)).startCheckpointScheduler();
		verify(spyCheckpointCoordinator, never()).restoreLatestCheckpointedState(eq(region2Vertices), any(Boolean.class), any(Boolean.class));
		verify(spyCheckpointCoordinator, never()).restoreLatestCheckpointedState(eq(region3Vertices), any(Boolean.class), any(Boolean.class));

		ev11.getCurrentExecutionAttempt().markFinished();
		ev21.scheduleForExecution(slotProvider, true, LocationPreferenceConstraint.ALL);
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
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev4).getState());

		// triggered before
		verify(spyCheckpointCoordinator, times(4)).stopCheckpointScheduler();
		verify(spyCheckpointCoordinator, times(1)).restoreLatestCheckpointedState(eq(region1Vertices), any(Boolean.class), any(Boolean.class));
		verify(spyCheckpointCoordinator, never()).restoreLatestCheckpointedState(eq(region2Vertices), any(Boolean.class), any(Boolean.class));
		verify(spyCheckpointCoordinator, times(1)).restoreLatestCheckpointedState(eq(region3Vertices), any(Boolean.class), any(Boolean.class));
		verify(spyCheckpointCoordinator, times(2)).startCheckpointScheduler();

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
		verify(spyCheckpointCoordinator, never()).restoreLatestCheckpointedState(any(List.class), any(Boolean.class), any(Boolean.class));
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

		ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraphDirectly(
				new DummyJobInformation(
					jobId,
					jobName),
				new DirectScheduledExecutorService(),
				TestingUtils.defaultExecutor(),
				AkkaUtils.getDefaultTimeout(),
				new InfiniteDelayRestartStrategy(10),
				new RestartPipelinedRegionStrategy.Factory(),
				scheduler,
				ordered);

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

		final JobGraph jobGraph = new JobGraph(jobId, jobName, new JobVertex[] {v1, v2});
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraphDirectly(
			jobGraph,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			new InfiniteDelayRestartStrategy(10),
			new FailoverPipelinedRegionWithDirectExecutor(100),
			scheduler,
			VoidBlobWriter.getInstance());

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
		verify(spyCheckpointCoordinator, never()).stopCheckpointScheduler();
		verify(spyCheckpointCoordinator, never()).startCheckpointScheduler();
		verify(spyCheckpointCoordinator, never()).restoreLatestCheckpointedState(any(List.class), any(Boolean.class), any(Boolean.class));

		ExecutionVertex ev2 = iter.next();
		ev2.getCurrentExecutionAttempt().fail(new Exception("new fail"));
		assertEquals(JobStatus.RUNNING, eg.getState());
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev1).getState());
		for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
			evs.getCurrentExecutionAttempt().cancelingComplete();
		}
		verify(spyCheckpointCoordinator, times(2)).stopCheckpointScheduler();
		verify(spyCheckpointCoordinator, times(1)).restoreLatestCheckpointedState(any(List.class), any(Boolean.class), any(Boolean.class));
		verify(spyCheckpointCoordinator, times(1)).startCheckpointScheduler();
	}

	/**
	 * Tests that a new failure comes while the failover region is restarting
	 * @throws Exception
	 */
	@Test
	public void testFailWhileRestarting() throws Exception {
		RestartStrategy restartStrategy = new InfiniteDelayRestartStrategy();
		ExecutionGraph eg = createSingleRegionExecutionGraph(restartStrategy);
		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();

		Iterator<ExecutionVertex> iter = eg.getAllExecutionVertices().iterator();
		ExecutionVertex ev1 = iter.next();
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev1).getState());

		ev1.getCurrentExecutionAttempt().fail(new Exception("new fail"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev1).getState());

		// mark cancelingComplete for v1 vertices, as others vertices are still in CREATED state
		// and can change to CANCELED directly
		for (ExecutionVertex evs : ev1.getJobVertex().getTaskVertices()) {
			evs.getCurrentExecutionAttempt().cancelingComplete();
		}
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev1).getState());

		ev1.getCurrentExecutionAttempt().fail(new Exception("new fail"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev1).getState());
		verify(spyCheckpointCoordinator, times(2)).stopCheckpointScheduler();
		verify(spyCheckpointCoordinator, times(1)).restoreLatestCheckpointedState(any(List.class), any(Boolean.class), any(Boolean.class));
		verify(spyCheckpointCoordinator, times(1)).startCheckpointScheduler();

		ExecutionVertex ev2 = iter.next();
		ev2.getCurrentExecutionAttempt().fail(new Exception("new fail"));
		assertEquals(JobStatus.RUNNING, eg.getState());
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev1).getState());
		for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
			evs.getCurrentExecutionAttempt().cancelingComplete();
		}
		verify(spyCheckpointCoordinator, times(4)).stopCheckpointScheduler();
		verify(spyCheckpointCoordinator, times(2)).restoreLatestCheckpointedState(any(List.class), any(Boolean.class), any(Boolean.class));
		verify(spyCheckpointCoordinator, times(2)).startCheckpointScheduler();
	}

	@Test
	public void testRegionFailedExceedingMaxLimit() throws Exception {
		RestartStrategy restartStrategy = new InfiniteDelayRestartStrategy(10);
		ExecutionGraph eg = createSingleRegionExecutionGraph(restartStrategy,1);

		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();

		ExecutionVertex ev = eg.getAllExecutionVertices().iterator().next();

		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev).getState());

		// Recover for the first time
		ev.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
		assertEquals(JobStatus.RUNNING, eg.getState());
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev).getState());

		for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
			if (!ExecutionState.CREATED.equals(evs.getExecutionState())) {
				evs.getCurrentExecutionAttempt().cancelingComplete();
			}
		}
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev).getState());
		assertEquals(JobStatus.RUNNING, eg.getState());

		// Failed for the second time
		ev.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
		for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
			if (!ExecutionState.CREATED.equals(evs.getExecutionState())) {
				evs.getCurrentExecutionAttempt().cancelingComplete();
			}
		}
		assertEquals(JobStatus.RESTARTING, eg.getState());
	}

	private ExecutionGraph createSingleRegionExecutionGraph(RestartStrategy restartStrategy) throws Exception {
		return createSingleRegionExecutionGraph(restartStrategy, 100);
	}

	private ExecutionGraph createSingleRegionExecutionGraph(RestartStrategy restartStrategy, int regionMaxAttempts) throws Exception {
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

		JobGraph jobGraph = new JobGraph(jobId, jobName, v1, v2, v3);
		ExecutionGraph eg = ExecutionGraphTestUtils.createExecutionGraphDirectly(
			jobGraph,
			new DirectScheduledExecutorService(),
			TestingUtils.defaultExecutor(),
			AkkaUtils.getDefaultTimeout(),
			restartStrategy,
			new FailoverPipelinedRegionWithDirectExecutor(regionMaxAttempts),
			scheduler,
			VoidBlobWriter.getInstance());

		enableCheckpointing(eg);
		spyCheckpointCoordinator = spy(eg.getCheckpointCoordinator());
		Whitebox.setInternalState(eg, "checkpointCoordinator", spyCheckpointCoordinator);

		eg.scheduleForExecution();
		return eg;
	}

	// ------------------------------------------------------------------------

	/**
	 * A factory to create a RestartPipelinedRegionStrategy that uses a
	 * direct (synchronous) executor for easier testing.
	 */
	private static class FailoverPipelinedRegionWithDirectExecutor implements Factory {

		private int regionFailLimit;

		public FailoverPipelinedRegionWithDirectExecutor(int regionFailLimit) {
			this.regionFailLimit = regionFailLimit;
		}

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new RestartPipelinedRegionStrategy(executionGraph, Executors.directExecutor(), regionFailLimit);
		}
	}

	private static void enableCheckpointing(ExecutionGraph executionGraph) throws Exception {
		ArrayList<ExecutionJobVertex> jobVertices = new ArrayList<>(executionGraph.getAllVertices().values());
		executionGraph
			.enableCheckpointing(
					100,
					100,
					0,
					1,
				CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION,
				jobVertices,
				jobVertices,
				jobVertices,
				Collections.emptyList(),
				new StandaloneCheckpointIDCounter(),
				new StandaloneCompletedCheckpointStore(1),
				new MemoryStateBackend(),
				new CheckpointStatsTracker(
					0,
					jobVertices,
					mock(CheckpointCoordinatorConfiguration.class),
					new UnregisteredMetricsGroup()));
	}

}
