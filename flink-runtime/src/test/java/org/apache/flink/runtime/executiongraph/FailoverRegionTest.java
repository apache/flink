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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTest;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy.Factory;
import org.apache.flink.runtime.executiongraph.failover.RestartPipelinedRegionStrategy;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilExecutionState;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilFailoverRegionState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class FailoverRegionTest extends TestLogger {

	private static final long checkpointId = 42L;

	/**
	 * Tests that a job only has one failover region and can recover from task failure successfully with state.
	 * @throws Exception if fail to create the single region execution graph or fail to acknowledge all checkpoints.
	 */
	@Test
	public void testSingleRegionFailover() throws Exception {
		RestartStrategy restartStrategy = new InfiniteDelayRestartStrategy(10);
		ExecutionGraph eg = createSingleRegionExecutionGraph(restartStrategy);
		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy) eg.getFailoverStrategy();

		ExecutionVertex ev = eg.getAllExecutionVertices().iterator().next();

		assertNotNull(eg.getCheckpointCoordinator());
		assertFalse(eg.getCheckpointCoordinator().getPendingCheckpoints().isEmpty());

		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev).getState());

		acknowledgeAllCheckpoints(eg.getCheckpointCoordinator(), eg.getAllExecutionVertices().iterator());

		// verify checkpoint has been completed successfully.
		assertEquals(1, eg.getCheckpointCoordinator().getCheckpointStore().getNumberOfRetainedCheckpoints());
		assertEquals(checkpointId, eg.getCheckpointCoordinator().getCheckpointStore().getLatestCheckpoint(false).getCheckpointID());

		ev.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev).getState());

		for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
			evs.getCurrentExecutionAttempt().completeCancelling();
		}

		verifyCheckpointRestoredAsExpected(eg);

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

		final Map<ExecutionAttemptID, JobManagerTaskRestore> attemptIDInitStateMap = new HashMap<>();
		final SlotProvider slotProvider = new SimpleSlotProvider(jobId, 20, new CollectTddTaskManagerGateway(attemptIDInitStateMap));

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

		final ExecutionGraph eg = new ExecutionGraphTestUtils.TestingExecutionGraphBuilder(jobId, jobName, v1, v2, v3, v4)
			.setRestartStrategy(new InfiniteDelayRestartStrategy(10))
			.setFailoverStrategyFactory(new FailoverPipelinedRegionWithDirectExecutor())
			.setSlotProvider(slotProvider)
			.allowQueuedScheduling()
			.build();

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

		enableCheckpointing(eg);

		eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		eg.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, eg.getState());

		attachPendingCheckpoints(eg);

		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev11).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev21).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev31).getState());

		acknowledgeAllCheckpoints(eg.getCheckpointCoordinator(), Arrays.asList(ev11, ev21, ev12, ev22, ev31, ev32, ev4).iterator());

		ev21.scheduleForExecution(eg.getSlotProviderStrategy(), LocationPreferenceConstraint.ALL, Collections.emptySet());
		ev21.getCurrentExecutionAttempt().fail(new Exception("New fail"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev11).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev22).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev31).getState());

		ev11.getCurrentExecutionAttempt().completeCancelling();
		verifyCheckpointRestoredAsExpected(eg);

		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev11).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev22).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev31).getState());

		ev11.getCurrentExecutionAttempt().markFinished();
		ev21.getCurrentExecutionAttempt().markFinished();
		ev22.scheduleForExecution(eg.getSlotProviderStrategy(), LocationPreferenceConstraint.ALL, Collections.emptySet());
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

		ev32.getCurrentExecutionAttempt().completeCancelling();
		verifyCheckpointRestoredAsExpected(eg);

		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev11).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev22).getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev31).getState());

		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev4).getState());
	}

	/**
	 * Tests that when a task fail, and restart strategy doesn't support restarting, the job will go to failed.
	 * @throws Exception if fail to create the single region execution graph.
	 */
	@Test
	public void testNoManualRestart() throws Exception {
		NoRestartStrategy restartStrategy = new NoRestartStrategy();
		ExecutionGraph eg = createSingleRegionExecutionGraph(restartStrategy);

		ExecutionVertex ev = eg.getAllExecutionVertices().iterator().next();

		ev.fail(new Exception("Test Exception"));

		for (ExecutionVertex evs : eg.getAllExecutionVertices()) {
			evs.getCurrentExecutionAttempt().completeCancelling();
		}
		assertEquals(JobStatus.FAILED, eg.getState());
	}

	/**
	 * Tests that two regions failover at the same time, they will not influence each other.
	 * @throws Exception if fail to create dummy job information, fail to schedule for execution
	 * or timeout before region switches to expected status.
	 */
	@Test
	public void testMultiRegionFailoverAtSameTime() throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jobId, 16);

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
				slotProvider);
		try {
			eg.attachJobGraph(ordered);
		}
		catch (JobException e) {
			e.printStackTrace();
			fail("Job failed with exception: " + e.getMessage());
		}
		eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
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

		ev32.getCurrentExecutionAttempt().completeCancelling();
		waitUntilFailoverRegionState(strategy.getFailoverRegion(ev31), JobStatus.RUNNING, 1000);

		ev12.getCurrentExecutionAttempt().completeCancelling();
		waitUntilFailoverRegionState(strategy.getFailoverRegion(ev11), JobStatus.RUNNING, 1000);
	}

	/**
	 * Tests that if a task reports the result of its preceding task is failed,
	 * its preceding task will be considered as failed, and start to failover
	 * TODO: as the report part is not finished yet, this case is ignored temporarily
	 * @throws Exception if fail to create dummy job information or fail to schedule for execution.
	 */
	@Ignore
	@Test
	public void testSucceedingNoticePreceding() throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jobId, 14);

		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");

		v1.setParallelism(1);
		v2.setParallelism(1);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		ExecutionGraph eg = new ExecutionGraphTestUtils.TestingExecutionGraphBuilder(jobId, jobName, v1, v2)
			.setRestartStrategy(new InfiniteDelayRestartStrategy(10))
			.setFailoverStrategyFactory(new FailoverPipelinedRegionWithDirectExecutor())
			.setSlotProvider(slotProvider)
			.setScheduleMode(ScheduleMode.EAGER)
			.build();

		eg.scheduleForExecution();
		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();

		ExecutionVertex ev11 = eg.getJobVertex(v2.getID()).getTaskVertices()[0];
		ExecutionVertex ev21 = eg.getJobVertex(v2.getID()).getTaskVertices()[0];
		ev21.getCurrentExecutionAttempt().fail(new Exception("Fail with v1"));

		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev21).getState());
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev11).getState());
	}

	/**
	 * Tests that a new failure comes while the failover region is in CANCELLING.
	 * @throws Exception if fail to create the single region execution graph.
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
	 * Tests that a new failure comes while the failover region is restarting.
	 * @throws Exception if fail to create the single region execution graph.
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
			evs.getCurrentExecutionAttempt().completeCancelling();
		}
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(ev1).getState());

		ev1.getCurrentExecutionAttempt().fail(new Exception("new fail"));
		assertEquals(JobStatus.CANCELLING, strategy.getFailoverRegion(ev1).getState());
	}

	@Test
	public void testStatusResettingOnRegionFailover() throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		final SlotProvider slotProvider = new SimpleSlotProvider(jobId, 20);

		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");

		v1.setParallelism(2);
		v2.setParallelism(2);

		v1.setInvokableClass(AbstractInvokable.class);
		v2.setInvokableClass(AbstractInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		List<JobVertex> ordered = Arrays.asList(v1, v2);

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
		eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		RestartPipelinedRegionStrategy strategy = (RestartPipelinedRegionStrategy)eg.getFailoverStrategy();

		ExecutionVertex ev11 = eg.getJobVertex(v1.getID()).getTaskVertices()[0];
		ExecutionVertex ev12 = eg.getJobVertex(v1.getID()).getTaskVertices()[1];
		ExecutionVertex ev21 = eg.getJobVertex(v2.getID()).getTaskVertices()[0];
		ExecutionVertex ev22 = eg.getJobVertex(v2.getID()).getTaskVertices()[1];

		eg.scheduleForExecution();

		// initial state
		assertEquals(ExecutionState.DEPLOYING, ev11.getExecutionState());
		assertEquals(ExecutionState.DEPLOYING, ev12.getExecutionState());
		assertEquals(ExecutionState.CREATED, ev21.getExecutionState());
		assertEquals(ExecutionState.CREATED, ev22.getExecutionState());
		assertFalse(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].areAllPartitionsFinished());
		assertFalse(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].getPartitions()[0].isConsumable());
		assertFalse(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].getPartitions()[1].isConsumable());

		// partitions all finished
		ev11.getCurrentExecutionAttempt().markFinished();
		ev12.getCurrentExecutionAttempt().markFinished();
		assertEquals(ExecutionState.FINISHED, ev11.getExecutionState());
		assertEquals(ExecutionState.FINISHED, ev12.getExecutionState());
		assertEquals(ExecutionState.DEPLOYING, ev21.getExecutionState());
		assertEquals(ExecutionState.DEPLOYING, ev22.getExecutionState());
		assertTrue(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].areAllPartitionsFinished());
		assertTrue(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].getPartitions()[0].isConsumable());
		assertTrue(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].getPartitions()[1].isConsumable());

		// force the partition producer to restart
		strategy.onTaskFailure(ev11.getCurrentExecutionAttempt(), new FlinkException("Fail for testing"));
		assertFalse(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].areAllPartitionsFinished());
		assertFalse(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].getPartitions()[0].isConsumable());
		assertFalse(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].getPartitions()[1].isConsumable());

		// failed partition finishes again
		ev11.getCurrentExecutionAttempt().markFinished();
		assertTrue(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].areAllPartitionsFinished());
		assertTrue(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].getPartitions()[0].isConsumable());
		assertTrue(eg.getJobVertex(v1.getID()).getProducedDataSets()[0].getPartitions()[1].isConsumable());
	}

	// --------------------------------------------------------------------------------------------

	private void verifyCheckpointRestoredAsExpected(ExecutionGraph eg) throws Exception {
		// pending checkpoints have already been cancelled.
		assertNotNull(eg.getCheckpointCoordinator());
		assertTrue(eg.getCheckpointCoordinator().getPendingCheckpoints().isEmpty());

		// verify checkpoint has been restored successfully.
		assertEquals(1, eg.getCheckpointCoordinator().getCheckpointStore().getNumberOfRetainedCheckpoints());
		assertEquals(checkpointId, eg.getCheckpointCoordinator().getCheckpointStore().getLatestCheckpoint(false).getCheckpointID());
	}

	private ExecutionGraph createSingleRegionExecutionGraph(RestartStrategy restartStrategy) throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";

		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jobId, 14);

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
			slotProvider);
		try {
			eg.attachJobGraph(ordered);
		}
		catch (JobException e) {
			e.printStackTrace();
			fail("Job failed with exception: " + e.getMessage());
		}

		enableCheckpointing(eg);

		eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		eg.scheduleForExecution();

		attachPendingCheckpoints(eg);
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
			return new RestartPipelinedRegionStrategy(executionGraph);
		}
	}

	private static void enableCheckpointing(ExecutionGraph eg) {
		ArrayList<ExecutionJobVertex> jobVertices = new ArrayList<>(eg.getAllVertices().values());
		CheckpointCoordinatorConfiguration chkConfig = new CheckpointCoordinatorConfiguration(
			1000,
			100,
			0,
			1,
			CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION,
			true,
			false,
			0);
		eg.enableCheckpointing(
				chkConfig,
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

	/**
	 * Attach pending checkpoints of chk-42 and chk-43 to the execution graph.
	 * If {@link #acknowledgeAllCheckpoints(CheckpointCoordinator, Iterator)} called then,
	 * chk-42 would become the completed checkpoint.
	 */
	private void attachPendingCheckpoints(ExecutionGraph eg) throws IOException {
		final Map<Long, PendingCheckpoint> pendingCheckpoints = new HashMap<>();
		final Map<ExecutionAttemptID, ExecutionVertex> verticesToConfirm = new HashMap<>();
		eg.getAllExecutionVertices().forEach(e -> {
			Execution ee = e.getCurrentExecutionAttempt();
			if (ee != null) {
				verticesToConfirm.put(ee.getAttemptId(), e);
			}
		});

		CheckpointCoordinator checkpointCoordinator = eg.getCheckpointCoordinator();
		assertNotNull(checkpointCoordinator);
		CheckpointStorageCoordinatorView checkpointStorage = checkpointCoordinator.getCheckpointStorage();
		pendingCheckpoints.put(checkpointId, new PendingCheckpoint(
			eg.getJobID(),
			checkpointId,
			0L,
			verticesToConfirm,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			checkpointStorage.initializeLocationForCheckpoint(checkpointId),
			eg.getFutureExecutor()));

		long newCheckpointId = checkpointId + 1;
		pendingCheckpoints.put(newCheckpointId, new PendingCheckpoint(
			eg.getJobID(),
			newCheckpointId,
			0L,
			verticesToConfirm,
			CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE),
			checkpointStorage.initializeLocationForCheckpoint(newCheckpointId),
			eg.getFutureExecutor()));
		Whitebox.setInternalState(checkpointCoordinator, "pendingCheckpoints", pendingCheckpoints);
	}

	/**
	 * Let the checkpoint coordinator to receive all acknowledges from given executionVertexes so that to complete the expected checkpoint.
	 */
	private void acknowledgeAllCheckpoints(CheckpointCoordinator checkpointCoordinator, Iterator<ExecutionVertex> executionVertexes) throws IOException, CheckpointException {
		while (executionVertexes.hasNext()) {
			ExecutionVertex executionVertex = executionVertexes.next();
			for (int index = 0; index < executionVertex.getJobVertex().getParallelism(); index++) {
				JobVertexID jobVertexID = executionVertex.getJobvertexId();
				OperatorStateHandle opStateBackend = CheckpointCoordinatorTest.generatePartitionableStateHandle(jobVertexID, index, 2, 8, false);
				OperatorSubtaskState operatorSubtaskState = new OperatorSubtaskState(opStateBackend, null, null, null);
				TaskStateSnapshot taskOperatorSubtaskStates = new TaskStateSnapshot();
				taskOperatorSubtaskStates.putSubtaskStateByOperatorID(OperatorID.fromJobVertexID(jobVertexID), operatorSubtaskState);

				AcknowledgeCheckpoint acknowledgeCheckpoint = new AcknowledgeCheckpoint(
					executionVertex.getJobId(),
					executionVertex.getJobVertex().getTaskVertices()[index].getCurrentExecutionAttempt().getAttemptId(),
					checkpointId,
					new CheckpointMetrics(),
					taskOperatorSubtaskStates);

				checkpointCoordinator.receiveAcknowledgeMessage(acknowledgeCheckpoint, "Unknown location");
			}
		}
	}

	private static class CollectTddTaskManagerGateway extends SimpleAckingTaskManagerGateway {

		private final Map<ExecutionAttemptID, JobManagerTaskRestore> attemptIDInitStateMap;

		CollectTddTaskManagerGateway(Map<ExecutionAttemptID, JobManagerTaskRestore> attemptIDInitStateMap) {
			this.attemptIDInitStateMap = attemptIDInitStateMap;
		}

		@Override
		public CompletableFuture<Acknowledge> submitTask(TaskDeploymentDescriptor tdd, Time timeout) {
			attemptIDInitStateMap.put(tdd.getExecutionAttemptId(), tdd.getTaskRestore());
			return super.submitTask(tdd, timeout);
		}
	}

}
