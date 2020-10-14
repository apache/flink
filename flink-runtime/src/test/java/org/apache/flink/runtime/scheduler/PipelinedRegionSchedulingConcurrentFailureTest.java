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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolUtils;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Test that concurrent failures will not result in unexpected crash
 * when using pipelined region scheduling.
 */
public class PipelinedRegionSchedulingConcurrentFailureTest extends TestLogger {

	private static ScheduledExecutorService singleThreadScheduledExecutorService;
	private static ComponentMainThreadExecutor mainThreadExecutor;

	private static final JobID JID = new JobID();
	private static final JobVertexID VID1 = new JobVertexID();
	private static final JobVertexID VID2 = new JobVertexID();
	private static final JobVertexID VID3 = new JobVertexID();
	private static final JobVertexID VID4 = new JobVertexID();

	@BeforeClass
	public static void setupClass() {
		singleThreadScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
		mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(singleThreadScheduledExecutorService);
	}

	@AfterClass
	public static void teardownClass() {
		if (singleThreadScheduledExecutorService != null) {
			singleThreadScheduledExecutorService.shutdownNow();
		}
	}

	/**
	 * This mocks the problematic case of FLINK-19552.
	 */
	@Test
	public void testNoImmediateSlotAllocationFailureOnConcurrentFailure() throws Exception {
		final SchedulerBase scheduler = createAndStartScheduler();

		ExecutionVertex ev11 = scheduler.getExecutionJobVertex(VID1).getTaskVertices()[0];
		ExecutionVertex ev12 = scheduler.getExecutionJobVertex(VID1).getTaskVertices()[1];
		ExecutionVertex ev21 = scheduler.getExecutionJobVertex(VID2).getTaskVertices()[0];
		ExecutionVertex ev22 = scheduler.getExecutionJobVertex(VID2).getTaskVertices()[1];

		// wait until DEPLOYING to make task CANCELING controllable
		ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev11, ExecutionState.DEPLOYING, 2000L);
		ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev12, ExecutionState.DEPLOYING, 2000L);
		ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev21, ExecutionState.DEPLOYING, 2000L);
		ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev22, ExecutionState.DEPLOYING, 2000L);

		// trigger failover of ev11 first
		mainThreadExecutor.execute(() -> scheduler.updateTaskExecutionState(
			new TaskExecutionState(
				JID,
				ev11.getCurrentExecutionAttempt().getAttemptId(),
				ExecutionState.FAILED,
				new Exception("Forced ev11 failure"))));

		// trigger failover of ev12 to supersede the restarting of tasks of v3 and v4.
		// Thus the canceling completion of ev11 failover will not reset tasks of v3 and v4 to CREATED.
		// they will stay in CANCELED with their canceled location futures.
		mainThreadExecutor.execute(() -> scheduler.updateTaskExecutionState(
			new TaskExecutionState(
				JID,
				ev12.getCurrentExecutionAttempt().getAttemptId(),
				ExecutionState.FAILED,
				new Exception("Forced ev12 failure"))));

		ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev21, ExecutionState.CANCELING, 2000L);
		ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev22, ExecutionState.CANCELING, 2000L);

		// complete canceling of ev21 to trigger the restart of ev11 and ev21
		mainThreadExecutor.execute(() -> scheduler.updateTaskExecutionState(
			new TaskExecutionState(
				JID,
				ev21.getCurrentExecutionAttempt().getAttemptId(),
				ExecutionState.CANCELED)));

		ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev11, ExecutionState.DEPLOYING, 2000L);
		ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev21, ExecutionState.DEPLOYING, 2000L);
	}

	private JobGraph createJobGraph() {
		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();

		final JobVertex v1 = new JobVertex("v1", VID1);
		v1.setInvokableClass(NoOpInvokable.class);
		v1.setParallelism(2);
		v1.setSlotSharingGroup(slotSharingGroup);

		final JobVertex v2 = new JobVertex("v2", VID2);
		v2.setInvokableClass(NoOpInvokable.class);
		v2.setParallelism(2);
		v2.setSlotSharingGroup(slotSharingGroup);
		v2.connectNewDataSetAsInput(v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobVertex v3 = new JobVertex("v3", VID3);
		v3.setInvokableClass(NoOpInvokable.class);
		v3.setParallelism(2);
		v3.setSlotSharingGroup(slotSharingGroup);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

		final JobVertex v4 = new JobVertex("v4", VID4);
		v4.setInvokableClass(NoOpInvokable.class);
		v4.setParallelism(2);
		v4.setSlotSharingGroup(slotSharingGroup);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		return new JobGraph(JID, "Test job", v1, v2, v3, v4);
	}

	private SchedulerBase createAndStartScheduler() throws Exception {
		final TestingJobMasterPartitionTracker partitionTracker = new TestingJobMasterPartitionTracker();
		partitionTracker.setIsPartitionTrackedFunction(ignored -> true);
		final SlotPoolImpl slotPool = new SlotPoolBuilder(mainThreadExecutor).build();
		SlotPoolUtils.offerSlots(slotPool, mainThreadExecutor, Arrays.asList(ResourceProfile.ANY, ResourceProfile.ANY));

		final DefaultSchedulerComponents components = DefaultSchedulerComponents.createSchedulerComponents(
			ScheduleMode.LAZY_FROM_SOURCES_WITH_BATCH_SLOT_REQUEST,
			new Configuration(),
			slotPool,
			Time.milliseconds(10L));
		final SchedulerBase scheduler = SchedulerTestingUtils
			.newSchedulerBuilder(createJobGraph())
			.setRestartBackoffTimeStrategy(new TestRestartBackoffTimeStrategy(true, 0))
			.setPartitionTracker(partitionTracker)
			.setDefaultSchedulerComponents(components)
			.build();

		scheduler.setMainThreadExecutor(mainThreadExecutor);
		mainThreadExecutor.execute(scheduler::startScheduling);

		return scheduler;
	}
}
