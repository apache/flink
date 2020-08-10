/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.Scheduler;
import org.apache.flink.runtime.jobmaster.slotpool.SchedulerImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.TestingSlotPoolImpl;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

/**
 * Tests ExecutionGraph schedule behavior with not enough resource.
 */
public class ExecutionGraphNotEnoughResourceTest extends TestLogger {

	private static TestingComponentMainThreadExecutor.Resource mainThreadExecutorResource;
	private static ComponentMainThreadExecutor mainThreadExecutor;


	private static final JobID TEST_JOB_ID = new JobID();
	private static final int NUM_TASKS = 31;

	@BeforeClass
	public static void setupClass() {
		mainThreadExecutorResource = new TestingComponentMainThreadExecutor.Resource();
		mainThreadExecutorResource.before();
		mainThreadExecutor = mainThreadExecutorResource.getComponentMainThreadTestExecutor().getMainThreadExecutor();
	}

	@AfterClass
	public static void teardownClass() {
		mainThreadExecutorResource.after();
	}

	@Test
	public void testRestartWithSlotSharingAndNotEnoughResources() throws Exception {
		final int numRestarts = 10;
		final int parallelism = 20;

		SlotPool slotPool = null;
		try {
			slotPool = new TestingSlotPoolImpl(TEST_JOB_ID);
			final Scheduler scheduler = createSchedulerWithSlots(
				parallelism - 1, slotPool, new LocalTaskManagerLocation());

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

			final JobGraph jobGraph = new JobGraph(TEST_JOB_ID, "Test Job", source, sink);
			jobGraph.setScheduleMode(ScheduleMode.EAGER);

			TestRestartStrategy restartStrategy = new TestRestartStrategy(numRestarts, false);

			final ExecutionGraph eg = TestingExecutionGraphBuilder
				.newBuilder()
				.setJobGraph(jobGraph)
				.setSlotProvider(scheduler)
				.setRestartStrategy(restartStrategy)
				.setAllocationTimeout(Time.milliseconds(1L))
				.build();

			eg.start(mainThreadExecutor);

			mainThreadExecutor.execute(ThrowingRunnable.unchecked(eg::scheduleForExecution));

			CommonTestUtils.waitUntilCondition(
				() -> CompletableFuture.supplyAsync(eg::getState, mainThreadExecutor).join() == JobStatus.FAILED,
				Deadline.fromNow(Duration.ofSeconds(10)));

			// the last suppressed restart is also counted
			assertEquals(numRestarts + 1, CompletableFuture.supplyAsync(eg::getNumberOfRestarts, mainThreadExecutor).join().longValue());

			final Throwable t = CompletableFuture.supplyAsync(eg::getFailureCause, mainThreadExecutor).join();
			if (!(t instanceof NoResourceAvailableException)) {
				ExceptionUtils.rethrowException(t, t.getMessage());
			}
		} finally {
			if (slotPool != null) {
				CompletableFuture.runAsync(slotPool::close, mainThreadExecutor).join();
			}
		}
	}

	private static Scheduler createSchedulerWithSlots(
			int numSlots,
			SlotPool slotPool,
			TaskManagerLocation taskManagerLocation) throws Exception {
		final TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		final String jobManagerAddress = "foobar";
		final ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		slotPool.start(JobMasterId.generate(), jobManagerAddress, mainThreadExecutor);
		slotPool.connectToResourceManager(resourceManagerGateway);
		Scheduler scheduler = new SchedulerImpl(LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
		scheduler.start(mainThreadExecutor);

		CompletableFuture.runAsync(() -> slotPool.registerTaskManager(taskManagerLocation.getResourceID()), mainThreadExecutor).join();

		final List<SlotOffer> slotOffers = new ArrayList<>(NUM_TASKS);
		for (int i = 0; i < numSlots; i++) {
			final AllocationID allocationId = new AllocationID();
			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.ANY);
			slotOffers.add(slotOffer);
		}

		CompletableFuture.runAsync(() -> slotPool.offerSlots(taskManagerLocation, taskManagerGateway, slotOffers), mainThreadExecutor).join();

		return scheduler;
	}
}
