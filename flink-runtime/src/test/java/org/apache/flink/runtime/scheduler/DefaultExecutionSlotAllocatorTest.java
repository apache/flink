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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingSlotProvider;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link DefaultExecutionSlotAllocator}.
 */
public class DefaultExecutionSlotAllocatorTest extends TestLogger {

	private DefaultExecutionSlotAllocator executionSlotAllocator;

	private TestingSlotProvider slotProvider;

	private ExecutionGraph executionGraph;

	private List<ExecutionVertexSchedulingRequirements> schedulingRequirements;

	private Queue<CompletableFuture<LogicalSlot>> slotFutures;

	private List<SlotRequestId> receivedSlotRequestIds;

	private List<SlotRequestId> cancelledSlotRequestIds;

	private List<ExecutionVertexID> executionVertexIds;

	private int executionVerticesNum;

	@Before
	public void setUp() throws Exception {
		executionGraph = createSimpleProducerConsumerGraph();

		executionVerticesNum = executionGraph.getTotalNumberOfVertices();
		receivedSlotRequestIds = new ArrayList<>(executionVerticesNum);
		cancelledSlotRequestIds = new ArrayList<>(executionVerticesNum);
		slotFutures = new ArrayDeque<>(executionVerticesNum);

		slotProvider = new TestingSlotProvider(slotRequestId -> {
			receivedSlotRequestIds.add(slotRequestId);
			return slotFutures.poll();
		});
		slotProvider.setSlotCanceller(slotRequestId -> cancelledSlotRequestIds.add(slotRequestId));

		executionSlotAllocator = new DefaultExecutionSlotAllocator(slotProvider, executionGraph);

		schedulingRequirements = new ArrayList<>(executionVerticesNum);
		executionVertexIds = new ArrayList<>(executionVerticesNum);

		for (ExecutionVertex executionVertex : executionGraph.getAllExecutionVertices()) {
			ExecutionVertexID executionVertexId =
					new ExecutionVertexID(executionVertex.getJobvertexId(), executionVertex.getParallelSubtaskIndex());
			schedulingRequirements.add(new ExecutionVertexSchedulingRequirements(
					executionVertexId,
					null,
					ResourceProfile.UNKNOWN,
					new SlotSharingGroupId(),
					null,
					null,
					null
			));
			executionVertexIds.add(executionVertexId);
			slotFutures.add(new CompletableFuture<>());
		}
	}

	/**
	 * Tests that it will allocate slots from slot provider and remove the slot assignments when request are fulfilled.
	 */
	@Test
	public void testAllocateSlotsFor() {
		List<CompletableFuture<LogicalSlot>> backupSlotFutures = new ArrayList<>(slotFutures);

		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		assertThat(receivedSlotRequestIds, hasSize(executionVerticesNum / 2));
		assertThat(executionSlotAllocator.getPendingSlotAssignments().keySet(), containsInAnyOrder(executionVertexIds.toArray()));

		completeProducerSlotRequest(backupSlotFutures);

		assertThat(receivedSlotRequestIds, hasSize(executionVerticesNum));
	}

	/**
	 * Tests that when cancelling a slot request, the request to slot provider should also be cancelled.
	 */
	@Test
	public void testCancel() {
		List<CompletableFuture<LogicalSlot>> backupSlotFutures = new ArrayList<>(slotFutures);

		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		// cancel a non-existing execution vertex
		ExecutionVertexID inValidExecutionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
		executionSlotAllocator.cancel(inValidExecutionVertexId);
		assertThat(cancelledSlotRequestIds, hasSize(0));

		assertThat(receivedSlotRequestIds, hasSize(executionVerticesNum / 2));

		completeProducerSlotRequest(backupSlotFutures);

		for (ExecutionVertexID executionVertexId : executionVertexIds) {
			executionSlotAllocator.cancel(executionVertexId);
		}

		// only the consumers slot request will be cancelled
		List<SlotRequestId> expectCancelledSlotRequestIds =
				receivedSlotRequestIds.subList(executionVerticesNum / 2, executionVerticesNum);
		assertThat(cancelledSlotRequestIds, containsInAnyOrder(expectCancelledSlotRequestIds.toArray()));
	}

	/**
	 * Tests that all unfulfilled slot requests will be cancelled when stopped.
	 */
	@Test
	public void testStop() {
		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		// only the producers will ask for slots as the preferred location futures of consumers are not fulfilled.
		assertThat(executionSlotAllocator.getPendingSlotAssignments().keySet(), hasSize(executionVerticesNum));
		assertThat(receivedSlotRequestIds, hasSize(executionVerticesNum / 2));

		executionSlotAllocator.stop().getNow(null);

		assertThat(cancelledSlotRequestIds, hasSize(executionVerticesNum / 2));
		assertThat(cancelledSlotRequestIds, containsInAnyOrder(receivedSlotRequestIds.toArray()));
		assertThat(executionSlotAllocator.getPendingSlotAssignments().keySet(), hasSize(0));
	}

	/**
	 * Tests that all prior allocation ids are computed by union all previous allocation ids in scheduling requirements.
	 */
	@Test
	public void testComputeAllPriorAllocationIds() {
		List<AllocationID> expectAllocationIds = Arrays.asList(new AllocationID(), new AllocationID());
		List<ExecutionVertexSchedulingRequirements> testSchedulingRequirements = Arrays.asList(
				new ExecutionVertexSchedulingRequirements(
						executionVertexIds.get(0),
						expectAllocationIds.get(0),
						ResourceProfile.UNKNOWN,
						null,
						null,
						null,
						null),
				new ExecutionVertexSchedulingRequirements(
						executionVertexIds.get(1),
						expectAllocationIds.get(0),
						ResourceProfile.UNKNOWN,
						null,
						null,
						null,
						null),
				new ExecutionVertexSchedulingRequirements(
						executionVertexIds.get(2),
						expectAllocationIds.get(1),
						ResourceProfile.UNKNOWN,
						null,
						null,
						null,
						null),
				new ExecutionVertexSchedulingRequirements(
						executionVertexIds.get(3),
						null,
						ResourceProfile.UNKNOWN,
						null,
						null,
						null,
						null)
		);

		Set<AllocationID> allPriorAllocationIds = executionSlotAllocator.computeAllPriorAllocationIds(testSchedulingRequirements);
		assertThat(allPriorAllocationIds, containsInAnyOrder(expectAllocationIds.toArray()));
	}

	/**
	 * Tests the calculation of preferred locations based on inputs for an execution.
	 */
	@Test
	public void testGetPreferredLocationsBasedOnInputs() throws Exception {
		TestingSlotProvider testingSlotProvider = new TestingSlotProvider((slotRequestId) -> new CompletableFuture<>());

		JobVertex producer1 = new JobVertex("producer1");
		producer1.setInvokableClass(NoOpInvokable.class);
		producer1.setParallelism(10);

		JobVertex producer2 = new JobVertex("producer2");
		producer2.setInvokableClass(NoOpInvokable.class);
		producer2.setParallelism(5);

		JobVertex producer3 = new JobVertex("producer3");
		producer3.setInvokableClass(NoOpInvokable.class);
		producer3.setParallelism(3);

		JobVertex consumer = new JobVertex("consumer");
		consumer.setInvokableClass(NoOpInvokable.class);
		consumer.setParallelism(1);

		consumer.connectNewDataSetAsInput(producer1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		consumer.connectNewDataSetAsInput(producer2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		consumer.connectNewDataSetAsInput(producer3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(new JobID(), producer1, producer2, producer3, consumer);

		Set<ExecutionVertexID> executionVertexIds = new HashSet<>(eg.getTotalNumberOfVertices());
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			executionVertexIds.add(new ExecutionVertexID(ev.getJobvertexId(), ev.getParallelSubtaskIndex()));
		}
		Execution execution = eg.getJobVertex(consumer.getID()).getTaskVertices()[0].getCurrentExecutionAttempt();

		// all producers have not been started.
		Collection<CompletableFuture<TaskManagerLocation>> preferredLocations1 =
				DefaultExecutionSlotAllocator.getPreferredLocationsBasedOnInputs(execution, Collections.emptySet());
		assertThat(preferredLocations1, hasSize(0));

		// all producers scheduled together, will use the locations of the inputs whose parallelism is smallest.
		Collection<CompletableFuture<TaskManagerLocation>> preferredLocations2 =
				DefaultExecutionSlotAllocator.getPreferredLocationsBasedOnInputs(execution, executionVertexIds);
		assertThat(preferredLocations2, hasSize(3));

		for (ExecutionVertex ev : eg.getJobVertex(producer1.getID()).getTaskVertices()) {
			ev.scheduleForExecution(testingSlotProvider, true, LocationPreferenceConstraint.ALL, Collections.EMPTY_SET);
		}

		// if source have too many different locations, it will be ignored
		Collection<CompletableFuture<TaskManagerLocation>> preferredLocations3 =
				DefaultExecutionSlotAllocator.getPreferredLocationsBasedOnInputs(execution, Collections.EMPTY_SET);
		assertThat(preferredLocations3, hasSize(0));

		List<TaskManagerLocation> locationsForProducer2 = new ArrayList<>(producer2.getParallelism());
		for (ExecutionVertex ev : eg.getJobVertex(producer2.getID()).getTaskVertices()) {
			ev.scheduleForExecution(testingSlotProvider, true, LocationPreferenceConstraint.ALL, Collections.EMPTY_SET);
			TaskManagerLocation location = new LocalTaskManagerLocation();
			ev.getCurrentTaskManagerLocationFuture().complete(location);
			locationsForProducer2.add(location);
		}

		// use the locations of input that have been stated
		Collection<CompletableFuture<TaskManagerLocation>> preferredLocations4 =
				DefaultExecutionSlotAllocator.getPreferredLocationsBasedOnInputs(execution, Collections.EMPTY_SET);
		assertThat(FutureUtils.combineAll(preferredLocations4).getNow(null), containsInAnyOrder(locationsForProducer2.toArray()));

		List<TaskManagerLocation> locationsForProducer3 = new ArrayList<>(producer3.getParallelism());
		for (ExecutionVertex ev : eg.getJobVertex(producer3.getID()).getTaskVertices()) {
			ev.scheduleForExecution(testingSlotProvider, true, LocationPreferenceConstraint.ALL, Collections.EMPTY_SET);
			TaskManagerLocation location = new LocalTaskManagerLocation();
			ev.getCurrentTaskManagerLocationFuture().complete(location);
			locationsForProducer3.add(location);
		}

		// use the locations of input whose parallelism is smaller
		Collection<CompletableFuture<TaskManagerLocation>> preferredLocations5 =
				DefaultExecutionSlotAllocator.getPreferredLocationsBasedOnInputs(execution, Collections.EMPTY_SET);
		assertThat(FutureUtils.combineAll(preferredLocations5).getNow(null), containsInAnyOrder(locationsForProducer3.toArray()));
	}

	private void completeProducerSlotRequest(List<CompletableFuture<LogicalSlot>> totalSlotFutures) {
		// fulfill the producers and the location preferences of consumers, so the consumers will allocate slots
		for (int i = 0; i < executionVerticesNum / 2; i++) {
			LogicalSlot slot = new TestingLogicalSlot();
			totalSlotFutures.get(i).complete(slot);
			ExecutionJobVertex ejv = executionGraph.getJobVertex(executionVertexIds.get(i).getJobVertexId());
			Execution execution = ejv.getTaskVertices()[executionVertexIds.get(i).getSubtaskIndex()].getCurrentExecutionAttempt();
			execution.getTaskManagerLocationFuture().complete(slot.getTaskManagerLocation());
		}
	}

	static ExecutionGraph createSimpleProducerConsumerGraph() throws Exception {
		int parallelism = 3;

		JobVertex producer = new JobVertex("producer");
		producer.setInvokableClass(NoOpInvokable.class);
		producer.setParallelism(parallelism);

		JobVertex consumer = new JobVertex("consumer");
		consumer.setInvokableClass(NoOpInvokable.class);
		consumer.setParallelism(parallelism);

		consumer.connectNewDataSetAsInput(producer, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		return ExecutionGraphTestUtils.createSimpleTestGraph(new JobID(), producer, consumer);
	}
}
