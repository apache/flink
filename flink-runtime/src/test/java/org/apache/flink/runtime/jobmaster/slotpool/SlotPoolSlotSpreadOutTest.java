/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Tests for the {@link SchedulerImpl} and {@link SlotPoolImpl} which verify
 * the spread out of slots.
 */
public class SlotPoolSlotSpreadOutTest extends TestLogger {

	public static final Time TIMEOUT = Time.seconds(10L);

	@Rule
	public final SlotPoolResource slotPoolResource = new SlotPoolResource(LocationPreferenceSlotSelectionStrategy.createEvenlySpreadOut());

	@Test
	public void allocateSingleSlot_withNoRequirements_selectsSlotSoThatWorkloadIsSpreadOut() {
		registerTaskExecutors(2, 4);

		final ScheduledUnit firstSlotRequest = createSimpleSlotRequest();
		final ScheduledUnit secondSlotRequest = createSimpleSlotRequest();

		final CompletableFuture<LogicalSlot> firstSlotFuture = allocateSlot(firstSlotRequest);
		final CompletableFuture<LogicalSlot> secondSlotFuture = allocateSlot(secondSlotRequest);

		final TaskManagerLocation firstTaskManagerLocation = getTaskManagerLocation(firstSlotFuture);
		final TaskManagerLocation secondTaskManagerLocation = getTaskManagerLocation(secondSlotFuture);

		assertThat(firstTaskManagerLocation, is(not(equalTo(secondTaskManagerLocation))));
	}

	@Test
	public void allocateSingleSlot_withInputPreference_inputPreferenceHasPrecedenceOverSpreadOut() {
		registerTaskExecutors(2, 2);

		final ScheduledUnit sourceSlotRequest = createSimpleSlotRequest();
		final ScheduledUnit sinkSlotRequest = createSimpleSlotRequest();

		final CompletableFuture<LogicalSlot> sourceSlotFuture = allocateSlot(sourceSlotRequest);
		final TaskManagerLocation sourceTaskManagerLocation = getTaskManagerLocation(sourceSlotFuture);

		Collection<TaskManagerLocation> preferredLocations = Collections.singleton(sourceTaskManagerLocation);
		final CompletableFuture<LogicalSlot> sinkSlotFuture = allocateSlotWithInputPreference(sinkSlotRequest, preferredLocations);
		final TaskManagerLocation sinkTaskManagerLocation = getTaskManagerLocation(sinkSlotFuture);

		// input preference should have precedence over task spread out
		assertThat(sinkTaskManagerLocation, is(equalTo(sourceTaskManagerLocation)));
	}

	@Test
	public void allocateSharedSlot_withNoRequirements_selectsSlotsSoThatWorkloadIsSpreadOut() {
		final int numberSlotsPerTaskExecutor = 2;
		final int numberTaskExecutors = 2;
		final int numberSlots = numberTaskExecutors * numberSlotsPerTaskExecutor;

		registerTaskExecutors(numberTaskExecutors, numberSlotsPerTaskExecutor);

		final JobVertexID sourceJobVertexId = new JobVertexID();
		final JobVertexID sinkJobVertexId = new JobVertexID();
		final SlotSharingGroupId slotSharingGroupId = new SlotSharingGroupId();

		final List<ScheduledUnit> sourceScheduledUnits = IntStream.range(0, numberSlots)
			.mapToObj(ignored -> createSharedSlotRequest(sourceJobVertexId, slotSharingGroupId))
			.collect(Collectors.toList());

		final List<ScheduledUnit> sinkScheduledUnits = IntStream.range(0, numberTaskExecutors)
			.mapToObj(ignored -> createSharedSlotRequest(sinkJobVertexId, slotSharingGroupId))
			.collect(Collectors.toList());

		sourceScheduledUnits.forEach(this::allocateSlot);
		final Set<TaskManagerLocation> sinkLocations = sinkScheduledUnits.stream()
			.map(this::allocateSlot)
			.map(this::getTaskManagerLocation)
			.collect(Collectors.toSet());

		// verify that the sinks have been evenly spread across the available TaskExecutors
		assertThat(sinkLocations, hasSize(numberTaskExecutors));
	}

	private ScheduledUnit createSharedSlotRequest(JobVertexID jobVertexId, SlotSharingGroupId slotSharingGroupId) {
		return new ScheduledUnit(jobVertexId, slotSharingGroupId, null);
	}

	private ScheduledUnit createSimpleSlotRequest() {
		return new ScheduledUnit(new JobVertexID(), null, null);
	}

	private CompletableFuture<LogicalSlot> allocateSlot(ScheduledUnit scheduledUnit) {
		return internalAllocateSlot(scheduledUnit, SlotProfile.noRequirements());
	}

	private CompletableFuture<LogicalSlot> internalAllocateSlot(ScheduledUnit scheduledUnit, SlotProfile slotProfile) {
		SlotProvider slotProvider = slotPoolResource.getSlotProvider();
		return slotProvider.allocateSlot(
			new SlotRequestId(),
			scheduledUnit,
			slotProfile,
			TIMEOUT);
	}

	private CompletableFuture<LogicalSlot> allocateSlotWithInputPreference(ScheduledUnit scheduledUnit, Collection<TaskManagerLocation> preferredLocations) {
		return internalAllocateSlot(scheduledUnit, SlotProfile.preferredLocality(ResourceProfile.UNKNOWN, preferredLocations));
	}

	private TaskManagerLocation getTaskManagerLocation(CompletableFuture<? extends LogicalSlot> slotFuture) {
		return slotFuture.join().getTaskManagerLocation();
	}

	private void registerTaskExecutors(int numberTaskExecutors, int numberSlotsPerTaskExecutor) {
		for (int i = 0; i < numberTaskExecutors; i++) {
			registerTaskExecutor(numberSlotsPerTaskExecutor);
		}
	}

	private void registerTaskExecutor(int numberSlotsPerTaskExecutor) {
		final SlotPool slotPool = slotPoolResource.getSlotPool();
		final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();

		slotPool.registerTaskManager(taskManagerLocation.getResourceID());

		final Collection<SlotOffer> slotOffers = IntStream
			.range(0, numberSlotsPerTaskExecutor)
			.mapToObj(index -> new SlotOffer(new AllocationID(), index, ResourceProfile.ANY))
			.collect(Collectors.toList());

		slotPool.offerSlots(taskManagerLocation, new SimpleAckingTaskManagerGateway(), slotOffers);
	}
}
