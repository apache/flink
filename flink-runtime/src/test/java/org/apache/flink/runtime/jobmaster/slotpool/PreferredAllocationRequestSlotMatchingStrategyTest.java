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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link PreferredAllocationRequestSlotMatchingStrategy}. */
@ExtendWith(TestLoggerExtension.class)
class PreferredAllocationRequestSlotMatchingStrategyTest {

    private static final ResourceProfile finedGrainProfile =
            ResourceProfile.newBuilder().setCpuCores(1d).build();

    private static final AllocationID allocationId1 = new AllocationID();
    private static final AllocationID allocationId2 = new AllocationID();

    private static final TaskManagerLocation tmLocation1 = new LocalTaskManagerLocation();
    private static final TaskManagerLocation tmLocation2 = new LocalTaskManagerLocation();

    // Slots on task-executor-1.
    private static final TestingPhysicalSlot slot1OfTm1 =
            createSlot(ResourceProfile.ANY, allocationId1, tmLocation1);
    private static final TestingPhysicalSlot slot2OfTm1 = createSlotAndAnyProfile(tmLocation1);
    private static final TestingPhysicalSlot slot3OfTm1 = createSlotAndAnyProfile(tmLocation1);
    private static final TestingPhysicalSlot slot4OfTm1 = createSlotAndGrainProfile(tmLocation1);
    private static final TestingPhysicalSlot slot5OfTm1 = createSlotAndGrainProfile(tmLocation1);

    // Slots on task-executor-2.
    private static final TestingPhysicalSlot slot6OfTm2 =
            createSlot(finedGrainProfile, allocationId2, tmLocation2);
    private static final TestingPhysicalSlot slot7OfTm2 = createSlotAndAnyProfile(tmLocation2);
    private static final TestingPhysicalSlot slot8OfTm2 = createSlotAndAnyProfile(tmLocation2);
    private static final TestingPhysicalSlot slot9OfTm2 = createSlotAndGrainProfile(tmLocation2);
    private static final TestingPhysicalSlot slot10OfTm2 = createSlotAndGrainProfile(tmLocation2);

    // Slot requests.
    private static final PendingRequest request1 = createRequest(2, allocationId1);
    private static final PendingRequest request2 =
            createRequest(finedGrainProfile, 3, allocationId2);
    private static final PendingRequest request3 = createRequest(2, null);
    private static final PendingRequest request4 = createRequest(1, null);
    private static final PendingRequest request5 = createRequest(finedGrainProfile, 3, null);
    private static final PendingRequest request6 = createRequest(finedGrainProfile, 5, null);

    /**
     * This test ensures that new slots are matched against the preferred allocationIds of the
     * pending requests.
     */
    @Test
    void testNewSlotsAreMatchedAgainstPreferredAllocationIDs() {
        final RequestSlotMatchingStrategy strategy =
                PreferredAllocationRequestSlotMatchingStrategy.create(
                        SimpleRequestSlotMatchingStrategy.INSTANCE);

        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();

        final Collection<TestingPhysicalSlot> slots =
                Arrays.asList(
                        TestingPhysicalSlot.builder().withAllocationID(allocationId1).build(),
                        TestingPhysicalSlot.builder().withAllocationID(allocationId2).build());
        final Collection<PendingRequest> pendingRequests =
                Arrays.asList(
                        PendingRequest.createNormalRequest(
                                new SlotRequestId(),
                                ResourceProfile.UNKNOWN,
                                DefaultLoadingWeight.EMPTY,
                                Collections.singleton(allocationId2)),
                        PendingRequest.createNormalRequest(
                                new SlotRequestId(),
                                ResourceProfile.UNKNOWN,
                                DefaultLoadingWeight.EMPTY,
                                Collections.singleton(allocationId1)));

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                strategy.matchRequestsAndSlots(slots, pendingRequests, new HashMap<>());

        assertThat(requestSlotMatches).hasSize(2);

        for (RequestSlotMatchingStrategy.RequestSlotMatch requestSlotMatch : requestSlotMatches) {
            assertThat(requestSlotMatch.getPendingRequest().getPreferredAllocations())
                    .contains(requestSlotMatch.getSlot().getAllocationId());
        }
    }

    /**
     * This test ensures that new slots are matched on {@link
     * PreviousAllocationSlotSelectionStrategy} and {@link
     * TasksBalancedRequestSlotMatchingStrategy}.
     */
    @Test
    void testNewSlotsAreMatchedAgainstAllocationAndBalancedPreferredIDs() {
        final RequestSlotMatchingStrategy strategy =
                PreferredAllocationRequestSlotMatchingStrategy.create(
                        TasksBalancedRequestSlotMatchingStrategy.INSTANCE);

        final Collection<PendingRequest> pendingRequests =
                Arrays.asList(request1, request2, request3, request4, request5, request6);
        List<TestingPhysicalSlot> slots = getSlots();
        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                strategy.matchRequestsAndSlots(slots, pendingRequests, new HashMap<>());
        final Map<TaskManagerLocation, List<SlotRequestId>> expectedResult = getExpectedResult();

        assertThat(requestSlotMatches).hasSize(6);

        expectedResult.forEach(
                (taskManagerLocation, slotRequestIds) -> {
                    List<SlotRequestId> requestIdsOfTm =
                            requestSlotMatches.stream()
                                    .filter(
                                            requestSlotMatch ->
                                                    requestSlotMatch
                                                            .getSlot()
                                                            .getTaskManagerLocation()
                                                            .equals(taskManagerLocation))
                                    .map(
                                            requestSlotMatch ->
                                                    requestSlotMatch
                                                            .getPendingRequest()
                                                            .getSlotRequestId())
                                    .collect(Collectors.toList());
                    assertThat(requestIdsOfTm).containsAll(slotRequestIds);
                });
    }

    private static Map<TaskManagerLocation, List<SlotRequestId>> getExpectedResult() {
        final Map<TaskManagerLocation, List<SlotRequestId>> expectedResult = new HashMap<>(2);
        expectedResult.put(
                tmLocation1,
                Arrays.asList(
                        request1.getSlotRequestId(),
                        request6.getSlotRequestId(),
                        request4.getSlotRequestId()));
        expectedResult.put(
                tmLocation2,
                Arrays.asList(
                        request2.getSlotRequestId(),
                        request5.getSlotRequestId(),
                        request3.getSlotRequestId()));
        return expectedResult;
    }

    private static List<TestingPhysicalSlot> getSlots() {
        return Arrays.asList(
                slot1OfTm1,
                slot2OfTm1,
                slot3OfTm1,
                slot4OfTm1,
                slot5OfTm1,
                slot6OfTm2,
                slot7OfTm2,
                slot8OfTm2,
                slot9OfTm2,
                slot10OfTm2);
    }

    private static PendingRequest createRequest(
            ResourceProfile requestProfile,
            float loading,
            @Nullable AllocationID preferAllocationId) {
        final List<AllocationID> preferAllocationIds =
                Objects.isNull(preferAllocationId)
                        ? Collections.emptyList()
                        : Collections.singletonList(preferAllocationId);
        return PendingRequest.createNormalRequest(
                new SlotRequestId(),
                requestProfile,
                new DefaultLoadingWeight(loading),
                preferAllocationIds);
    }

    private static PendingRequest createRequest(
            float loading, @Nullable AllocationID preferAllocationId) {
        return createRequest(ResourceProfile.UNKNOWN, loading, preferAllocationId);
    }

    private static TestingPhysicalSlot createSlotAndAnyProfile(TaskManagerLocation tmLocation) {
        return createSlot(ResourceProfile.ANY, new AllocationID(), tmLocation);
    }

    private static TestingPhysicalSlot createSlotAndGrainProfile(TaskManagerLocation tmLocation) {
        return createSlot(finedGrainProfile, new AllocationID(), tmLocation);
    }

    static TestingPhysicalSlot createSlot(
            ResourceProfile profile, AllocationID allocationId, TaskManagerLocation tmLocation) {
        return TestingPhysicalSlot.builder()
                .withAllocationID(allocationId)
                .withTaskManagerLocation(tmLocation)
                .withResourceProfile(profile)
                .build();
    }
}
