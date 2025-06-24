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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link org.apache.flink.runtime.scheduler.MergingSharedSlotProfileRetrieverFactory}.
 */
class MergingSharedSlotProfileRetrieverTest {

    private static final SyncPreferredLocationsRetriever EMPTY_PREFERRED_LOCATIONS_RETRIEVER =
            (executionVertexId, producersToIgnore) -> Collections.emptyList();

    @Test
    void testGetEmptySlotProfile() {
        SharedSlotProfileRetriever sharedSlotProfileRetriever =
                new MergingSharedSlotProfileRetrieverFactory(
                                EMPTY_PREFERRED_LOCATIONS_RETRIEVER,
                                executionVertexID -> Optional.of(new AllocationID()),
                                () -> Collections.emptySet())
                        .createFromBulk(Collections.emptySet());

        SlotProfile slotProfile =
                sharedSlotProfileRetriever.getSlotProfile(
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()),
                        ResourceProfile.ZERO);

        assertThat(slotProfile.getTaskResourceProfile()).isEqualTo(ResourceProfile.ZERO);
        assertThat(slotProfile.getPhysicalSlotResourceProfile()).isEqualTo(ResourceProfile.ZERO);
        assertThat(slotProfile.getPreferredLocations()).isEmpty();
        assertThat(slotProfile.getPreferredAllocations()).isEmpty();
        assertThat(slotProfile.getReservedAllocations()).isEmpty();
    }

    @Test
    void testResourceProfileOfSlotProfile() {
        ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemory(MemorySize.ofMebiBytes(1))
                        .build();
        SlotProfile slotProfile =
                getSlotProfile(resourceProfile, Collections.nCopies(3, new AllocationID()), 2);

        assertThat(slotProfile.getTaskResourceProfile()).isEqualTo(resourceProfile);
        assertThat(slotProfile.getPhysicalSlotResourceProfile()).isEqualTo(resourceProfile);
    }

    @Test
    void testPreferredLocationsOfSlotProfile() {
        // preferred locations
        List<ExecutionVertexID> executions =
                IntStream.range(0, 3)
                        .mapToObj(i -> new ExecutionVertexID(new JobVertexID(), 0))
                        .collect(Collectors.toList());

        List<TaskManagerLocation> allLocations =
                executions.stream()
                        .map(e -> createTaskManagerLocation())
                        .collect(Collectors.toList());
        Map<ExecutionVertexID, Collection<TaskManagerLocation>> locations = new HashMap<>();
        locations.put(executions.get(0), Arrays.asList(allLocations.get(0), allLocations.get(1)));
        locations.put(executions.get(1), Arrays.asList(allLocations.get(1), allLocations.get(2)));

        List<AllocationID> prevAllocationIds = Collections.nCopies(3, new AllocationID());
        SlotProfile slotProfile =
                getSlotProfile(
                        (executionVertexId, producersToIgnore) -> {
                            assertThat(producersToIgnore).containsAll(executions);
                            return locations.get(executionVertexId);
                        },
                        executions,
                        ResourceProfile.ZERO,
                        prevAllocationIds,
                        prevAllocationIds,
                        2);

        assertThat(
                        slotProfile.getPreferredLocations().stream()
                                .filter(allLocations.get(0)::equals)
                                .count())
                .isOne();
        assertThat(
                        slotProfile.getPreferredLocations().stream()
                                .filter(allLocations.get(1)::equals)
                                .count())
                .isEqualTo(2L);
        assertThat(
                        slotProfile.getPreferredLocations().stream()
                                .filter(allLocations.get(2)::equals)
                                .count())
                .isOne();
    }

    @Test
    void testPreferredAllocationsOfSlotProfile() {
        AllocationID prevAllocationID1 = new AllocationID();
        AllocationID prevAllocationID2 = new AllocationID();
        List<AllocationID> prevAllocationIDs =
                Arrays.asList(prevAllocationID1, prevAllocationID2, new AllocationID());

        SlotProfile slotProfile = getSlotProfile(ResourceProfile.ZERO, prevAllocationIDs, 2);

        assertThat(slotProfile.getPreferredAllocations())
                .contains(prevAllocationID1, prevAllocationID2);
    }

    @Test
    void testReservedAllocationsOfSlotProfile() {
        List<AllocationID> reservedAllocationIds =
                Arrays.asList(new AllocationID(), new AllocationID(), new AllocationID());

        SlotProfile slotProfile =
                getSlotProfile(
                        EMPTY_PREFERRED_LOCATIONS_RETRIEVER,
                        Collections.emptyList(),
                        ResourceProfile.ZERO,
                        Collections.emptyList(),
                        reservedAllocationIds,
                        0);

        assertThat(slotProfile.getReservedAllocations()).containsAll(reservedAllocationIds);
    }

    private static SlotProfile getSlotProfile(
            ResourceProfile resourceProfile,
            List<AllocationID> prevAllocationIDs,
            int executionSlotSharingGroupSize) {
        List<ExecutionVertexID> executions =
                prevAllocationIDs.stream()
                        .map(stub -> new ExecutionVertexID(new JobVertexID(), 0))
                        .collect(Collectors.toList());
        return getSlotProfile(
                EMPTY_PREFERRED_LOCATIONS_RETRIEVER,
                executions,
                resourceProfile,
                prevAllocationIDs,
                prevAllocationIDs,
                executionSlotSharingGroupSize);
    }

    private static SlotProfile getSlotProfile(
            SyncPreferredLocationsRetriever preferredLocationsRetriever,
            List<ExecutionVertexID> executions,
            ResourceProfile resourceProfile,
            List<AllocationID> prevAllocationIDs,
            Collection<AllocationID> reservedAllocationIds,
            int executionSlotSharingGroupSize) {
        SharedSlotProfileRetriever sharedSlotProfileRetriever =
                new MergingSharedSlotProfileRetrieverFactory(
                                preferredLocationsRetriever,
                                executionVertexID ->
                                        Optional.ofNullable(
                                                prevAllocationIDs.get(
                                                        executions.indexOf(executionVertexID))),
                                () -> new HashSet<>(reservedAllocationIds))
                        .createFromBulk(new HashSet<>(executions));

        ExecutionSlotSharingGroup executionSlotSharingGroup =
                new ExecutionSlotSharingGroup(new SlotSharingGroup());
        executions.stream()
                .limit(executionSlotSharingGroupSize)
                .forEach(executionSlotSharingGroup::addVertex);
        return sharedSlotProfileRetriever.getSlotProfile(
                executionSlotSharingGroup, resourceProfile);
    }

    private static TaskManagerLocation createTaskManagerLocation() {
        try {
            return new TaskManagerLocation(
                    ResourceID.generate(), InetAddress.getByAddress(new byte[] {1, 2, 3, 4}), 8888);
        } catch (UnknownHostException e) {
            throw new FlinkRuntimeException("unexpected", e);
        }
    }
}
