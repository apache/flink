/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobAllocationsInformation.VertexAllocationInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation.VertexInformation;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;

/** {@link StateLocalitySlotAssigner} test. */
class StateLocalitySlotAssignerTest {

    @Test
    public void testDownScaleWithUnevenStateSize() {
        int newParallelism = 1;
        VertexInformation vertex = createVertex(newParallelism);
        AllocationID allocationWith100bytes = new AllocationID();
        AllocationID allocationWith200bytes = new AllocationID();

        List<VertexAllocationInformation> allocations =
                Arrays.asList(
                        new VertexAllocationInformation(
                                allocationWith100bytes,
                                vertex.getJobVertexID(),
                                KeyGroupRange.of(0, 99),
                                1),
                        new VertexAllocationInformation(
                                allocationWith200bytes,
                                vertex.getJobVertexID(),
                                KeyGroupRange.of(100, 100),
                                200));

        Collection<SlotAssignment> assignments =
                assign(
                        vertex,
                        Arrays.asList(allocationWith100bytes, allocationWith200bytes),
                        allocations);

        verifyAssignments(assignments, newParallelism, allocationWith200bytes);
    }

    @Test
    // In case of local recovery, we want to preserve slot allocations even if there is no
    // keyed managed state available.
    public void testSlotsPreservationWithNoStateSameParallelism() {
        final int parallelism = 2;
        final VertexInformation vertex = createVertex(parallelism);
        final AllocationID allocationID1 = new AllocationID();
        final AllocationID allocationID2 = new AllocationID();

        final List<VertexAllocationInformation> previousAllocations =
                Arrays.asList(
                        new VertexAllocationInformation(
                                allocationID1, vertex.getJobVertexID(), KeyGroupRange.of(0, 63), 0),
                        new VertexAllocationInformation(
                                allocationID2,
                                vertex.getJobVertexID(),
                                KeyGroupRange.of(64, 127),
                                0));

        final Collection<SlotAssignment> assignments =
                assign(
                        vertex,
                        // Providing allocation IDs in reverse order to check that assigner fixes
                        // the order based on previous allocations.
                        Arrays.asList(allocationID2, allocationID1),
                        previousAllocations);

        // Extract allocation IDs from assignments sorted by subtask index.
        final List<AllocationID> subtaskOrderedNewAllocations =
                assignments.stream()
                        .sorted(
                                Comparator.comparingInt(
                                        assignment ->
                                                assignment
                                                        .getTargetAs(
                                                                SlotSharingSlotAllocator
                                                                        .ExecutionSlotSharingGroup
                                                                        .class)
                                                        .getContainedExecutionVertices()
                                                        .stream()
                                                        .mapToInt(
                                                                ExecutionVertexID::getSubtaskIndex)
                                                        .findAny()
                                                        .orElseThrow()))
                        .map(assignment -> assignment.getSlotInfo().getAllocationId())
                        .collect(Collectors.toList());

        assertThat(subtaskOrderedNewAllocations).containsExactly(allocationID1, allocationID2);
    }

    @Test
    void testSlotsAreNotWasted() {
        VertexInformation vertex = createVertex(2);
        AllocationID alloc1 = new AllocationID();
        AllocationID alloc2 = new AllocationID();

        List<VertexAllocationInformation> allocations =
                Arrays.asList(
                        new VertexAllocationInformation(
                                alloc1, vertex.getJobVertexID(), KeyGroupRange.of(0, 9), 1),
                        new VertexAllocationInformation(
                                alloc2, vertex.getJobVertexID(), KeyGroupRange.of(10, 19), 1));

        assign(vertex, Arrays.asList(alloc1, alloc2), allocations);
    }

    @Test
    void testUpScaling() {
        final int oldParallelism = 3;
        final int newParallelism = 7;
        final int numFreeSlots = 100;
        final VertexInformation vertex = createVertex(newParallelism);
        final List<AllocationID> allocationIDs = createAllocationIDS(numFreeSlots);

        List<VertexAllocationInformation> prevAllocations = new ArrayList<>();
        Iterator<AllocationID> iterator = allocationIDs.iterator();
        for (int i = 0; i < oldParallelism; i++) {
            prevAllocations.add(
                    new VertexAllocationInformation(
                            iterator.next(),
                            vertex.getJobVertexID(),
                            KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                                    vertex.getMaxParallelism(), oldParallelism, i),
                            1));
        }

        Collection<SlotAssignment> assignments = assign(vertex, allocationIDs, prevAllocations);

        verifyAssignments(
                assignments,
                newParallelism,
                prevAllocations.stream()
                        .map(VertexAllocationInformation::getAllocationID)
                        .toArray(AllocationID[]::new));
    }

    @Test
    void testDownScaling() {
        final int oldParallelism = 5;
        final int newParallelism = 1;
        final int numFreeSlots = 100;
        final VertexInformation vertex = createVertex(newParallelism);
        final List<AllocationID> allocationIDs = createAllocationIDS(numFreeSlots);

        // pretend that the 1st (0) subtask had half of key groups ...
        final Iterator<AllocationID> iterator = allocationIDs.iterator();
        final AllocationID biggestAllocation = iterator.next();
        final List<VertexAllocationInformation> prevAllocations = new ArrayList<>();
        final int halfOfKeyGroupRange = vertex.getMaxParallelism() / 2;
        prevAllocations.add(
                new VertexAllocationInformation(
                        biggestAllocation,
                        vertex.getJobVertexID(),
                        KeyGroupRange.of(0, halfOfKeyGroupRange - 1),
                        1));

        // and the remaining subtasks had only one key group each
        for (int subtaskIdx = 1; subtaskIdx < oldParallelism; subtaskIdx++) {
            int keyGroup = halfOfKeyGroupRange + subtaskIdx;
            prevAllocations.add(
                    new VertexAllocationInformation(
                            iterator.next(),
                            vertex.getJobVertexID(),
                            KeyGroupRange.of(keyGroup, keyGroup),
                            1));
        }

        Collection<SlotAssignment> assignments = assign(vertex, allocationIDs, prevAllocations);

        verifyAssignments(assignments, newParallelism, biggestAllocation);
    }

    private static void verifyAssignments(
            Collection<SlotAssignment> assignments,
            int expectedSize,
            AllocationID... mustHaveAllocationID) {
        MatcherAssert.assertThat(assignments, hasSize(expectedSize));
        MatcherAssert.assertThat(
                assignments.stream()
                        .map(e -> e.getSlotInfo().getAllocationId())
                        .collect(Collectors.toSet()),
                hasItems(mustHaveAllocationID));
    }

    private static Collection<SlotAssignment> assign(
            VertexInformation vertexInformation,
            List<AllocationID> allocationIDs,
            List<VertexAllocationInformation> allocations) {
        return new StateLocalitySlotAssigner()
                .assignSlots(
                        new TestJobInformation(singletonList(vertexInformation)),
                        allocationIDs.stream().map(TestingSlot::new).collect(Collectors.toList()),
                        new VertexParallelism(
                                singletonMap(
                                        vertexInformation.getJobVertexID(),
                                        vertexInformation.getParallelism())),
                        new JobAllocationsInformation(
                                singletonMap(vertexInformation.getJobVertexID(), allocations)));
    }

    private static VertexInformation createVertex(int parallelism) {
        JobVertexID id = new JobVertexID();
        SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        slotSharingGroup.addVertexToGroup(id);
        return new TestVertexInformation(id, parallelism, slotSharingGroup);
    }

    private static List<AllocationID> createAllocationIDS(int numFreeSlots) {
        return IntStream.range(0, numFreeSlots)
                .mapToObj(i -> new AllocationID())
                .collect(Collectors.toList());
    }
}
