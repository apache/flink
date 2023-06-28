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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobAllocationsInformation.VertexAllocationInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.topology.VertexID;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/** Tests for the {@link SlotSharingSlotAllocator}. */
public class SlotSharingSlotAllocatorTest extends TestLogger {

    private static final FreeSlotFunction TEST_FREE_SLOT_FUNCTION = (a, c, t) -> {};
    private static final ReserveSlotFunction TEST_RESERVE_SLOT_FUNCTION =
            (allocationId, resourceProfile) ->
                    TestingPhysicalSlot.builder()
                            .withAllocationID(allocationId)
                            .withResourceProfile(resourceProfile)
                            .build();
    private static final IsSlotAvailableAndFreeFunction TEST_IS_SLOT_FREE_FUNCTION =
            ignored -> true;

    private static final SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
    private static final SlotSharingGroup slotSharingGroup2 = new SlotSharingGroup();
    private static final JobInformation.VertexInformation vertex1 =
            new TestVertexInformation(new JobVertexID(), 4, slotSharingGroup1);
    private static final JobInformation.VertexInformation vertex2 =
            new TestVertexInformation(new JobVertexID(), 2, slotSharingGroup1);
    private static final JobInformation.VertexInformation vertex3 =
            new TestVertexInformation(new JobVertexID(), 3, slotSharingGroup2);

    @Test
    public void testCalculateRequiredSlots() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);

        final ResourceCounter resourceCounter =
                slotAllocator.calculateRequiredSlots(Arrays.asList(vertex1, vertex2, vertex3));

        assertThat(resourceCounter.getResources()).contains(ResourceProfile.UNKNOWN);
        assertThat(resourceCounter.getResourceCount(ResourceProfile.UNKNOWN))
                .isEqualTo(
                        Math.max(vertex1.getParallelism(), vertex2.getParallelism())
                                + vertex3.getParallelism());
    }

    @Test
    public void testDetermineParallelismWithMinimumSlots() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2, vertex3));

        final VertexParallelism vertexParallelism =
                slotAllocator.determineParallelism(jobInformation, getSlots(2)).get();

        assertThat(vertexParallelism.getParallelism(vertex1.getJobVertexID()), is(1));
        assertThat(vertexParallelism.getParallelism(vertex2.getJobVertexID()), is(1));
        assertThat(vertexParallelism.getParallelism(vertex3.getJobVertexID()), is(1));
    }

    @Test
    public void testDetermineParallelismWithManySlots() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2, vertex3));

        final VertexParallelism vertexParallelism =
                slotAllocator.determineParallelism(jobInformation, getSlots(50)).get();

        assertThat(
                vertexParallelism.getParallelism(vertex1.getJobVertexID()),
                is(vertex1.getParallelism()));
        assertThat(
                vertexParallelism.getParallelism(vertex2.getJobVertexID()),
                is(vertex2.getParallelism()));
        assertThat(
                vertexParallelism.getParallelism(vertex3.getJobVertexID()),
                is(vertex3.getParallelism()));
    }

    @Test
    public void testDetermineParallelismWithVariedParallelism() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);
        final SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
        final JobInformation.VertexInformation vertex11 =
                new TestVertexInformation(new JobVertexID(), 4, slotSharingGroup1);
        final JobInformation.VertexInformation vertex12 =
                new TestVertexInformation(new JobVertexID(), 1, slotSharingGroup1);
        final JobInformation.VertexInformation vertex2 =
                new TestVertexInformation(new JobVertexID(), 2, new SlotSharingGroup());

        TestJobInformation testJobInformation =
                new TestJobInformation(Arrays.asList(vertex11, vertex12, vertex2));

        VertexParallelism vertexParallelism =
                slotAllocator
                        .determineParallelism(
                                testJobInformation,
                                getSlots(vertex11.getParallelism() + vertex2.getParallelism()))
                        .get();

        Assertions.assertThat(vertexParallelism.getParallelism(vertex11.getJobVertexID()))
                .isEqualTo(vertex11.getParallelism());
        Assertions.assertThat(vertexParallelism.getParallelism(vertex12.getJobVertexID()))
                .isEqualTo(vertex12.getParallelism());
        Assertions.assertThat(vertexParallelism.getParallelism(vertex2.getJobVertexID()))
                .isEqualTo(vertex2.getParallelism());
    }

    @Test
    public void testDetermineParallelismUnsuccessfulWithLessSlotsThanSlotSharingGroups() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2, vertex3));

        final Optional<VertexParallelism> vertexParallelism =
                slotAllocator.determineParallelism(jobInformation, getSlots(1));

        assertThat(vertexParallelism.isPresent(), is(false));
    }

    @Test
    public void testDetermineParallelismWithPartiallyEqualLowerUpperBound() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);
        final JobInformation.VertexInformation vertex1 =
                new TestVertexInformation(new JobVertexID(), 1, 8, new SlotSharingGroup());
        final JobInformation.VertexInformation vertex2 =
                new TestVertexInformation(new JobVertexID(), 10, 10, new SlotSharingGroup());

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2));

        final Optional<VertexParallelism> vertexParallelism =
                slotAllocator.determineParallelism(jobInformation, getSlots(13));

        assertThat(vertexParallelism)
                .hasValueSatisfying(
                        vertexParallelism1 -> {
                            assertThat(vertexParallelism1.getParallelism(vertex1.getJobVertexID()))
                                    .isEqualTo(3);
                            assertThat(vertexParallelism1.getParallelism(vertex2.getJobVertexID()))
                                    .isEqualTo(10);
                        });
    }

    @Test
    public void testDetermineParallelismWithLowerBoundsInsufficientSlots() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);
        final JobInformation.VertexInformation vertex1 =
                new TestVertexInformation(new JobVertexID(), 4, 4, new SlotSharingGroup());
        final JobInformation.VertexInformation vertex2 =
                new TestVertexInformation(new JobVertexID(), 10, 10, new SlotSharingGroup());

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2));

        final Optional<VertexParallelism> vertexParallelism =
                slotAllocator.determineParallelism(jobInformation, getSlots(3));

        assertThat(vertexParallelism).isNotPresent();
    }

    @Test
    public void testDetermineParallelismWithAllEqualLowerUpperBoundFreSlots() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);
        final JobInformation.VertexInformation vertex1 =
                new TestVertexInformation(new JobVertexID(), 4, 10, new SlotSharingGroup());
        final JobInformation.VertexInformation vertex2 =
                new TestVertexInformation(new JobVertexID(), 4, 10, new SlotSharingGroup());

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2));

        final Optional<VertexParallelism> vertexParallelism =
                slotAllocator.determineParallelism(
                        jobInformation,
                        getSlots(vertex1.getMinParallelism() + vertex2.getMinParallelism()));

        assertThat(vertexParallelism)
                .hasValueSatisfying(
                        vertexParallelism1 -> {
                            assertThat(vertexParallelism1.getParallelism(vertex1.getJobVertexID()))
                                    .isEqualTo(vertex1.getMinParallelism());
                            assertThat(vertexParallelism1.getParallelism(vertex2.getJobVertexID()))
                                    .isEqualTo(vertex2.getMinParallelism());
                        });
    }

    @Test
    public void testDetermineParallelismWithAllEqualLowerUpperBoundManySlots() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);
        final JobInformation.VertexInformation vertex1 =
                new TestVertexInformation(new JobVertexID(), 4, 4, new SlotSharingGroup());
        final JobInformation.VertexInformation vertex2 =
                new TestVertexInformation(new JobVertexID(), 10, 10, new SlotSharingGroup());

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2));

        final Optional<VertexParallelism> vertexParallelism =
                slotAllocator.determineParallelism(
                        jobInformation,
                        getSlots(vertex1.getMinParallelism() + vertex2.getMinParallelism() + 12));

        assertThat(vertexParallelism)
                .hasValueSatisfying(
                        vertexParallelism1 -> {
                            assertThat(vertexParallelism1.getParallelism(vertex1.getJobVertexID()))
                                    .isEqualTo(vertex1.getMinParallelism());
                            assertThat(vertexParallelism1.getParallelism(vertex2.getJobVertexID()))
                                    .isEqualTo(vertex2.getMinParallelism());
                        });
    }

    @Test
    public void testReserveAvailableResources() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2, vertex3));

        final JobSchedulingPlan jobSchedulingPlan =
                slotAllocator
                        .determineParallelismAndCalculateAssignment(
                                jobInformation, getSlots(50), JobAllocationsInformation.empty())
                        .get();

        final ReservedSlots reservedSlots =
                slotAllocator
                        .tryReserveResources(jobSchedulingPlan)
                        .orElseThrow(
                                () -> new RuntimeException("Expected that reservation succeeds."));

        final Map<ExecutionVertexID, SlotInfo> expectedAssignments = new HashMap<>();
        for (JobSchedulingPlan.SlotAssignment assignment : jobSchedulingPlan.getSlotAssignments()) {
            ExecutionSlotSharingGroup target =
                    assignment.getTargetAs(ExecutionSlotSharingGroup.class);
            for (ExecutionVertexID containedExecutionVertex :
                    target.getContainedExecutionVertices()) {
                expectedAssignments.put(containedExecutionVertex, assignment.getSlotInfo());
            }
        }

        for (Map.Entry<ExecutionVertexID, SlotInfo> expectedAssignment :
                expectedAssignments.entrySet()) {
            final LogicalSlot assignedSlot = reservedSlots.getSlotFor(expectedAssignment.getKey());

            final SlotInfo backingSlot = expectedAssignment.getValue();

            assertThat(assignedSlot.getAllocationId(), is(backingSlot.getAllocationId()));
        }
    }

    @Test
    public void testReserveUnavailableResources() {
        final SlotSharingSlotAllocator slotSharingSlotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION, TEST_FREE_SLOT_FUNCTION, ignored -> false);

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2, vertex3));

        JobSchedulingPlan jobSchedulingPlan =
                slotSharingSlotAllocator
                        .determineParallelismAndCalculateAssignment(
                                jobInformation, getSlots(50), JobAllocationsInformation.empty())
                        .get();

        final Optional<? extends ReservedSlots> reservedSlots =
                slotSharingSlotAllocator.tryReserveResources(jobSchedulingPlan);

        assertFalse(reservedSlots.isPresent());
    }

    /**
     * Basic test to verify that allocation takes previous allocations into account to facilitate
     * Local Recovery.
     */
    @Test
    public void testStickyAllocation() {
        Map<JobVertexID, List<VertexAllocationInformation>> locality = new HashMap<>();

        // previous allocation allocation1: v1, v2
        AllocationID allocation1 = new AllocationID();
        locality.put(
                vertex1.getJobVertexID(),
                Collections.singletonList(
                        new VertexAllocationInformation(
                                allocation1, vertex1.getJobVertexID(), KeyGroupRange.of(1, 100))));
        locality.put(
                vertex2.getJobVertexID(),
                Collections.singletonList(
                        new VertexAllocationInformation(
                                allocation1, vertex2.getJobVertexID(), KeyGroupRange.of(1, 100))));

        // previous allocation allocation2: v3
        AllocationID allocation2 = new AllocationID();
        locality.put(
                vertex3.getJobVertexID(),
                Collections.singletonList(
                        new VertexAllocationInformation(
                                allocation2, vertex3.getJobVertexID(), KeyGroupRange.of(1, 100))));

        List<SlotInfo> freeSlots = new ArrayList<>();
        IntStream.range(0, 10).forEach(i -> freeSlots.add(new TestSlotInfo(new AllocationID())));
        freeSlots.add(new TestSlotInfo(allocation1));
        freeSlots.add(new TestSlotInfo(allocation2));

        Map<JobVertexID, Long> stateSizes = new HashMap<>();
        stateSizes.put(vertex1.getJobVertexID(), 10L);
        stateSizes.put(vertex2.getJobVertexID(), 10L);
        stateSizes.put(vertex3.getJobVertexID(), 10L);

        JobSchedulingPlan schedulingPlan =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                                (allocationId, resourceProfile) ->
                                        TestingPhysicalSlot.builder().build(),
                                (allocationID, cause, ts) -> {},
                                id -> false)
                        .determineParallelismAndCalculateAssignment(
                                new TestJobInformation(Arrays.asList(vertex1, vertex2, vertex3)),
                                freeSlots,
                                new JobAllocationsInformation(locality))
                        .get();

        Map<AllocationID, Set<VertexID>> allocated = new HashMap<>();
        for (JobSchedulingPlan.SlotAssignment assignment : schedulingPlan.getSlotAssignments()) {
            ExecutionSlotSharingGroup target =
                    assignment.getTargetAs(ExecutionSlotSharingGroup.class);
            Set<VertexID> set =
                    allocated.computeIfAbsent(
                            assignment.getSlotInfo().getAllocationId(), ign -> new HashSet<>());
            for (ExecutionVertexID id : target.getContainedExecutionVertices()) {
                set.add(id.getJobVertexId());
            }
        }
        assertThat(allocated.get(allocation1)).contains(vertex1.getJobVertexID());
        assertThat(allocated.get(allocation1)).contains(vertex2.getJobVertexID());
        assertThat(allocated.get(allocation2)).contains(vertex3.getJobVertexID());
    }

    private static Collection<SlotInfo> getSlots(int count) {
        final Collection<SlotInfo> slotInfo = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            slotInfo.add(new TestSlotInfo());
        }
        return slotInfo;
    }
}
