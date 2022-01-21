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

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

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

    private static final SlotSharingGroup SLOT_SHARING_GROUP_1 = new SlotSharingGroup();
    private static final SlotSharingGroup SLOT_SHARING_GROUP_2 = new SlotSharingGroup();
    private static final JobInformation.VertexInformation VERTEX_1 =
            new TestVertexInformation(new JobVertexID(), 4, SLOT_SHARING_GROUP_1);
    private static final JobInformation.VertexInformation VERTEX_2 =
            new TestVertexInformation(new JobVertexID(), 2, SLOT_SHARING_GROUP_1);
    private static final JobInformation.VertexInformation VERTEX_3 =
            new TestVertexInformation(new JobVertexID(), 3, SLOT_SHARING_GROUP_2);

    @Test
    void testCalculateRequiredSlots() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);

        final ResourceCounter resourceCounter =
                slotAllocator.calculateRequiredSlots(Arrays.asList(VERTEX_1, VERTEX_2, VERTEX_3));

        assertThat(resourceCounter.getResources()).contains(ResourceProfile.UNKNOWN);
        assertThat(resourceCounter.getResourceCount(ResourceProfile.UNKNOWN))
                .isEqualTo(
                        Math.max(VERTEX_1.getParallelism(), VERTEX_2.getParallelism())
                                + VERTEX_3.getParallelism());
    }

    @Test
    void testDetermineParallelismWithMinimumSlots() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(VERTEX_1, VERTEX_2, VERTEX_3));

        final VertexParallelism slotSharingAssignments =
                slotAllocator.determineParallelism(jobInformation, getSlots(2)).get();

        final Map<JobVertexID, Integer> maxParallelismForVertices =
                slotSharingAssignments.getMaxParallelismForVertices();

        assertThat(maxParallelismForVertices.get(VERTEX_1.getJobVertexID())).isEqualTo(1);
        assertThat(maxParallelismForVertices.get(VERTEX_2.getJobVertexID())).isEqualTo(1);
        assertThat(maxParallelismForVertices.get(VERTEX_3.getJobVertexID())).isEqualTo(1);
    }

    @Test
    void testDetermineParallelismWithManySlots() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(VERTEX_1, VERTEX_2, VERTEX_3));

        final VertexParallelism slotSharingAssignments =
                slotAllocator.determineParallelism(jobInformation, getSlots(50)).get();

        final Map<JobVertexID, Integer> maxParallelismForVertices =
                slotSharingAssignments.getMaxParallelismForVertices();

        assertThat(maxParallelismForVertices.get(VERTEX_1.getJobVertexID()))
                .isEqualTo(VERTEX_1.getParallelism());
        assertThat(maxParallelismForVertices.get(VERTEX_2.getJobVertexID()))
                .isEqualTo(VERTEX_2.getParallelism());
        assertThat(maxParallelismForVertices.get(VERTEX_3.getJobVertexID()))
                .isEqualTo(VERTEX_3.getParallelism());
    }

    @Test
    void testDetermineParallelismUnsuccessfulWithLessSlotsThanSlotSharingGroups() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(VERTEX_1, VERTEX_2, VERTEX_3));

        final Optional<? extends VertexParallelism> slotSharingAssignments =
                slotAllocator.determineParallelism(jobInformation, getSlots(1));

        assertThat(slotSharingAssignments.isPresent()).isFalse();
    }

    @Test
    void testReserveAvailableResources() {
        final SlotSharingSlotAllocator slotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION,
                        TEST_FREE_SLOT_FUNCTION,
                        TEST_IS_SLOT_FREE_FUNCTION);

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(VERTEX_1, VERTEX_2, VERTEX_3));

        final VertexParallelismWithSlotSharing slotAssignments =
                (VertexParallelismWithSlotSharing)
                        slotAllocator.determineParallelism(jobInformation, getSlots(50)).get();

        final ReservedSlots reservedSlots =
                slotAllocator
                        .tryReserveResources(slotAssignments)
                        .orElseThrow(
                                () -> new RuntimeException("Expected that reservation succeeds."));

        final Map<ExecutionVertexID, SlotInfo> expectedAssignments = new HashMap<>();
        for (SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot assignment :
                slotAssignments.getAssignments()) {
            for (ExecutionVertexID containedExecutionVertex :
                    assignment.getExecutionSlotSharingGroup().getContainedExecutionVertices()) {
                expectedAssignments.put(containedExecutionVertex, assignment.getSlotInfo());
            }
        }

        for (Map.Entry<ExecutionVertexID, SlotInfo> expectedAssignment :
                expectedAssignments.entrySet()) {
            final LogicalSlot assignedSlot = reservedSlots.getSlotFor(expectedAssignment.getKey());

            final SlotInfo backingSlot = expectedAssignment.getValue();

            assertThat(assignedSlot.getAllocationId()).isEqualTo(backingSlot.getAllocationId());
        }
    }

    @Test
    void testReserveUnavailableResources() {
        final SlotSharingSlotAllocator slotSharingSlotAllocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION, TEST_FREE_SLOT_FUNCTION, ignored -> false);

        final JobInformation jobInformation =
                new TestJobInformation(Arrays.asList(VERTEX_1, VERTEX_2, VERTEX_3));

        final VertexParallelismWithSlotSharing slotAssignments =
                (VertexParallelismWithSlotSharing)
                        slotSharingSlotAllocator
                                .determineParallelism(jobInformation, getSlots(50))
                                .get();

        final Optional<? extends ReservedSlots> reservedSlots =
                slotSharingSlotAllocator.tryReserveResources(slotAssignments);

        assertThat(reservedSlots).isEmpty();
    }

    @Test
    void testXXX() throws Exception {
        final AllocationID[] slots = new AllocationID[32];
        for (int i = 0; i < slots.length; i++) {
            slots[i] = new AllocationID();
        }

        final SlotSharingGroup group = new SlotSharingGroup();
        final JobVertexID firstVertex = new JobVertexID();
        final JobVertexID secondVertex = new JobVertexID();
        final VertexParallelismWithSlotSharing assignment =
                new AllocationTester()
                        .addFreeSlots(slots)
                        .addVertex(firstVertex, group)
                        .addVertex(secondVertex, group)
                        .calculateAssignment();

        System.out.println(assignment);
    }

    public static class AllocationTester {

        private final SlotSharingSlotAllocator allocator =
                SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                        TEST_RESERVE_SLOT_FUNCTION, TEST_FREE_SLOT_FUNCTION, ignored -> false);

        private final List<SlotInfo> freeSlots = new ArrayList<>();
        private final List<TestVertexInformation> vertices = new ArrayList<>();

        public AllocationTester addFreeSlots(AllocationID... allocationIds) {
            for (AllocationID allocationId : allocationIds) {
                freeSlots.add(new TestSlotInfo(allocationId));
            }
            return this;
        }

        public AllocationTester addVertex(JobVertexID jobVertexId, SlotSharingGroup group) {
            vertices.add(new TestVertexInformation(jobVertexId, 3, group));
            return this;
        }

        public VertexParallelismWithSlotSharing calculateAssignment() {
            return allocator
                    .determineParallelismAndCalculateAssignment(
                            new TestJobInformation(vertices),
                            freeSlots,
                            new StateLocalitySlotAssigner(null))
                    .orElseThrow(
                            () -> new IllegalStateException("Unable to calculate assignment."));
        }
    }

    private static ArchivedExecutionJobVertex createArchivedExecutionJobVertex(
            JobVertexID jobVertexID, int parallelism, AllocationID allocationId) {
        final StringifiedAccumulatorResult[] emptyAccumulators =
                new StringifiedAccumulatorResult[0];
        final long[] timestamps = new long[ExecutionState.values().length];

        final LocalTaskManagerLocation assignedResourceLocation = new LocalTaskManagerLocation();

        final ArchivedExecutionVertex[] subtasks = new ArchivedExecutionVertex[parallelism];
        for (int subtaskIndex = 0; subtaskIndex < parallelism; subtaskIndex++) {
            subtasks[subtaskIndex] =
                    new ArchivedExecutionVertex(
                            subtaskIndex,
                            "test task",
                            new ArchivedExecution(
                                    emptyAccumulators,
                                    new IOMetrics(0L, 0L, 0L, 0L),
                                    new ExecutionAttemptID(),
                                    1,
                                    ExecutionState.CANCELED,
                                    null,
                                    assignedResourceLocation,
                                    subtaskIndex == 0 ? allocationId : new AllocationID(),
                                    subtaskIndex,
                                    timestamps),
                            new EvictingBoundedList<>(0));
        }
        return new ArchivedExecutionJobVertex(
                subtasks,
                jobVertexID,
                jobVertexID.toString(),
                parallelism,
                128,
                ResourceProfile.UNKNOWN,
                emptyAccumulators);
    }

    private static Collection<SlotInfo> getSlots(int count) {
        final Collection<SlotInfo> slotInfo = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            slotInfo.add(new TestSlotInfo());
        }
        return slotInfo;
    }

    private static class TestJobInformation implements JobInformation {

        private final Map<JobVertexID, VertexInformation> vertexIdToInformation;
        private final Collection<SlotSharingGroup> slotSharingGroups;

        private TestJobInformation(Collection<? extends VertexInformation> vertexIdToInformation) {
            this.vertexIdToInformation =
                    vertexIdToInformation.stream()
                            .collect(
                                    Collectors.toMap(
                                            VertexInformation::getJobVertexID,
                                            Function.identity()));
            this.slotSharingGroups =
                    vertexIdToInformation.stream()
                            .map(VertexInformation::getSlotSharingGroup)
                            .collect(Collectors.toSet());
        }

        @Override
        public Collection<SlotSharingGroup> getSlotSharingGroups() {
            return slotSharingGroups;
        }

        @Override
        public VertexInformation getVertexInformation(JobVertexID jobVertexId) {
            return vertexIdToInformation.get(jobVertexId);
        }
    }

    private static class TestVertexInformation implements JobInformation.VertexInformation {

        private final JobVertexID jobVertexId;
        private final int parallelism;
        private final SlotSharingGroup slotSharingGroup;

        private TestVertexInformation(
                JobVertexID jobVertexId, int parallelism, SlotSharingGroup slotSharingGroup) {
            this.jobVertexId = jobVertexId;
            this.parallelism = parallelism;
            this.slotSharingGroup = slotSharingGroup;
            slotSharingGroup.addVertexToGroup(jobVertexId);
        }

        @Override
        public JobVertexID getJobVertexID() {
            return jobVertexId;
        }

        @Override
        public int getParallelism() {
            return parallelism;
        }

        @Override
        public int getMaxParallelism() {
            return 128;
        }

        @Override
        public SlotSharingGroup getSlotSharingGroup() {
            return slotSharingGroup;
        }
    }
}
