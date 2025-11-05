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
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collection;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import static org.assertj.core.api.Assertions.assertThat;

/** Base testing class for the implementations of {@link SlotMatchingResolver}. */
abstract class AbstractSlotMatchingResolverTest {

    public static final SlotSharingGroup SLOT_SHARING_GROUP = new SlotSharingGroup();

    protected final TaskManagerLocation tml1 = new LocalTaskManagerLocation();
    protected final TestingSlot slot1OfTml1 = createAnySlotOf(tml1);
    protected final TestingSlot slot2OfTml1 = createAnySlotOf(tml1);
    protected final TestingSlot slot3OfTml1 = createAnySlotOf(tml1);

    protected final TaskManagerLocation tml2 = new LocalTaskManagerLocation();
    protected final TestingSlot slot1OfTml2 = createAnySlotOf(tml2);
    protected final TestingSlot slot2OfTml2 = createAnySlotOf(tml2);
    protected final TestingSlot slot3OfTml2 = createAnySlotOf(tml2);

    protected final TaskManagerLocation tml3 = new LocalTaskManagerLocation();
    protected final TestingSlot slot1OfTml3 = createAnySlotOf(tml3);
    protected final TestingSlot slot2OfTml3 = createAnySlotOf(tml3);
    protected final TestingSlot slot3OfTml3 = createAnySlotOf(tml3);

    protected final ExecutionSlotSharingGroup requestGroup1 = createGroup(1);
    protected final ExecutionSlotSharingGroup requestGroup2 = createGroup(2);
    protected final ExecutionSlotSharingGroup requestGroup3 = createGroup(3);
    protected final ExecutionSlotSharingGroup requestGroup4 = createGroup(4);
    protected final ExecutionSlotSharingGroup requestGroup5 = createGroup(5);
    protected final ExecutionSlotSharingGroup requestGroup6 = createGroup(6);
    protected final ExecutionSlotSharingGroup requestGroup7 = createGroup(7);

    protected final List<PhysicalSlot> freeSlots =
            Arrays.asList(
                    slot1OfTml1,
                    slot2OfTml1,
                    slot3OfTml1,
                    slot1OfTml2,
                    slot2OfTml2,
                    slot3OfTml2,
                    slot1OfTml3,
                    slot2OfTml3,
                    slot3OfTml3);

    protected final List<ExecutionSlotSharingGroup> requestedGroups =
            Arrays.asList(
                    requestGroup1,
                    requestGroup2,
                    requestGroup3,
                    requestGroup4,
                    requestGroup5,
                    requestGroup6,
                    requestGroup7);

    protected SlotMatchingResolver slotMatchingResolver;

    @BeforeEach
    protected void setUp() {
        this.slotMatchingResolver = createSlotMatchingResolver();
    }

    protected abstract SlotMatchingResolver createSlotMatchingResolver();

    protected abstract void assertAssignments(Collection<SlotAssignment> assignments);

    @Test
    void testMatchSlotSharingGroupWithSlots() {
        Collection<SlotAssignment> slotAssignments =
                slotMatchingResolver.matchSlotSharingGroupWithSlots(requestedGroups, freeSlots);
        assertAssignments(slotAssignments);
    }

    protected static @Nonnull Map<TaskManagerLocation, Set<SlotAssignment>>
            getAssignmentsPerTaskManager(Collection<SlotAssignment> assignments) {
        return assignments.stream()
                .collect(
                        Collectors.groupingBy(
                                assignment -> assignment.getSlotInfo().getTaskManagerLocation(),
                                Collectors.toSet()));
    }

    private static TestingSlot createAnySlotOf(TaskManagerLocation tml) {
        return new TestingSlot(new AllocationID(), ResourceProfile.ANY, tml);
    }

    private static ExecutionSlotSharingGroup createGroup(int executionVertices) {
        return new ExecutionSlotSharingGroup(
                SLOT_SHARING_GROUP,
                IntStream.range(0, executionVertices)
                        .mapToObj(ignored -> new ExecutionVertexID(new JobVertexID(), 0))
                        .collect(Collectors.toSet()));
    }
}

/** Test for {@link TasksBalancedSlotMatchingResolver}. */
class TasksBalancedSlotMatchingResolverTest extends AbstractSlotMatchingResolverTest {

    @Override
    protected SlotMatchingResolver createSlotMatchingResolver() {
        return TasksBalancedSlotMatchingResolver.INSTANCE;
    }

    @Override
    protected void assertAssignments(Collection<SlotAssignment> assignments) {
        Map<TaskManagerLocation, Set<SlotAssignment>> assignmentsPerTm =
                getAssignmentsPerTaskManager(assignments);
        assertThat(assignmentsPerTm)
                .allSatisfy(
                        (taskManagerLocation, slotAssignments) -> {
                            assertThat(
                                            slotAssignments.stream()
                                                    .map(
                                                            s ->
                                                                    s.getTargetAs(
                                                                                    ExecutionSlotSharingGroup
                                                                                            .class)
                                                                            .getLoading())
                                                    .reduce(
                                                            DefaultLoadingWeight.EMPTY,
                                                            LoadingWeight::merge)
                                                    .getLoading())
                                    .isGreaterThanOrEqualTo(9f);
                        });
    }
}

/** Test for {@link SlotsBalancedSlotMatchingResolver}. */
class SlotsBalancedSlotMatchingResolverTest extends AbstractSlotMatchingResolverTest {

    @Override
    protected SlotMatchingResolver createSlotMatchingResolver() {
        return SlotsBalancedSlotMatchingResolver.INSTANCE;
    }

    @Override
    protected void assertAssignments(Collection<SlotAssignment> assignments) {
        Map<TaskManagerLocation, Set<SlotAssignment>> assignmentsPerTm =
                getAssignmentsPerTaskManager(assignments);
        IntSummaryStatistics stats =
                assignmentsPerTm.values().stream().collect(Collectors.summarizingInt(Set::size));
        assertThat(stats.getMax() - stats.getMin()).isBetween(0, 1);
    }
}
