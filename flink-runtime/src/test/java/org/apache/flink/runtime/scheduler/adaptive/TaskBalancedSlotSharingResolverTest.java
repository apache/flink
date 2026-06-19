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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.allocator.FreeSlotFunction;
import org.apache.flink.runtime.scheduler.adaptive.allocator.IsSlotAvailableAndFreeFunction;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.ReserveSlotFunction;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingResolver;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TaskBalancedSlotSharingResolver;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TestJobInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TestVertexInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.adaptive.allocator.TestingSlot.getSlots;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TaskBalancedSlotSharingResolver}. */
class TaskBalancedSlotSharingResolverTest {

    private static final IsSlotAvailableAndFreeFunction is_slot_free_function = ignored -> true;
    private static final FreeSlotFunction free_slot_function = (a, c, t) -> {};
    private static final ReserveSlotFunction reserve_slot_function =
            (allocationId, resourceProfile) ->
                    TestingPhysicalSlot.builder()
                            .withAllocationID(allocationId)
                            .withResourceProfile(resourceProfile)
                            .build();
    private static final boolean disable_local_recovery = false;
    private static final String NULL_EXECUTION_TARGET = null;
    private static final SlotSharingResolver slotSharingResolver =
            TaskBalancedSlotSharingResolver.INSTANCE;
    private static final SlotSharingSlotAllocator slotAllocator =
            SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                    reserve_slot_function,
                    free_slot_function,
                    is_slot_free_function,
                    disable_local_recovery,
                    NULL_EXECUTION_TARGET,
                    false,
                    TaskManagerOptions.TaskManagerLoadBalanceMode.NONE);

    private SlotSharingGroup slotSharingGroup1;
    private SlotSharingGroup slotSharingGroup2;
    private TestVertexInformation.TestingCoLocationGroup coLocationGroup1;
    private TestVertexInformation.TestingCoLocationGroup coLocationGroup2;

    @BeforeEach
    void setup() {
        slotSharingGroup1 = new SlotSharingGroup();
        slotSharingGroup2 = new SlotSharingGroup();
        coLocationGroup1 = new TestVertexInformation.TestingCoLocationGroup();
        coLocationGroup2 = new TestVertexInformation.TestingCoLocationGroup();
    }

    @Test
    void testGetExecutionSlotSharingGroupsInOneSlotSharingGroup() {
        final JobInformation.VertexInformation vertex1 =
                new TestVertexInformation(1, slotSharingGroup1);
        final JobInformation.VertexInformation vertex2 =
                new TestVertexInformation(2, slotSharingGroup1);
        final JobInformation.VertexInformation vertex3 =
                new TestVertexInformation(3, slotSharingGroup1);
        final TestJobInformation testJobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2, vertex3));

        final VertexParallelism vertexParallelism =
                getVertexParallelism(testJobInformation, getSlotsFor(vertex1, vertex2, vertex3));
        final Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroup>
                executionSlotSharingGroups =
                        slotSharingResolver.getExecutionSlotSharingGroups(
                                testJobInformation, vertexParallelism);

        assertThat(executionSlotSharingGroups).hasSize(3);
        assertExecutionSlotSharingGroupsInSlotSharingGroupsExactlyAnyOrderOf(
                executionSlotSharingGroups, slotSharingGroup1);
        assertAscendingTasksPerExecutionSlotSharingGroupOfSlotSharingGroup(
                slotSharingGroup1, executionSlotSharingGroups, 2, 2, 2);
    }

    @Test
    void testGetExecutionSlotSharingGroupsInOneSlotSharingGroupWithCoLocationGroup() {
        final JobInformation.VertexInformation vertex1 =
                new TestVertexInformation(1, slotSharingGroup1, coLocationGroup1);
        final JobInformation.VertexInformation vertex2 =
                new TestVertexInformation(2, slotSharingGroup1, coLocationGroup1);
        final JobInformation.VertexInformation vertex3 =
                new TestVertexInformation(3, slotSharingGroup1);
        final TestJobInformation testJobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2, vertex3));

        final VertexParallelism vertexParallelism =
                getVertexParallelism(testJobInformation, getSlotsFor(vertex1, vertex2, vertex3));
        final Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroup>
                executionSlotSharingGroups =
                        slotSharingResolver.getExecutionSlotSharingGroups(
                                testJobInformation, vertexParallelism);

        assertThat(executionSlotSharingGroups).hasSize(3);
        assertExecutionSlotSharingGroupsInSlotSharingGroupsExactlyAnyOrderOf(
                executionSlotSharingGroups, slotSharingGroup1);
        assertAscendingTasksPerExecutionSlotSharingGroupOfSlotSharingGroup(
                slotSharingGroup1, executionSlotSharingGroups, 1, 2, 3);
    }

    @Test
    void testGetExecutionSlotSharingGroupsInMultiSlotSharingGroups() {
        final JobInformation.VertexInformation vertex1 =
                new TestVertexInformation(1, slotSharingGroup1);
        final JobInformation.VertexInformation vertex2 =
                new TestVertexInformation(2, slotSharingGroup1);
        final JobInformation.VertexInformation vertex3 =
                new TestVertexInformation(3, slotSharingGroup1);

        final JobInformation.VertexInformation vertex4 =
                new TestVertexInformation(1, slotSharingGroup2);
        final JobInformation.VertexInformation vertex5 =
                new TestVertexInformation(2, slotSharingGroup2);

        final TestJobInformation testJobInformation =
                new TestJobInformation(Arrays.asList(vertex1, vertex2, vertex3, vertex4, vertex5));

        final VertexParallelism vertexParallelism =
                getVertexParallelism(
                        testJobInformation,
                        getSlotsFor(vertex1, vertex2, vertex3, vertex4, vertex5));
        final Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroup>
                executionSlotSharingGroups =
                        slotSharingResolver.getExecutionSlotSharingGroups(
                                testJobInformation, vertexParallelism);

        assertThat(executionSlotSharingGroups).hasSize(5);
        assertExecutionSlotSharingGroupsInSlotSharingGroupsExactlyAnyOrderOf(
                executionSlotSharingGroups, slotSharingGroup1, slotSharingGroup2);
        assertAscendingTasksPerExecutionSlotSharingGroupOfSlotSharingGroup(
                slotSharingGroup1, executionSlotSharingGroups, 2, 2, 2);
        assertAscendingTasksPerExecutionSlotSharingGroupOfSlotSharingGroup(
                slotSharingGroup2, executionSlotSharingGroups, 1, 2);
    }

    @Test
    void testGetExecutionSlotSharingGroupsInMultiSlotSharingGroupsWithCoLocationGroups() {
        final JobInformation.VertexInformation vertex1 =
                new TestVertexInformation(1, slotSharingGroup1, coLocationGroup1);
        final JobInformation.VertexInformation vertex2 =
                new TestVertexInformation(2, slotSharingGroup1, coLocationGroup1);
        final JobInformation.VertexInformation vertex3 =
                new TestVertexInformation(3, slotSharingGroup1);

        final JobInformation.VertexInformation vertex4 =
                new TestVertexInformation(1, slotSharingGroup2, coLocationGroup2);
        final JobInformation.VertexInformation vertex5 =
                new TestVertexInformation(3, slotSharingGroup2);
        final JobInformation.VertexInformation vertex6 =
                new TestVertexInformation(2, slotSharingGroup2, coLocationGroup2);
        final JobInformation.VertexInformation vertex7 =
                new TestVertexInformation(1, slotSharingGroup2);

        final TestJobInformation testJobInformation =
                new TestJobInformation(
                        Arrays.asList(
                                vertex1, vertex2, vertex3, vertex4, vertex5, vertex6, vertex7));

        final VertexParallelism vertexParallelism =
                getVertexParallelism(
                        testJobInformation,
                        getSlotsFor(vertex1, vertex2, vertex3, vertex4, vertex5, vertex6, vertex7));
        final Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroup>
                executionSlotSharingGroups =
                        slotSharingResolver.getExecutionSlotSharingGroups(
                                testJobInformation, vertexParallelism);

        assertThat(executionSlotSharingGroups).hasSize(6);
        assertExecutionSlotSharingGroupsInSlotSharingGroupsExactlyAnyOrderOf(
                executionSlotSharingGroups, slotSharingGroup1, slotSharingGroup2);
        assertAscendingTasksPerExecutionSlotSharingGroupOfSlotSharingGroup(
                slotSharingGroup1, executionSlotSharingGroups, 1, 2, 3);
        assertAscendingTasksPerExecutionSlotSharingGroupOfSlotSharingGroup(
                slotSharingGroup2, executionSlotSharingGroups, 2, 2, 3);
    }

    private Collection<PhysicalSlot> getSlotsFor(
            JobInformation.VertexInformation... verticesInformation) {
        if (verticesInformation == null || verticesInformation.length < 1) {
            return Collections.emptyList();
        }
        int slots =
                Arrays.stream(verticesInformation)
                        .map(JobInformation.VertexInformation::getParallelism)
                        .reduce(0, Integer::sum);
        return getSlots(slots);
    }

    private VertexParallelism getVertexParallelism(
            JobInformation jobInformation, Collection<PhysicalSlot> slots) {
        return slotAllocator
                .determineParallelism(jobInformation, slots)
                .orElse(VertexParallelism.empty());
    }

    private static void assertExecutionSlotSharingGroupsInSlotSharingGroupsExactlyAnyOrderOf(
            Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroup>
                    executionSlotSharingGroups,
            SlotSharingGroup... slotSharingGroups) {
        assertThat(
                        executionSlotSharingGroups.stream()
                                .map(
                                        SlotSharingSlotAllocator.ExecutionSlotSharingGroup
                                                ::getSlotSharingGroup)
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(slotSharingGroups);
    }

    private static void assertAscendingTasksPerExecutionSlotSharingGroupOfSlotSharingGroup(
            SlotSharingGroup slotSharingGroup,
            Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroup>
                    executionSlotSharingGroups,
            Integer... numbers) {
        assertThat(
                        executionSlotSharingGroups.stream()
                                .filter(essg -> essg.getSlotSharingGroup().equals(slotSharingGroup))
                                .map(essg -> essg.getContainedExecutionVertices().size())
                                .sorted()
                                .collect(Collectors.toList()))
                .containsExactly(numbers);
    }
}
