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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.util.EvictingBoundedList;

import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test suite for {@link StateLocalitySlotAssigner}. */
public class StateLocalitySlotAssignerTest {

    private static void addTasks(
            Map<JobVertexID, ArchivedExecutionJobVertex> tasks,
            SlotSharingGroup group,
            int numVertices,
            List<SlotInfo> slots) {
        final AllocationID[] allocationIds = new AllocationID[slots.size()];
        for (int idx = 0; idx < slots.size(); idx++) {
            allocationIds[idx] = slots.get(idx).getAllocationId();
        }
        for (int vertexIdx = 0; vertexIdx < numVertices; vertexIdx++) {
            final JobVertexID vertex = new JobVertexID();
            tasks.put(
                    vertex,
                    createArchivedExecutionJobVertex(vertex, allocationIds.length, allocationIds));
            group.addVertexToGroup(vertex);
        }
    }

    private static ArchivedExecutionJobVertex createArchivedExecutionJobVertex(
            JobVertexID jobVertexID, int parallelism, AllocationID[] allocationIds) {
        final StringifiedAccumulatorResult[] emptyAccumulators =
                new StringifiedAccumulatorResult[0];
        final long[] timestamps = new long[ExecutionState.values().length];

        final LocalTaskManagerLocation assignedResourceLocation = new LocalTaskManagerLocation();

        final ArchivedExecutionVertex[] subtasks = new ArchivedExecutionVertex[parallelism];
        for (int subtaskIdx = 0; subtaskIdx < parallelism; subtaskIdx++) {
            subtasks[subtaskIdx] =
                    new ArchivedExecutionVertex(
                            subtaskIdx,
                            "subtask #" + subtaskIdx,
                            new ArchivedExecution(
                                    emptyAccumulators,
                                    new IOMetrics(0L, 0L, 0L, 0L),
                                    new ExecutionAttemptID(),
                                    1,
                                    ExecutionState.CANCELED,
                                    null,
                                    assignedResourceLocation,
                                    allocationIds[subtaskIdx],
                                    subtaskIdx,
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

    @Test
    void testSplitSlotsBetweenSingleGroup() {
        final TestingPhysicalSlot[] slots = new TestingPhysicalSlot[32];
        for (int slotIdx = 0; slotIdx < slots.length; slotIdx++) {
            slots[slotIdx] =
                    TestingPhysicalSlot.builder()
                            .withAllocationID(new AllocationID(0L, slotIdx))
                            .build();
        }
        final StateLocalitySlotAssigner assigner = new StateLocalitySlotAssigner(null);
        final SlotSharingGroup group = new SlotSharingGroup();
        final Map<SlotSharingGroupId, Set<? extends SlotInfo>> split =
                assigner.splitSlotsBetweenSlotSharingGroups(
                        Arrays.asList(slots), Collections.singletonList(group));
        assertThat(split).hasSize(1);
        assertThat(split.get(group.getSlotSharingGroupId())).hasSize(32);
    }

    @Test
    void testSplitSlotsBetweenTwoGroupsBasedOnDataLocality() {
        final int numSlots = 32;
        final List<SlotInfo> slots = new ArrayList<>();
        for (int slotIdx = 0; slotIdx < numSlots; slotIdx++) {
            slots.add(
                    TestingPhysicalSlot.builder()
                            .withAllocationID(new AllocationID(0L, slotIdx))
                            .build());
        }

        final Map<JobVertexID, ArchivedExecutionJobVertex> tasks = new HashMap<>();
        // Two groups with three slots each.
        final SlotSharingGroup firstGroup = new SlotSharingGroup();
        addTasks(tasks, firstGroup, 3, slots.subList(0, 3));
        final SlotSharingGroup secondGroup = new SlotSharingGroup();
        addTasks(tasks, secondGroup, 2, slots.subList(3, 6));

        final ArchivedExecutionGraph archivedGraph =
                new ArchivedExecutionGraphBuilder()
                        .setState(JobStatus.SUSPENDED)
                        .setTasks(tasks)
                        .build();

        final StateLocalitySlotAssigner assigner = new StateLocalitySlotAssigner(archivedGraph);

        @SuppressWarnings({"unchecked", "rawtypes"})
        final Map<SlotSharingGroupId, Set<SlotInfo>> split =
                (Map)
                        assigner.splitSlotsBetweenSlotSharingGroups(
                                slots, Arrays.asList(firstGroup, secondGroup));

        assertThat(split).hasSize(2);

        assertThat(split.get(firstGroup.getSlotSharingGroupId()))
                .hasSize(numSlots / 2)
                .containsAll(slots.subList(0, 3));

        assertThat(split.get(secondGroup.getSlotSharingGroupId()))
                .hasSize(numSlots / 2)
                .containsAll(slots.subList(3, 6));
    }

    @Test
    void testAssignDouble() {
        final int numSlots = 32;
        final List<SlotInfo> slots = new ArrayList<>();
        for (int slotIdx = 0; slotIdx < numSlots; slotIdx++) {
            slots.add(
                    TestingPhysicalSlot.builder()
                            .withAllocationID(new AllocationID(0L, slotIdx))
                            .build());
        }

        final Map<JobVertexID, ArchivedExecutionJobVertex> tasks = new HashMap<>();
        final SlotSharingGroup firstGroup = new SlotSharingGroup();
        addTasks(tasks, firstGroup, 3, slots.subList(0, 16));

        final ArchivedExecutionGraph archivedGraph =
                new ArchivedExecutionGraphBuilder()
                        .setState(JobStatus.SUSPENDED)
                        .setTasks(tasks)
                        .build();

        final StateLocalitySlotAssigner assigner = new StateLocalitySlotAssigner(archivedGraph);

        final List<SlotSharingSlotAllocator.ExecutionSlotSharingGroup> groups = new ArrayList<>();

        final int numVertices = firstGroup.getJobVertexIds().size();
        final JobVertexID[] vertices = firstGroup.getJobVertexIds().toArray(new JobVertexID[0]);
        for (int subtaskIdx = 0; subtaskIdx < numSlots; subtaskIdx++) {
            final Set<ExecutionVertexID> containedVertices = new HashSet<>();
            for (int vertexIdx = 0; vertexIdx < numVertices; vertexIdx++) {
                containedVertices.add(new ExecutionVertexID(vertices[vertexIdx], subtaskIdx));
            }
            groups.add(
                    new SlotSharingSlotAllocator.ExecutionSlotSharingGroup(
                            containedVertices, Integer.toString(subtaskIdx)));
        }

        // Randomize ...
        Collections.shuffle(slots);

        final List<SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> result =
                assigner.assignSlots(slots, groups);

        final Map<String, SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> resultById =
                result.stream()
                        .collect(
                                Collectors.toMap(
                                        g -> g.getExecutionSlotSharingGroup().getId(),
                                        Function.identity()));

        for (SlotSharingSlotAllocator.ExecutionSlotSharingGroup group : groups) {
            final ExecutionVertexID executionVertexId =
                    Objects.requireNonNull(
                            Iterables.getFirst(group.getContainedExecutionVertices(), null));
            final ArchivedExecutionJobVertex archivedTask =
                    tasks.get(executionVertexId.getJobVertexId());
            final String oldAllocationId =
                    archivedTask.getParallelism() > executionVertexId.getSubtaskIndex()
                            ? archivedTask
                                    .getTaskVertices()[executionVertexId.getSubtaskIndex()]
                                    .getCurrentExecutionAttempt()
                                    .getAssignedAllocationID()
                                    .toString()
                            : "(none)";
            final String newAllocationId =
                    resultById.get(group.getId()).getSlotInfo().getAllocationId().toString();
            System.out.printf(
                    "Result: [%s] %s -> %s%n", group.getId(), oldAllocationId, newAllocationId);
        }
        System.out.println(resultById);
    }
}
