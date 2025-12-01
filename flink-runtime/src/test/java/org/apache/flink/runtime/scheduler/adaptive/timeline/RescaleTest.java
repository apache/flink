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

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismStore;
import org.apache.flink.runtime.scheduler.adaptive.JobGraphJobInformation;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.adaptive.State;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator;
import org.apache.flink.runtime.scheduler.adaptive.allocator.TestingSlot;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link Rescale}. */
class RescaleTest {

    private final Function<Integer, Optional<String>> ingoredFunction = integer -> Optional.empty();

    private final SlotSharingGroup slotSharingGroupA = new SlotSharingGroup();
    private final SlotSharingGroup slotSharingGroupB = new SlotSharingGroup();

    private List<SlotSharingGroup> slotSharingGroups;

    private JobVertex jobVertex1OfSlotSharingGroupA;
    private JobVertex jobVertex2OfSlotSharingGroupA;
    private JobVertex jobVertex1OfSlotSharingGroupB;
    private JobVertex jobVertex2OfSlotSharingGroupB;

    private List<JobVertex> jobVertices;

    private JobGraphJobInformation.JobVertexInformation jobVertexInformation1OfSlotSharingGroupA;
    private JobGraphJobInformation.JobVertexInformation jobVertexInformation2OfSlotSharingGroupA;
    private JobGraphJobInformation.JobVertexInformation jobVertexInformation1OfSlotSharingGroupB;
    private JobGraphJobInformation.JobVertexInformation jobVertexInformation2OfSlotSharingGroupB;

    private JobInformation jobInformation;

    @BeforeEach
    void setup() {
        this.slotSharingGroupA.setSlotSharingGroupName("ssgA");
        this.slotSharingGroupB.setSlotSharingGroupName("ssgB");

        this.slotSharingGroups =
                new ArrayList<>() {
                    {
                        add(slotSharingGroupA);
                        add(slotSharingGroupB);
                    }
                };

        this.jobVertex1OfSlotSharingGroupA = new JobVertex("JobVertex1OfSlotSharingGroupA");
        jobVertex1OfSlotSharingGroupA.setSlotSharingGroup(slotSharingGroupA);
        this.jobVertex2OfSlotSharingGroupA = new JobVertex("JobVertex2OfSlotSharingGroupA");
        jobVertex2OfSlotSharingGroupA.setSlotSharingGroup(slotSharingGroupA);

        this.jobVertex1OfSlotSharingGroupB = new JobVertex("JobVertex1OfSlotSharingGroupB");
        jobVertex1OfSlotSharingGroupB.setSlotSharingGroup(slotSharingGroupB);
        this.jobVertex2OfSlotSharingGroupB = new JobVertex("JobVertex2OfSlotSharingGroupB");
        jobVertex2OfSlotSharingGroupB.setSlotSharingGroup(slotSharingGroupB);

        this.jobVertices =
                new ArrayList<>() {
                    {
                        add(jobVertex1OfSlotSharingGroupA);
                        add(jobVertex2OfSlotSharingGroupA);
                        add(jobVertex1OfSlotSharingGroupB);
                        add(jobVertex2OfSlotSharingGroupB);
                    }
                };

        this.jobVertexInformation1OfSlotSharingGroupA =
                new JobGraphJobInformation.JobVertexInformation(
                        jobVertex1OfSlotSharingGroupA,
                        new DefaultVertexParallelismInfo(1, 2, 4, ingoredFunction));
        this.jobVertexInformation2OfSlotSharingGroupA =
                new JobGraphJobInformation.JobVertexInformation(
                        jobVertex2OfSlotSharingGroupA,
                        new DefaultVertexParallelismInfo(2, 3, 8, ingoredFunction));
        this.jobVertexInformation1OfSlotSharingGroupB =
                new JobGraphJobInformation.JobVertexInformation(
                        jobVertex1OfSlotSharingGroupB,
                        new DefaultVertexParallelismInfo(3, 4, 16, ingoredFunction));
        this.jobVertexInformation2OfSlotSharingGroupB =
                new JobGraphJobInformation.JobVertexInformation(
                        jobVertex2OfSlotSharingGroupB,
                        new DefaultVertexParallelismInfo(4, 5, 32, ingoredFunction));

        DefaultVertexParallelismStore defaultVertexParallelismStore =
                new DefaultVertexParallelismStore();
        defaultVertexParallelismStore.setParallelismInfo(
                jobVertex1OfSlotSharingGroupA.getID(),
                jobVertexInformation1OfSlotSharingGroupA.getVertexParallelismInfo());
        defaultVertexParallelismStore.setParallelismInfo(
                jobVertex2OfSlotSharingGroupA.getID(),
                jobVertexInformation2OfSlotSharingGroupA.getVertexParallelismInfo());
        defaultVertexParallelismStore.setParallelismInfo(
                jobVertex1OfSlotSharingGroupB.getID(),
                jobVertexInformation1OfSlotSharingGroupB.getVertexParallelismInfo());
        defaultVertexParallelismStore.setParallelismInfo(
                jobVertex2OfSlotSharingGroupB.getID(),
                jobVertexInformation2OfSlotSharingGroupB.getVertexParallelismInfo());

        this.jobInformation =
                new TestingJobInformation(
                        new HashSet<>() {
                            {
                                add(slotSharingGroupA);
                                add(slotSharingGroupB);
                            }
                        },
                        new ArrayList<>() {
                            {
                                add(jobVertex1OfSlotSharingGroupA);
                                add(jobVertex2OfSlotSharingGroupA);
                                add(jobVertex1OfSlotSharingGroupB);
                                add(jobVertex2OfSlotSharingGroupB);
                            }
                        },
                        defaultVertexParallelismStore);
    }

    @Test
    void testAddSchedulerState() {
        Rescale rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));

        // Test for add a state span into a terminated rescale.
        rescale.setTerminatedReason(TerminatedReason.SUCCEEDED);
        rescale.addSchedulerState(new SchedulerStateSpan("", null, null, null, null));
        assertThat(rescale.getSchedulerStates()).isEmpty();

        // Test for add a state span into a non-terminated rescale.
        rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));
        rescale.addSchedulerState(new SchedulerStateSpan("", null, null, null, null));
        assertThat(rescale.getSchedulerStates()).hasSize(1);

        // Test the correctness of throwable processing.
        String stringifiedException1 =
                ExceptionUtils.stringifyException(new RuntimeException("exception1"));
        rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));
        rescale.setStringifiedException(stringifiedException1);
        rescale.addSchedulerState(new SchedulerStateSpan("", null, null, null, null));
        SchedulerStateSpan schedulerStateSpan = rescale.getSchedulerStates().get(0);
        assertThat(schedulerStateSpan.getStringifiedException()).isEqualTo(stringifiedException1);
        assertThat(rescale.getStringifiedException()).isNull();

        rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));
        rescale.addSchedulerState(
                new SchedulerStateSpan("", null, null, null, stringifiedException1));
        schedulerStateSpan = rescale.getSchedulerStates().get(0);
        assertThat(schedulerStateSpan.getStringifiedException()).isEqualTo(stringifiedException1);

        // Test the correctness of the end time of span auto-fulfill.
        State stateWithoutEndTimestamp = new TestingAdaptiveSchedulerState(2L, null);
        rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));
        rescale.setStringifiedException(stringifiedException1);
        rescale.addSchedulerState(stateWithoutEndTimestamp, null);
        schedulerStateSpan = rescale.getSchedulerStates().get(0);
        assertThat(schedulerStateSpan.getLeaveTimestamp())
                .isLessThanOrEqualTo(Instant.now().toEpochMilli());
    }

    @Test
    void testSetDesiredSlots() {
        Rescale rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));
        rescale.setDesiredSlots(jobInformation);
        Map<SlotSharingGroupId, SlotSharingGroupRescale> slots = rescale.getSlots();
        assertThat(slots).hasSize(2);
        assertThat(slots.keySet())
                .hasSameElementsAs(
                        slotSharingGroups.stream()
                                .map(SlotSharingGroup::getSlotSharingGroupId)
                                .collect(Collectors.toSet()));
        assertThat(
                        slots.values().stream()
                                .map(SlotSharingGroupRescale::getDesiredSlots)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(3, 5);
        assertThat(
                        slots.values().stream()
                                .map(SlotSharingGroupRescale::getRequiredResourceProfile)
                                .collect(Collectors.toSet()))
                .containsExactly(ResourceProfile.UNKNOWN);
    }

    @Test
    void testSetDesiredVertexParallelism() {
        Rescale rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));
        rescale.setDesiredVertexParallelism(jobInformation);
        Map<JobVertexID, VertexParallelismRescale> vertices = rescale.getVertices();
        assertThat(vertices).hasSize(4);
        assertThat(vertices.keySet())
                .isEqualTo(
                        vertices.values().stream()
                                .map(VertexParallelismRescale::getJobVertexId)
                                .collect(Collectors.toSet()))
                .hasSameElementsAs(
                        jobVertices.stream().map(JobVertex::getID).collect(Collectors.toSet()));

        assertThat(
                        vertices.values().stream()
                                .map(VertexParallelismRescale::getJobVertexName)
                                .collect(Collectors.toSet()))
                .hasSameElementsAs(
                        jobVertices.stream().map(JobVertex::getName).collect(Collectors.toSet()));

        assertThat(
                        vertices.values().stream()
                                .map(VertexParallelismRescale::getSlotSharingGroupId)
                                .collect(Collectors.toSet()))
                .hasSameElementsAs(
                        jobVertices.stream()
                                .map(jv -> jv.getSlotSharingGroup().getSlotSharingGroupId())
                                .collect(Collectors.toSet()));

        assertThat(
                        vertices.values().stream()
                                .map(VertexParallelismRescale::getSlotSharingGroupName)
                                .collect(Collectors.toSet()))
                .hasSameElementsAs(
                        jobVertices.stream()
                                .map(jv -> jv.getSlotSharingGroup().getSlotSharingGroupName())
                                .collect(Collectors.toSet()));

        assertThat(
                        vertices.values().stream()
                                .map(VertexParallelismRescale::getDesiredParallelism)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(2, 3, 4, 5);
        assertThat(
                        vertices.values().stream()
                                .map(VertexParallelismRescale::getSufficientParallelism)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(1, 2, 3, 4);
    }

    @Test
    void testSetMinimalRequiredSlots() {
        Rescale rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));
        rescale.setMinimalRequiredSlots(jobInformation);
        Map<SlotSharingGroupId, SlotSharingGroupRescale> slots = rescale.getSlots();
        assertThat(slots).hasSize(2);
        assertThat(slots.keySet())
                .containsExactlyInAnyOrder(
                        slotSharingGroupA.getSlotSharingGroupId(),
                        slotSharingGroupB.getSlotSharingGroupId());
        assertThat(
                        slots.values().stream()
                                .map(SlotSharingGroupRescale::getMinimalRequiredSlots)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(2, 4);
    }

    @Test
    void testSetPreRescaleSlotsAndParallelisms() {
        // Test for null last rescale.
        Rescale rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 2L));
        rescale.setPreRescaleSlotsAndParallelisms(jobInformation, null);
        assertThat(rescale.getSlots()).isEmpty();
        assertThat(rescale.getVertices()).isEmpty();

        // Test for non-null last rescale.
        // Prepare the last completed rescale.
        Rescale lastCompletedRescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));
        Map<JobVertexID, VertexParallelismRescale> lastRescaleVertices =
                lastCompletedRescale.getModifiableVertices();
        jobVertices.forEach(
                jobVertex -> {
                    VertexParallelismRescale vertexParallelismRescale =
                            lastRescaleVertices.computeIfAbsent(
                                    jobVertex.getID(),
                                    ignored ->
                                            new VertexParallelismRescale(
                                                    jobVertex.getID(),
                                                    jobInformation
                                                            .getVertexInformation(jobVertex.getID())
                                                            .getVertexName(),
                                                    jobInformation
                                                            .getVertexInformation(jobVertex.getID())
                                                            .getSlotSharingGroup()));
                    vertexParallelismRescale.setPostRescaleParallelism(4);
                });
        Map<SlotSharingGroupId, SlotSharingGroupRescale> lastRescaleSlotSharingGroups =
                lastCompletedRescale.getModifiableSlots();
        slotSharingGroups.forEach(
                group -> {
                    SlotSharingGroupRescale slotSharingGroupRescale =
                            lastRescaleSlotSharingGroups.computeIfAbsent(
                                    group.getSlotSharingGroupId(),
                                    ignored -> new SlotSharingGroupRescale(group));
                    slotSharingGroupRescale.setPostRescaleSlots(4);
                });

        rescale.setPreRescaleSlotsAndParallelisms(jobInformation, lastCompletedRescale);

        assertThat(rescale.getSlots()).hasSize(2);
        assertThat(
                        rescale.getSlots().values().stream()
                                .map(SlotSharingGroupRescale::getPreRescaleSlots)
                                .collect(Collectors.toSet()))
                .containsExactly(4);

        assertThat(
                        rescale.getVertices().values().stream()
                                .map(VertexParallelismRescale::getPreRescaleParallelism)
                                .collect(Collectors.toSet()))
                .containsExactly(4);
    }

    @Test
    void testSetPostRescaleVertexParallelism() {
        Map<JobVertexID, Integer> parallelismForVertices = new HashMap<>();
        VertexParallelism postVertexParallelism = new VertexParallelism(parallelismForVertices);
        jobVertices.forEach(
                vertex -> {
                    parallelismForVertices.put(vertex.getID(), 2);
                });
        Rescale rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));
        rescale.setPostRescaleVertexParallelism(jobInformation, postVertexParallelism);
        assertThat(
                        rescale.getVertices().values().stream()
                                .map(VertexParallelismRescale::getPostRescaleParallelism)
                                .collect(Collectors.toSet()))
                .containsExactly(2);
    }

    @Test
    void testSetPostRescaleSlots() {
        List<JobSchedulingPlan.SlotAssignment> slotAssignments = new ArrayList<>();

        slotAssignments.add(
                new JobSchedulingPlan.SlotAssignment(
                        new TestingSlot(ResourceProfile.UNKNOWN),
                        new SlotSharingSlotAllocator.ExecutionSlotSharingGroup(
                                slotSharingGroupA, null)));
        slotAssignments.add(
                new JobSchedulingPlan.SlotAssignment(
                        new TestingSlot(ResourceProfile.UNKNOWN),
                        new SlotSharingSlotAllocator.ExecutionSlotSharingGroup(
                                slotSharingGroupA, null)));

        slotAssignments.add(
                new JobSchedulingPlan.SlotAssignment(
                        new TestingSlot(ResourceProfile.UNKNOWN),
                        new SlotSharingSlotAllocator.ExecutionSlotSharingGroup(
                                slotSharingGroupB, null)));
        slotAssignments.add(
                new JobSchedulingPlan.SlotAssignment(
                        new TestingSlot(ResourceProfile.UNKNOWN),
                        new SlotSharingSlotAllocator.ExecutionSlotSharingGroup(
                                slotSharingGroupB, null)));
        slotAssignments.add(
                new JobSchedulingPlan.SlotAssignment(
                        new TestingSlot(ResourceProfile.UNKNOWN),
                        new SlotSharingSlotAllocator.ExecutionSlotSharingGroup(
                                slotSharingGroupB, null)));
        slotAssignments.add(
                new JobSchedulingPlan.SlotAssignment(
                        new TestingSlot(ResourceProfile.UNKNOWN),
                        new SlotSharingSlotAllocator.ExecutionSlotSharingGroup(
                                slotSharingGroupB, null)));

        Rescale rescale = new Rescale(new RescaleIdInfo(new AbstractID(), 1L));
        rescale.setPostRescaleSlots(slotAssignments);
        assertThat(
                        rescale.getSlots().values().stream()
                                .map(SlotSharingGroupRescale::getPostRescaleSlots)
                                .collect(Collectors.toSet()))
                .containsExactly(2, 4);
        assertThat(
                        rescale.getSlots().values().stream()
                                .map(SlotSharingGroupRescale::getAcquiredResourceProfile)
                                .collect(Collectors.toSet()))
                .containsExactly(ResourceProfile.UNKNOWN);
    }
}
