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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.adaptive.State;
import org.apache.flink.runtime.scheduler.adaptive.allocator.JobInformation;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.SlotSharingGroupMetaInfo;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Rescale event. */
public class Rescale implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final Logger LOG = LoggerFactory.getLogger(Rescale.class);

    @Nullable private transient String stringedException;

    private final RescaleIdInfo rescaleIdInfo;

    private final Map<JobVertexID, VertexParallelismRescale> vertices;
    private final Map<SlotSharingGroupId, SlotSharingGroupRescale> slots;

    private final List<SchedulerStateSpan> schedulerStates;

    private Long startTimestamp;
    private Long endTimestamp;

    private TriggerCause triggerCause;
    @Nullable private TerminalState terminalState;
    @Nullable private TerminatedReason terminatedReason;

    Rescale(RescaleIdInfo rescaleIdInfo) {
        this.rescaleIdInfo = rescaleIdInfo;
        this.vertices = new HashMap<>();
        this.slots = new HashMap<>();
        this.schedulerStates = new ArrayList<>();
    }

    @VisibleForTesting
    Rescale addSchedulerState(SchedulerStateSpan schedulerStateSpan) {
        if (this.isTerminated()) {
            LOG.warn(
                    "Rescale is already terminated. The scheduler state {} will be ignored.",
                    schedulerStateSpan);
            return this;
        }
        String exceptionStr =
                Objects.isNull(schedulerStateSpan.getStringedException())
                        ? stringedException
                        : schedulerStateSpan.getStringedException();
        if (stringedException != null) {
            stringedException = null;
        }

        schedulerStateSpan.setStringedException(exceptionStr);
        this.schedulerStates.add(schedulerStateSpan);
        return this;
    }

    public Rescale addSchedulerState(State state) {
        return addSchedulerState(state, null);
    }

    public Rescale addSchedulerState(State schedulerState, @Nullable Throwable throwable) {
        Long outTimestamp = schedulerState.getDurable().getOutTimestamp();
        long logicEndMillis =
                Objects.isNull(outTimestamp) ? Instant.now().toEpochMilli() : outTimestamp;
        return addSchedulerState(
                new SchedulerStateSpan(
                        schedulerState.getClass().getSimpleName(),
                        schedulerState.getDurable().getInTimestamp(),
                        logicEndMillis,
                        logicEndMillis - schedulerState.getDurable().getInTimestamp(),
                        ExceptionUtils.stringifyException(throwable)));
    }

    public List<SchedulerStateSpan> getSchedulerStates() {
        return Collections.unmodifiableList(schedulerStates);
    }

    public Rescale clearSchedulerStates() {
        this.schedulerStates.clear();
        return this;
    }

    @Nullable
    public TerminalState getTerminalState() {
        return terminalState;
    }

    @Nullable
    public String getStringedException() {
        return stringedException;
    }

    private boolean isTerminated() {
        return terminalState != null;
    }

    public Duration getDuration() {
        if (this.isTerminated() && startTimestamp != null && endTimestamp != null) {
            return Duration.ofMillis(endTimestamp - startTimestamp);
        }
        return Duration.ZERO;
    }

    public Rescale setTerminatedReason(TerminatedReason terminatedReason) {
        Preconditions.checkNotNull(terminatedReason);
        if (this.terminatedReason != null) {
            LOG.warn("The old sealed reason was already set to '{}'", this.terminatedReason);
        }
        this.terminatedReason = terminatedReason;
        this.terminalState = terminatedReason.getTerminalState();
        return this;
    }

    public Rescale setStartTimestamp(long timestamp) {
        if (this.startTimestamp != null) {
            LOG.warn("The old startTimestamp was already set to '{}'", this.startTimestamp);
        }
        this.startTimestamp = timestamp;
        return this;
    }

    public Rescale setEndTimestamp(Long endTimestamp) {
        if (this.endTimestamp != null) {
            LOG.warn("The old endTimestamp was already set to '{}'", this.endTimestamp);
        }
        this.endTimestamp = endTimestamp;
        return this;
    }

    public Rescale setDesiredSlots(JobInformation jobInformation) {
        for (SlotSharingGroup sharingGroup : jobInformation.getSlotSharingGroups()) {
            int desiredSlot =
                    sharingGroup.getJobVertexIds().stream()
                            .map(
                                    jobVertexID ->
                                            jobInformation
                                                    .getVertexInformation(jobVertexID)
                                                    .getParallelism())
                            .max(Integer::compare)
                            .orElse(0);
            SlotSharingGroupId sharingGroupId = sharingGroup.getSlotSharingGroupId();
            SlotSharingGroupRescale sharingGroupRescaleInfo =
                    slots.computeIfAbsent(sharingGroupId, SlotSharingGroupRescale::new);
            sharingGroupRescaleInfo.setSlotSharingGroupMetaInfo(sharingGroup);
            sharingGroupRescaleInfo.setDesiredSlots(desiredSlot);
        }
        return this;
    }

    public Rescale setDesiredVertexParallelism(JobInformation jobInformation) {
        Map<JobVertexID, VertexParallelismInformation> allParallelismInfo =
                jobInformation.getVertexParallelismStore().getAllParallelismInfo();
        for (Map.Entry<JobVertexID, VertexParallelismInformation> entry :
                allParallelismInfo.entrySet()) {
            JobVertexID jvId = entry.getKey();
            VertexParallelismInformation vertexParallelInfo = entry.getValue();
            VertexParallelismRescale vertexParallelismRescale =
                    this.vertices.computeIfAbsent(
                            jvId, jobVertexID -> new VertexParallelismRescale(jvId));
            SlotSharingGroup slotSharingGroup =
                    jobInformation.getVertexInformation(jvId).getSlotSharingGroup();
            vertexParallelismRescale.setSlotSharingGroupMetaInfo(slotSharingGroup);
            vertexParallelismRescale.setJobVertexName(jobInformation.getVertexName(jvId));
            vertexParallelismRescale.setRequiredParallelisms(vertexParallelInfo);
        }
        return this;
    }

    public Rescale setMinimalRequiredSlots(JobInformation jobInformation) {
        final Map<SlotSharingGroup, SlotSharingGroupMetaInfo> slotSharingGroupMetaInfo =
                SlotSharingGroupMetaInfo.from(jobInformation.getVertices());
        for (Map.Entry<SlotSharingGroup, SlotSharingGroupMetaInfo> entry :
                slotSharingGroupMetaInfo.entrySet()) {
            SlotSharingGroupId groupId = entry.getKey().getSlotSharingGroupId();
            SlotSharingGroupRescale slotSharingGroupRescale =
                    slots.computeIfAbsent(groupId, SlotSharingGroupRescale::new);
            slotSharingGroupRescale.setMinimalRequiredSlots(entry.getValue().getMaxLowerBound());
        }
        return this;
    }

    public Rescale setPreRescaleSlotsAndParallelisms(@Nullable Rescale lastCompletedRescale) {
        if (lastCompletedRescale == null) {
            LOG.info("No available previous parallelism to set.");
            return this;
        }
        for (JobVertexID jobVertexID : lastCompletedRescale.getVertices().keySet()) {
            Integer preRescaleParallelism =
                    lastCompletedRescale.vertices.get(jobVertexID).getPostRescaleParallelism();
            VertexParallelismRescale vertexParallelismRescale =
                    vertices.computeIfAbsent(jobVertexID, VertexParallelismRescale::new);
            vertexParallelismRescale.setPreRescaleParallelism(preRescaleParallelism);
        }

        for (SlotSharingGroupId sharingGroupId : lastCompletedRescale.getSlots().keySet()) {
            Integer preRescaleSlot =
                    lastCompletedRescale.slots.get(sharingGroupId).getPostRescaleSlots();
            SlotSharingGroupRescale slotSharingGroupRescale =
                    slots.computeIfAbsent(sharingGroupId, SlotSharingGroupRescale::new);
            slotSharingGroupRescale.setPreRescaleSlots(preRescaleSlot);
        }

        return this;
    }

    public Rescale setPostRescaleVertexParallelism(VertexParallelism postRescaleVertexParallelism) {
        Set<JobVertexID> vertices = postRescaleVertexParallelism.getVertices();
        for (JobVertexID vertexID : vertices) {
            VertexParallelismRescale vertexParallelismRescale =
                    this.vertices.computeIfAbsent(vertexID, VertexParallelismRescale::new);
            vertexParallelismRescale.setPostRescaleParallelism(
                    postRescaleVertexParallelism.getParallelism(vertexID));
        }
        return this;
    }

    public Rescale setPostRescaleSlots(
            Collection<JobSchedulingPlan.SlotAssignment> postRescaleSlotAssignments) {
        Map<SlotSharingGroupId, Set<JobSchedulingPlan.SlotAssignment>> assignmentsPerSharingGroup =
                postRescaleSlotAssignments.stream()
                        .collect(
                                Collectors.groupingBy(
                                        slotAssignment ->
                                                slotAssignment
                                                        .getTargetAs(
                                                                ExecutionSlotSharingGroup.class)
                                                        .getSlotSharingGroup()
                                                        .getSlotSharingGroupId(),
                                        Collectors.toSet()));
        for (Map.Entry<SlotSharingGroupId, Set<JobSchedulingPlan.SlotAssignment>> entry :
                assignmentsPerSharingGroup.entrySet()) {
            SlotSharingGroupId sharingGroupId = entry.getKey();
            Set<JobSchedulingPlan.SlotAssignment> assignments =
                    assignmentsPerSharingGroup.get(sharingGroupId);
            int postRescaleSlot = assignments.size();
            ResourceProfile acquiredResource =
                    assignments.iterator().next().getSlotInfo().getResourceProfile();
            SlotSharingGroupRescale slotSharingGroupRescale =
                    slots.computeIfAbsent(sharingGroupId, SlotSharingGroupRescale::new);
            slotSharingGroupRescale.setPostRescaleSlots(postRescaleSlot);
            slotSharingGroupRescale.setAcquiredResourceProfile(acquiredResource);
        }
        return this;
    }

    public Rescale setTriggerCause(TriggerCause triggerCause) {
        this.triggerCause = triggerCause;
        return this;
    }

    public void log() {
        LOG.info("Updated rescale is: {}", this);
    }

    public Map<JobVertexID, VertexParallelismRescale> getVertices() {
        return vertices;
    }

    public Map<SlotSharingGroupId, SlotSharingGroupRescale> getSlots() {
        return slots;
    }

    public static boolean isTerminated(Rescale rescale) {
        return rescale != null && rescale.isTerminated();
    }

    public Rescale setStringedException(String stringedException) {
        this.stringedException = stringedException;
        return this;
    }

    @Override
    public String toString() {
        return "Rescale{"
                + "stringedException='"
                + stringedException
                + '\''
                + ", rescaleIdInfo="
                + rescaleIdInfo
                + ", vertices="
                + vertices
                + ", slots="
                + slots
                + ", schedulerStates="
                + schedulerStates
                + ", startTimestamp="
                + startTimestamp
                + ", endTimestamp="
                + endTimestamp
                + ", triggerCause="
                + triggerCause
                + ", terminalState="
                + terminalState
                + ", terminatedReason="
                + terminatedReason
                + '}';
    }
}
