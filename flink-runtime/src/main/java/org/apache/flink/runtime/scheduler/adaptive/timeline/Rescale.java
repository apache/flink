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

/**
 * The rescale to record the related vertices and slots change during the rescaling process.
 *
 * <p>This rescale begins when the scheduler initiates a rescaling operation and ends when the
 * rescaling succeeds.
 *
 * <pre>
 *
 * The structure of the rescale as follows:
 *
 * +--> rescale id information:
 * +    +-->rescale uuid
 * +    +-->resource requirements id
 * +    +-->rescale attempt id
 * +--> vertices:
 * +    +--> job vertex id-1 -> vertex-1 parallelism rescale:
 * +    +                       +--> vertex id
 * +    +                       +--> vertex name
 * +    +                       +--> slot sharing group id
 * +    +                       +--> slot sharing group name
 * +    +                       +--> desired parallelism
 * +    +                       +--> sufficient parallelism
 * +    +                       +--> pre-rescale parallelism
 * +    +                       +--> post-rescale parallelism
 * +    +--> job vertex id-2 -> vertex-2 parallelism rescale:
 * +    +                       +--> ...
 * +    +                       ...
 * +    ...
 * +--> slots:
 * +    +--> slot sharing group id-1 -> slot-1 sharing group rescale:
 * +    +                               +--> slot sharing group id
 * +    +                               +--> slot sharing group name
 * +    +                               +--> required resource profile
 * +    +                               +--> minimal required slots
 * +    +                               +--> pre-rescale slots
 * +    +                               +--> post-rescale slots
 * +    +                               +--> acquired resource profile
 * +    +--> slot sharing group id-2 -> slot-2 sharing group rescale:
 * +    +                               +--> ...
 * +    +                               ...
 * +    ...
 * +--> scheduler states:
 * +    +--> scheduler state span:
 * +    +    +--> state
 * +    +    +--> enter timestamp
 * +    +    +--> leave timestamp
 * +    +    +--> duration
 * +    +    +--> exception information
 * +    +--> ...
 * +    ...
 * +--> start timestamp
 * +--> end timestamp
 * +--> trigger cause
 * +--> terminal state
 * +--> terminated reason
 *
 * </pre>
 *
 * <p>The more design details about the rescale could be viewed in <a
 * href="https://cwiki.apache.org/confluence/x/TQr0Ew">FLIP-495</a>.
 */
public class Rescale implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final Logger LOG = LoggerFactory.getLogger(Rescale.class);

    @Nullable private transient String stringifiedException;

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
                Objects.isNull(schedulerStateSpan.getStringifiedException())
                        ? stringifiedException
                        : schedulerStateSpan.getStringifiedException();
        if (stringifiedException != null) {
            stringifiedException = null;
        }

        schedulerStateSpan.setStringifiedException(exceptionStr);
        this.schedulerStates.add(schedulerStateSpan);
        return this;
    }

    public Rescale addSchedulerState(State state) {
        return addSchedulerState(state, null);
    }

    public Rescale addSchedulerState(State schedulerState, @Nullable Throwable throwable) {
        Long enterTimestamp = schedulerState.getDurable().getEnterTimestamp();
        Long leaveTimestamp = schedulerState.getDurable().getLeaveTimestamp();

        long logicLeaveTimestamp =
                Objects.isNull(leaveTimestamp) ? Instant.now().toEpochMilli() : leaveTimestamp;
        return addSchedulerState(
                new SchedulerStateSpan(
                        schedulerState.getClass().getSimpleName(),
                        enterTimestamp,
                        logicLeaveTimestamp,
                        logicLeaveTimestamp - enterTimestamp,
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
    public String getStringifiedException() {
        return stringifiedException;
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

    public Long getStartTimestamp() {
        return startTimestamp;
    }

    public Rescale setEndTimestamp(Long endTimestamp) {
        if (this.endTimestamp != null) {
            LOG.warn("The old endTimestamp was already set to '{}'", this.endTimestamp);
        }
        this.endTimestamp = endTimestamp;
        return this;
    }

    public Long getEndTimestamp() {
        return endTimestamp;
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
            SlotSharingGroupRescale sharingGroupRescaleInfo =
                    slots.computeIfAbsent(
                            sharingGroup.getSlotSharingGroupId(),
                            ignored -> new SlotSharingGroupRescale(sharingGroup));
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
            SlotSharingGroup slotSharingGroup =
                    jobInformation.getVertexInformation(jvId).getSlotSharingGroup();
            String vertexName = jobInformation.getVertexInformation(jvId).getVertexName();
            VertexParallelismRescale vertexParallelismRescale =
                    this.vertices.computeIfAbsent(
                            jvId,
                            jobVertexID ->
                                    new VertexParallelismRescale(
                                            jvId, vertexName, slotSharingGroup));
            vertexParallelismRescale.setRequiredParallelisms(entry.getValue());
        }
        return this;
    }

    public Rescale setMinimalRequiredSlots(JobInformation jobInformation) {
        final Map<SlotSharingGroup, SlotSharingGroupMetaInfo> slotSharingGroupMetaInfo =
                SlotSharingGroupMetaInfo.from(jobInformation.getVertices());
        for (Map.Entry<SlotSharingGroup, SlotSharingGroupMetaInfo> entry :
                slotSharingGroupMetaInfo.entrySet()) {
            SlotSharingGroup sharingGroup = entry.getKey();
            SlotSharingGroupRescale slotSharingGroupRescale =
                    slots.computeIfAbsent(
                            sharingGroup.getSlotSharingGroupId(),
                            ignored -> new SlotSharingGroupRescale(sharingGroup));
            slotSharingGroupRescale.setMinimalRequiredSlots(entry.getValue().getMaxLowerBound());
        }
        return this;
    }

    public Rescale setPreRescaleSlotsAndParallelisms(
            JobInformation jobInformation, @Nullable Rescale lastCompletedRescale) {
        if (lastCompletedRescale == null) {
            LOG.info("No available previous parallelism to set.");
            return this;
        }
        for (JobVertexID jobVertexID : lastCompletedRescale.getVertices().keySet()) {
            Integer preRescaleParallelism =
                    lastCompletedRescale.vertices.get(jobVertexID).getPostRescaleParallelism();
            JobInformation.VertexInformation vertexInformation =
                    jobInformation.getVertexInformation(jobVertexID);
            VertexParallelismRescale vertexParallelismRescale =
                    vertices.computeIfAbsent(
                            jobVertexID,
                            jobVertexId ->
                                    new VertexParallelismRescale(
                                            jobVertexId,
                                            vertexInformation.getVertexName(),
                                            vertexInformation.getSlotSharingGroup()));
            vertexParallelismRescale.setPreRescaleParallelism(preRescaleParallelism);
        }

        Map<SlotSharingGroupId, SlotSharingGroupRescale> slotsRescales =
                lastCompletedRescale.getSlots();
        for (SlotSharingGroup sharingGroup : jobInformation.getSlotSharingGroups()) {
            SlotSharingGroupId slotSharingGroupId = sharingGroup.getSlotSharingGroupId();
            Integer preRescaleSlot = slotsRescales.get(slotSharingGroupId).getPostRescaleSlots();
            SlotSharingGroupRescale slotSharingGroupRescale =
                    slots.computeIfAbsent(
                            slotSharingGroupId,
                            ignored -> new SlotSharingGroupRescale(sharingGroup));
            slotSharingGroupRescale.setPreRescaleSlots(preRescaleSlot);
        }

        return this;
    }

    public Rescale setPostRescaleVertexParallelism(
            JobInformation jobInformation, VertexParallelism postRescaleVertexParallelism) {

        Set<JobVertexID> vertices = postRescaleVertexParallelism.getVertices();
        for (JobVertexID vertexID : vertices) {
            JobInformation.VertexInformation vertexInformation =
                    jobInformation.getVertexInformation(vertexID);
            VertexParallelismRescale vertexParallelismRescale =
                    this.vertices.computeIfAbsent(
                            vertexID,
                            jobVertexId ->
                                    new VertexParallelismRescale(
                                            jobVertexId,
                                            vertexInformation.getVertexName(),
                                            vertexInformation.getSlotSharingGroup()));

            vertexParallelismRescale.setPostRescaleParallelism(
                    postRescaleVertexParallelism.getParallelism(vertexID));
        }
        return this;
    }

    public Rescale setPostRescaleSlots(
            Collection<JobSchedulingPlan.SlotAssignment> postRescaleSlotAssignments) {
        Map<SlotSharingGroup, Set<JobSchedulingPlan.SlotAssignment>> assignmentsPerSharingGroup =
                postRescaleSlotAssignments.stream()
                        .collect(
                                Collectors.groupingBy(
                                        slotAssignment ->
                                                slotAssignment
                                                        .getTargetAs(
                                                                ExecutionSlotSharingGroup.class)
                                                        .getSlotSharingGroup(),
                                        Collectors.toSet()));
        for (Map.Entry<SlotSharingGroup, Set<JobSchedulingPlan.SlotAssignment>> entry :
                assignmentsPerSharingGroup.entrySet()) {
            SlotSharingGroup sharingGroup = entry.getKey();
            Set<JobSchedulingPlan.SlotAssignment> assignments =
                    assignmentsPerSharingGroup.get(sharingGroup);
            int postRescaleSlot = assignments.size();
            ResourceProfile acquiredResource =
                    assignments.iterator().next().getSlotInfo().getResourceProfile();
            SlotSharingGroupRescale slotSharingGroupRescale =
                    slots.computeIfAbsent(
                            sharingGroup.getSlotSharingGroupId(),
                            ignored -> new SlotSharingGroupRescale(sharingGroup));
            slotSharingGroupRescale.setPostRescaleSlots(postRescaleSlot);
            slotSharingGroupRescale.setAcquiredResourceProfile(acquiredResource);
        }
        return this;
    }

    public Rescale setTriggerCause(TriggerCause triggerCause) {
        this.triggerCause = triggerCause;
        return this;
    }

    public TriggerCause getTriggerCause() {
        return triggerCause;
    }

    public void log() {
        LOG.info("Updated rescale is: {}", this);
    }

    public Map<JobVertexID, VertexParallelismRescale> getVertices() {
        return Collections.unmodifiableMap(vertices);
    }

    public Map<SlotSharingGroupId, SlotSharingGroupRescale> getSlots() {
        return Collections.unmodifiableMap(slots);
    }

    public static boolean isTerminated(Rescale rescale) {
        return rescale != null && rescale.isTerminated();
    }

    public Rescale setStringifiedException(String stringifiedException) {
        this.stringifiedException = stringifiedException;
        return this;
    }

    @Override
    public String toString() {
        return "Rescale{"
                + "stringifiedException='"
                + stringifiedException
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

    @VisibleForTesting
    Map<JobVertexID, VertexParallelismRescale> getModifiableVertices() {
        return vertices;
    }

    @VisibleForTesting
    Map<SlotSharingGroupId, SlotSharingGroupRescale> getModifiableSlots() {
        return slots;
    }
}
