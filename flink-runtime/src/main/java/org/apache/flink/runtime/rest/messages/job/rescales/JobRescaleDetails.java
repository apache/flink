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

package org.apache.flink.runtime.rest.messages.job.rescales;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.rest.messages.ResourceProfileInfo;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDKeyDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDKeySerializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIDKeyDeserializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIDKeySerializer;
import org.apache.flink.runtime.rest.messages.json.SlotSharingGroupIDSerializer;
import org.apache.flink.runtime.scheduler.VertexParallelismInformation;
import org.apache.flink.runtime.scheduler.adaptive.timeline.Rescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.SlotSharingGroupRescale;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TerminalState;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TerminatedReason;
import org.apache.flink.runtime.scheduler.adaptive.timeline.TriggerCause;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import io.swagger.v3.oas.annotations.media.Schema;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Job rescales details class. */
@Schema(name = "JobRescaleDetails")
public class JobRescaleDetails implements ResponseBody, Serializable {
    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_RESCALE_UUID = "rescaleUuid";
    public static final String FIELD_NAME_RESOURCE_REQUIREMENTS_UUID = "resourceRequirementsUuid";
    public static final String FIELD_NAME_RESCALE_ATTEMPT_ID = "rescaleAttemptId";
    public static final String FIELD_NAME_VERTICES = "vertices";
    public static final String FIELD_NAME_SLOTS = "slots";
    public static final String FIELD_NAME_SCHEDULER_STATES = "schedulerStates";
    public static final String FIELD_NAME_START_TIMESTAMP = "startTimestampInMillis";
    public static final String FIELD_NAME_END_TIMESTAMP = "endTimestampInMillis";
    public static final String FIELD_NAME_TERMINAL_STATE = "terminalState";
    public static final String FIELD_NAME_TRIGGER_CAUSE = "triggerCause";
    public static final String FIELD_NAME_TERMINATED_REASON = "terminatedReason";

    @JsonProperty(FIELD_NAME_RESCALE_UUID)
    private final String rescaleUuid;

    @JsonProperty(FIELD_NAME_RESOURCE_REQUIREMENTS_UUID)
    private final String resourceRequirementsUuid;

    @JsonProperty(FIELD_NAME_RESCALE_ATTEMPT_ID)
    private final long rescaleAttemptId;

    @JsonProperty(FIELD_NAME_VERTICES)
    @JsonSerialize(keyUsing = JobVertexIDKeySerializer.class)
    @JsonDeserialize(keyUsing = JobVertexIDKeyDeserializer.class)
    private final Map<JobVertexID, VertexParallelismRescaleInfo> vertices;

    @JsonProperty(FIELD_NAME_SLOTS)
    @JsonSerialize(keyUsing = SlotSharingGroupIDKeySerializer.class)
    @JsonDeserialize(keyUsing = SlotSharingGroupIDKeyDeserializer.class)
    private final Map<SlotSharingGroupId, SlotSharingGroupRescaleInfo> slots;

    @JsonProperty(FIELD_NAME_SCHEDULER_STATES)
    private final List<SchedulerStateSpan> schedulerStates;

    @JsonProperty(FIELD_NAME_START_TIMESTAMP)
    private final Long startTimestamp;

    @Nullable
    @JsonProperty(FIELD_NAME_END_TIMESTAMP)
    private final Long endTimestamp;

    @JsonProperty(FIELD_NAME_TERMINAL_STATE)
    private final TerminalState terminalState;

    @JsonProperty(FIELD_NAME_TRIGGER_CAUSE)
    private final TriggerCause triggerCause;

    @JsonProperty(FIELD_NAME_TERMINATED_REASON)
    private final TerminatedReason terminatedReason;

    @JsonCreator
    public JobRescaleDetails(
            @JsonProperty(FIELD_NAME_RESCALE_UUID) String rescaleUuid,
            @JsonProperty(FIELD_NAME_RESOURCE_REQUIREMENTS_UUID) String resourceRequirementsUuid,
            @JsonProperty(FIELD_NAME_RESCALE_ATTEMPT_ID) long rescaleAttemptId,
            @JsonSerialize(keyUsing = JobVertexIDKeySerializer.class)
                    @JsonDeserialize(keyUsing = JobVertexIDKeyDeserializer.class)
                    @JsonProperty(FIELD_NAME_VERTICES)
                    Map<JobVertexID, VertexParallelismRescaleInfo> vertices,
            @JsonSerialize(keyUsing = SlotSharingGroupIDKeySerializer.class)
                    @JsonDeserialize(keyUsing = SlotSharingGroupIDKeyDeserializer.class)
                    @JsonProperty(FIELD_NAME_SLOTS)
                    Map<SlotSharingGroupId, SlotSharingGroupRescaleInfo> slots,
            @JsonProperty(FIELD_NAME_SCHEDULER_STATES) List<SchedulerStateSpan> schedulerStates,
            @JsonProperty(FIELD_NAME_START_TIMESTAMP) Long startTimestamp,
            @Nullable @JsonProperty(FIELD_NAME_END_TIMESTAMP) Long endTimestamp,
            @JsonProperty(FIELD_NAME_TERMINAL_STATE) TerminalState terminalState,
            @JsonProperty(FIELD_NAME_TRIGGER_CAUSE) TriggerCause triggerCause,
            @JsonProperty(FIELD_NAME_TERMINATED_REASON) TerminatedReason terminatedReason) {
        this.rescaleUuid = rescaleUuid;
        this.resourceRequirementsUuid = resourceRequirementsUuid;
        this.rescaleAttemptId = rescaleAttemptId;
        this.vertices = vertices;
        this.slots = slots;
        this.schedulerStates = schedulerStates;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
        this.terminalState = terminalState;
        this.triggerCause = triggerCause;
        this.terminatedReason = terminatedReason;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobRescaleDetails that = (JobRescaleDetails) o;
        return rescaleAttemptId == that.rescaleAttemptId
                && Objects.equals(rescaleUuid, that.rescaleUuid)
                && Objects.equals(resourceRequirementsUuid, that.resourceRequirementsUuid)
                && Objects.equals(vertices, that.vertices)
                && Objects.equals(slots, that.slots)
                && Objects.equals(schedulerStates, that.schedulerStates)
                && Objects.equals(startTimestamp, that.startTimestamp)
                && Objects.equals(endTimestamp, that.endTimestamp)
                && terminalState == that.terminalState
                && triggerCause == that.triggerCause
                && terminatedReason == that.terminatedReason;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                rescaleUuid,
                resourceRequirementsUuid,
                rescaleAttemptId,
                vertices,
                slots,
                schedulerStates,
                startTimestamp,
                endTimestamp,
                terminalState,
                triggerCause,
                terminatedReason);
    }

    public static JobRescaleDetails fromRescale(Rescale rescale, boolean includeDetailedInfo) {
        return new JobRescaleDetails(
                rescale.getRescaleIdInfo().getRescaleUuid().toString(),
                rescale.getRescaleIdInfo().getResourceRequirementsId().toString(),
                rescale.getRescaleIdInfo().getRescaleAttemptId(),
                includeDetailedInfo ? rescale.getVertices() : Collections.emptyMap(),
                includeDetailedInfo
                        ? convertMapValues(
                                rescale.getSlots(),
                                SlotSharingGroupRescaleInfo::fromSlotSharingGroupRescale)
                        : Collections.emptyMap(),
                includeDetailedInfo ? rescale.getSchedulerStates() : Collections.emptyList(),
                rescale.getStartTimestamp(),
                rescale.getEndTimestamp(),
                rescale.getTerminalState(),
                rescale.getTriggerCause(),
                rescale.getTerminatedReason());
    }

    private static <K, NV, OV> Map<K, NV> convertMapValues(
            Map<K, OV> rawMap, Function<OV, NV> valueMapper) {
        return rawMap == null
                ? new HashMap<>()
                : rawMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        kovEntry -> valueMapper.apply(kovEntry.getValue())));
    }

    /** The rescale information of a {@link org.apache.flink.runtime.jobgraph.JobVertex}. */
    public static final class VertexParallelismRescaleInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        public static final String FIELD_NAME_JOB_VERTEX_ID = "jobVertexId";
        public static final String FIELD_NAME_VERTEX_NAME = "jobVertexName";
        public static final String FIELD_NAME_SLOT_SHARING_GROUP_ID = "slotSharingGroupId";
        public static final String FIELD_NAME_SLOT_SHARING_GROUP_NAME = "slotSharingGroupName";
        public static final String FIELD_NAME_DESIRED_PARALLELISM = "desiredParallelism";
        public static final String FIELD_NAME_SUFFICIENT_PARALLELISM = "sufficientParallelism";
        public static final String FIELD_NAME_PRE_RESCALE_PARALLELISM = "preRescaleParallelism";
        public static final String FIELD_NAME_POST_RESCALE_PARALLELISM = "postRescaleParallelism";

        @JsonProperty(FIELD_NAME_JOB_VERTEX_ID)
        @JsonSerialize(using = JobVertexIDSerializer.class)
        @JsonDeserialize(using = JobVertexIDDeserializer.class)
        private final JobVertexID jobVertexId;

        @JsonProperty(FIELD_NAME_VERTEX_NAME)
        private String jobVertexName;

        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID)
        @JsonSerialize(using = SlotSharingGroupIDSerializer.class)
        @JsonDeserialize(using = SlotSharingGroupIDDeserializer.class)
        private SlotSharingGroupId slotSharingGroupId;

        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_NAME)
        private String slotSharingGroupName;

        @JsonProperty(FIELD_NAME_DESIRED_PARALLELISM)
        private Integer desiredParallelism;

        @JsonProperty(FIELD_NAME_SUFFICIENT_PARALLELISM)
        private Integer sufficientParallelism;

        @Nullable
        @JsonProperty(FIELD_NAME_PRE_RESCALE_PARALLELISM)
        private Integer preRescaleParallelism;

        @Nullable
        @JsonProperty(FIELD_NAME_POST_RESCALE_PARALLELISM)
        private Integer postRescaleParallelism;

        @JsonCreator
        public VertexParallelismRescaleInfo(
                @JsonSerialize(using = JobVertexIDSerializer.class)
                        @JsonDeserialize(using = JobVertexIDDeserializer.class)
                        @JsonProperty(FIELD_NAME_JOB_VERTEX_ID)
                        JobVertexID jobVertexId,
                @JsonProperty(FIELD_NAME_VERTEX_NAME) String jobVertexName,
                @JsonSerialize(using = SlotSharingGroupIDSerializer.class)
                        @JsonDeserialize(using = SlotSharingGroupIDDeserializer.class)
                        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID)
                        SlotSharingGroupId slotSharingGroupId,
                @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_NAME) String slotSharingGroupName,
                @JsonProperty(FIELD_NAME_DESIRED_PARALLELISM) Integer desiredParallelism,
                @JsonProperty(FIELD_NAME_SUFFICIENT_PARALLELISM) Integer sufficientParallelism,
                @JsonProperty(FIELD_NAME_PRE_RESCALE_PARALLELISM) Integer preRescaleParallelism,
                @Nullable @JsonProperty(FIELD_NAME_POST_RESCALE_PARALLELISM)
                        Integer postRescaleParallelism) {
            this.jobVertexId = jobVertexId;
            this.jobVertexName = jobVertexName;
            this.slotSharingGroupId = slotSharingGroupId;
            this.slotSharingGroupName = slotSharingGroupName;
            this.desiredParallelism = desiredParallelism;
            this.sufficientParallelism = sufficientParallelism;
            this.preRescaleParallelism = preRescaleParallelism;
            this.postRescaleParallelism = postRescaleParallelism;
        }

        @JsonIgnore
        public VertexParallelismRescaleInfo(
                JobVertexID jobVertexId, String jobVertexName, SlotSharingGroup slotSharingGroup) {
            this.jobVertexId = Preconditions.checkNotNull(jobVertexId);
            this.jobVertexName = jobVertexName;
            this.slotSharingGroupName = slotSharingGroup.getSlotSharingGroupName();
            this.slotSharingGroupId = slotSharingGroup.getSlotSharingGroupId();
        }

        public JobVertexID getJobVertexId() {
            return jobVertexId;
        }

        public String getJobVertexName() {
            return jobVertexName;
        }

        public void setJobVertexName(String jobVertexName) {
            this.jobVertexName = jobVertexName;
        }

        public SlotSharingGroupId getSlotSharingGroupId() {
            return slotSharingGroupId;
        }

        public String getSlotSharingGroupName() {
            return slotSharingGroupName;
        }

        @Nullable
        public Integer getPreRescaleParallelism() {
            return preRescaleParallelism;
        }

        public void setPreRescaleParallelism(@Nullable Integer preRescaleParallelism) {
            this.preRescaleParallelism = preRescaleParallelism;
        }

        public Integer getDesiredParallelism() {
            return desiredParallelism;
        }

        public Integer getSufficientParallelism() {
            return sufficientParallelism;
        }

        public void setRequiredParallelisms(
                VertexParallelismInformation vertexParallelismInformation) {
            this.sufficientParallelism = vertexParallelismInformation.getMinParallelism();
            this.desiredParallelism = vertexParallelismInformation.getParallelism();
        }

        @Nullable
        public Integer getPostRescaleParallelism() {
            return postRescaleParallelism;
        }

        public void setPostRescaleParallelism(Integer postRescaleParallelism) {
            this.postRescaleParallelism = postRescaleParallelism;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            VertexParallelismRescaleInfo that = (VertexParallelismRescaleInfo) o;
            return Objects.equals(jobVertexId, that.jobVertexId)
                    && Objects.equals(jobVertexName, that.jobVertexName)
                    && Objects.equals(slotSharingGroupId, that.slotSharingGroupId)
                    && Objects.equals(slotSharingGroupName, that.slotSharingGroupName)
                    && Objects.equals(preRescaleParallelism, that.preRescaleParallelism)
                    && Objects.equals(desiredParallelism, that.desiredParallelism)
                    && Objects.equals(sufficientParallelism, that.sufficientParallelism)
                    && Objects.equals(postRescaleParallelism, that.postRescaleParallelism);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    jobVertexId,
                    jobVertexName,
                    slotSharingGroupId,
                    slotSharingGroupName,
                    preRescaleParallelism,
                    desiredParallelism,
                    sufficientParallelism,
                    postRescaleParallelism);
        }

        @Override
        public String toString() {
            return "VertexParallelismRescaleInfo{"
                    + "jobVertexId="
                    + jobVertexId
                    + ", jobVertexName='"
                    + jobVertexName
                    + '\''
                    + ", slotSharingGroupId="
                    + slotSharingGroupId
                    + ", slotSharingGroupName='"
                    + slotSharingGroupName
                    + '\''
                    + ", desiredParallelism="
                    + desiredParallelism
                    + ", sufficientParallelism="
                    + sufficientParallelism
                    + ", preRescaleParallelism="
                    + preRescaleParallelism
                    + ", postRescaleParallelism="
                    + postRescaleParallelism
                    + '}';
        }
    }

    public static final class SlotSharingGroupRescaleInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        public static final String FIELD_NAME_SLOT_SHARING_GROUP_ID = "slotSharingGroupId";
        public static final String FIELD_NAME_SLOT_SHARING_GROUP_NAME = "slotSharingGroupName";
        public static final String FIELD_NAME_REQUEST_RESOURCE_PROFILE = "requestResourceProfile";
        public static final String FIELD_NAME_DESIRED_SLOTS = "desiredSlots";
        public static final String FIELD_NAME_MINIMAL_REQUIRED_SLOTS = "minimalRequiredSlots";
        public static final String FIELD_NAME_PRE_RESCALE_SLOTS = "preRescaleSlots";
        public static final String FIELD_NAME_POST_RESCALE_SLOTS = "postRescaleSlots";
        public static final String FIELD_NAME_ACQUIRED_RESOURCE_PROFILE = "acquiredResourceProfile";

        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID)
        @JsonSerialize(using = SlotSharingGroupIDSerializer.class)
        @JsonDeserialize(using = SlotSharingGroupIDDeserializer.class)
        private final SlotSharingGroupId slotSharingGroupId;

        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_NAME)
        private final String slotSharingGroupName;

        @JsonProperty(FIELD_NAME_REQUEST_RESOURCE_PROFILE)
        private final ResourceProfileInfo requestResourceProfileInfo;

        @JsonProperty(FIELD_NAME_DESIRED_SLOTS)
        private final Integer desiredSlots;

        @JsonProperty(FIELD_NAME_MINIMAL_REQUIRED_SLOTS)
        private final Integer minimalRequiredSlots;

        @JsonProperty(FIELD_NAME_PRE_RESCALE_SLOTS)
        private final Integer preRescaleSlots;

        @JsonProperty(FIELD_NAME_POST_RESCALE_SLOTS)
        private final Integer postRescaleSlots;

        @JsonProperty(FIELD_NAME_ACQUIRED_RESOURCE_PROFILE)
        private final ResourceProfileInfo acquiredResourceProfileInfo;

        @JsonCreator
        public SlotSharingGroupRescaleInfo(
                @JsonSerialize(using = SlotSharingGroupIDSerializer.class)
                        @JsonDeserialize(using = SlotSharingGroupIDDeserializer.class)
                        @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_ID)
                        SlotSharingGroupId slotSharingGroupId,
                @JsonProperty(FIELD_NAME_SLOT_SHARING_GROUP_NAME) String slotSharingGroupName,
                @JsonProperty(FIELD_NAME_REQUEST_RESOURCE_PROFILE)
                        ResourceProfileInfo requestResourceProfileInfo,
                @JsonProperty(FIELD_NAME_DESIRED_SLOTS) Integer desiredSlots,
                @JsonProperty(FIELD_NAME_MINIMAL_REQUIRED_SLOTS) Integer minimalRequiredSlots,
                @JsonProperty(FIELD_NAME_PRE_RESCALE_SLOTS) Integer preRescaleSlots,
                @JsonProperty(FIELD_NAME_POST_RESCALE_SLOTS) Integer postRescaleSlots,
                @JsonProperty(FIELD_NAME_ACQUIRED_RESOURCE_PROFILE)
                        ResourceProfileInfo acquiredResourceProfileInfo) {
            this.slotSharingGroupId = slotSharingGroupId;
            this.slotSharingGroupName = slotSharingGroupName;
            this.requestResourceProfileInfo = requestResourceProfileInfo;
            this.desiredSlots = desiredSlots;
            this.minimalRequiredSlots = minimalRequiredSlots;
            this.preRescaleSlots = preRescaleSlots;
            this.postRescaleSlots = postRescaleSlots;
            this.acquiredResourceProfileInfo = acquiredResourceProfileInfo;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SlotSharingGroupRescaleInfo that = (SlotSharingGroupRescaleInfo) o;
            return Objects.equals(slotSharingGroupId, that.slotSharingGroupId)
                    && Objects.equals(slotSharingGroupName, that.slotSharingGroupName)
                    && Objects.equals(requestResourceProfileInfo, that.requestResourceProfileInfo)
                    && Objects.equals(desiredSlots, that.desiredSlots)
                    && Objects.equals(minimalRequiredSlots, that.minimalRequiredSlots)
                    && Objects.equals(preRescaleSlots, that.preRescaleSlots)
                    && Objects.equals(postRescaleSlots, that.postRescaleSlots)
                    && Objects.equals(
                            acquiredResourceProfileInfo, that.acquiredResourceProfileInfo);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    slotSharingGroupId,
                    slotSharingGroupName,
                    requestResourceProfileInfo,
                    desiredSlots,
                    minimalRequiredSlots,
                    preRescaleSlots,
                    postRescaleSlots,
                    acquiredResourceProfileInfo);
        }

        public static SlotSharingGroupRescaleInfo fromSlotSharingGroupRescale(
                SlotSharingGroupRescale slotSharingGroupRescale) {
            return new SlotSharingGroupRescaleInfo(
                    slotSharingGroupRescale.getSlotSharingGroupId(),
                    slotSharingGroupRescale.getSlotSharingGroupName(),
                    ResourceProfileInfo.fromResourceProfile(
                            slotSharingGroupRescale.getRequestResourceProfile()),
                    slotSharingGroupRescale.getDesiredSlots(),
                    slotSharingGroupRescale.getMinimalRequiredSlots(),
                    slotSharingGroupRescale.getPreRescaleSlots(),
                    slotSharingGroupRescale.getPostRescaleSlots(),
                    ResourceProfileInfo.fromResourceProfile(
                            Optional.ofNullable(
                                            slotSharingGroupRescale.getAcquiredResourceProfile())
                                    .orElse(ResourceProfile.UNKNOWN)));
        }
    }
}
