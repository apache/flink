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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.rest.messages.json.JobIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobIDSerializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;
import org.apache.flink.runtime.rest.messages.json.RawJsonDeserializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonRawValue;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/** Details about a job. */
public class JobDetailsInfo implements ResponseBody {

    public static final String FIELD_NAME_JOB_ID = "jid";

    public static final String FIELD_NAME_JOB_NAME = "name";

    public static final String FIELD_NAME_IS_STOPPABLE = "isStoppable";

    public static final String FIELD_NAME_JOB_STATUS = "state";

    public static final String FIELD_NAME_START_TIME = "start-time";

    public static final String FIELD_NAME_END_TIME = "end-time";

    public static final String FIELD_NAME_DURATION = "duration";

    // TODO: For what do we need this???
    public static final String FIELD_NAME_NOW = "now";

    public static final String FIELD_NAME_TIMESTAMPS = "timestamps";

    public static final String FIELD_NAME_JOB_VERTEX_INFOS = "vertices";

    public static final String FIELD_NAME_JOB_VERTICES_PER_STATE = "status-counts";

    public static final String FIELD_NAME_JSON_PLAN = "plan";

    @JsonProperty(FIELD_NAME_JOB_ID)
    @JsonSerialize(using = JobIDSerializer.class)
    private final JobID jobId;

    @JsonProperty(FIELD_NAME_JOB_NAME)
    private final String name;

    @JsonProperty(FIELD_NAME_IS_STOPPABLE)
    private final boolean isStoppable;

    @JsonProperty(FIELD_NAME_JOB_STATUS)
    private final JobStatus jobStatus;

    @JsonProperty(FIELD_NAME_START_TIME)
    private final long startTime;

    @JsonProperty(FIELD_NAME_END_TIME)
    private final long endTime;

    @JsonProperty(FIELD_NAME_DURATION)
    private final long duration;

    @JsonProperty(FIELD_NAME_NOW)
    private final long now;

    @JsonProperty(FIELD_NAME_TIMESTAMPS)
    private final Map<JobStatus, Long> timestamps;

    @JsonProperty(FIELD_NAME_JOB_VERTEX_INFOS)
    private final Collection<JobVertexDetailsInfo> jobVertexInfos;

    @JsonProperty(FIELD_NAME_JOB_VERTICES_PER_STATE)
    private final Map<ExecutionState, Integer> jobVerticesPerState;

    @JsonProperty(FIELD_NAME_JSON_PLAN)
    @JsonRawValue
    private final String jsonPlan;

    @JsonCreator
    public JobDetailsInfo(
            @JsonDeserialize(using = JobIDDeserializer.class) @JsonProperty(FIELD_NAME_JOB_ID)
                    JobID jobId,
            @JsonProperty(FIELD_NAME_JOB_NAME) String name,
            @JsonProperty(FIELD_NAME_IS_STOPPABLE) boolean isStoppable,
            @JsonProperty(FIELD_NAME_JOB_STATUS) JobStatus jobStatus,
            @JsonProperty(FIELD_NAME_START_TIME) long startTime,
            @JsonProperty(FIELD_NAME_END_TIME) long endTime,
            @JsonProperty(FIELD_NAME_DURATION) long duration,
            @JsonProperty(FIELD_NAME_NOW) long now,
            @JsonProperty(FIELD_NAME_TIMESTAMPS) Map<JobStatus, Long> timestamps,
            @JsonProperty(FIELD_NAME_JOB_VERTEX_INFOS)
                    Collection<JobVertexDetailsInfo> jobVertexInfos,
            @JsonProperty(FIELD_NAME_JOB_VERTICES_PER_STATE)
                    Map<ExecutionState, Integer> jobVerticesPerState,
            @JsonProperty(FIELD_NAME_JSON_PLAN) @JsonDeserialize(using = RawJsonDeserializer.class)
                    String jsonPlan) {
        this.jobId = Preconditions.checkNotNull(jobId);
        this.name = Preconditions.checkNotNull(name);
        this.isStoppable = isStoppable;
        this.jobStatus = Preconditions.checkNotNull(jobStatus);
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = duration;
        this.now = now;
        this.timestamps = Preconditions.checkNotNull(timestamps);
        this.jobVertexInfos = Preconditions.checkNotNull(jobVertexInfos);
        this.jobVerticesPerState = Preconditions.checkNotNull(jobVerticesPerState);
        this.jsonPlan = Preconditions.checkNotNull(jsonPlan);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobDetailsInfo that = (JobDetailsInfo) o;
        return isStoppable == that.isStoppable
                && startTime == that.startTime
                && endTime == that.endTime
                && duration == that.duration
                && now == that.now
                && Objects.equals(jobId, that.jobId)
                && Objects.equals(name, that.name)
                && jobStatus == that.jobStatus
                && Objects.equals(timestamps, that.timestamps)
                && Objects.equals(jobVertexInfos, that.jobVertexInfos)
                && Objects.equals(jobVerticesPerState, that.jobVerticesPerState)
                && Objects.equals(jsonPlan, that.jsonPlan);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jobId,
                name,
                isStoppable,
                jobStatus,
                startTime,
                endTime,
                duration,
                now,
                timestamps,
                jobVertexInfos,
                jobVerticesPerState,
                jsonPlan);
    }

    @JsonIgnore
    public JobID getJobId() {
        return jobId;
    }

    @JsonIgnore
    public String getName() {
        return name;
    }

    @JsonIgnore
    public boolean isStoppable() {
        return isStoppable;
    }

    @JsonIgnore
    public JobStatus getJobStatus() {
        return jobStatus;
    }

    @JsonIgnore
    public long getStartTime() {
        return startTime;
    }

    @JsonIgnore
    public long getEndTime() {
        return endTime;
    }

    @JsonIgnore
    public long getDuration() {
        return duration;
    }

    @JsonIgnore
    public long getNow() {
        return now;
    }

    @JsonIgnore
    public Map<JobStatus, Long> getTimestamps() {
        return timestamps;
    }

    @JsonIgnore
    public Collection<JobVertexDetailsInfo> getJobVertexInfos() {
        return jobVertexInfos;
    }

    @JsonIgnore
    public Map<ExecutionState, Integer> getJobVerticesPerState() {
        return jobVerticesPerState;
    }

    @JsonIgnore
    public String getJsonPlan() {
        return jsonPlan;
    }

    // ---------------------------------------------------
    // Static inner classes
    // ---------------------------------------------------

    /** Detailed information about a job vertex. */
    public static final class JobVertexDetailsInfo {

        public static final String FIELD_NAME_JOB_VERTEX_ID = "id";

        public static final String FIELD_NAME_JOB_VERTEX_NAME = "name";

        public static final String FIELD_NAME_PARALLELISM = "parallelism";

        public static final String FIELD_NAME_JOB_VERTEX_STATE = "status";

        public static final String FIELD_NAME_JOB_VERTEX_START_TIME = "start-time";

        public static final String FIELD_NAME_JOB_VERTEX_END_TIME = "end-time";

        public static final String FIELD_NAME_JOB_VERTEX_DURATION = "duration";

        public static final String FIELD_NAME_TASKS_PER_STATE = "tasks";

        public static final String FIELD_NAME_JOB_VERTEX_METRICS = "metrics";

        @JsonProperty(FIELD_NAME_JOB_VERTEX_ID)
        @JsonSerialize(using = JobVertexIDSerializer.class)
        private final JobVertexID jobVertexID;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_NAME)
        private final String name;

        @JsonProperty(FIELD_NAME_PARALLELISM)
        private final int parallelism;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_STATE)
        private final ExecutionState executionState;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_START_TIME)
        private final long startTime;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_END_TIME)
        private final long endTime;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_DURATION)
        private final long duration;

        @JsonProperty(FIELD_NAME_TASKS_PER_STATE)
        private final Map<ExecutionState, Integer> tasksPerState;

        @JsonProperty(FIELD_NAME_JOB_VERTEX_METRICS)
        private final IOMetricsInfo jobVertexMetrics;

        @JsonCreator
        public JobVertexDetailsInfo(
                @JsonDeserialize(using = JobVertexIDDeserializer.class)
                        @JsonProperty(FIELD_NAME_JOB_VERTEX_ID)
                        JobVertexID jobVertexID,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_NAME) String name,
                @JsonProperty(FIELD_NAME_PARALLELISM) int parallelism,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_STATE) ExecutionState executionState,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_START_TIME) long startTime,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_END_TIME) long endTime,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_DURATION) long duration,
                @JsonProperty(FIELD_NAME_TASKS_PER_STATE)
                        Map<ExecutionState, Integer> tasksPerState,
                @JsonProperty(FIELD_NAME_JOB_VERTEX_METRICS) IOMetricsInfo jobVertexMetrics) {
            this.jobVertexID = Preconditions.checkNotNull(jobVertexID);
            this.name = Preconditions.checkNotNull(name);
            this.parallelism = parallelism;
            this.executionState = Preconditions.checkNotNull(executionState);
            this.startTime = startTime;
            this.endTime = endTime;
            this.duration = duration;
            this.tasksPerState = Preconditions.checkNotNull(tasksPerState);
            this.jobVertexMetrics = Preconditions.checkNotNull(jobVertexMetrics);
        }

        @JsonIgnore
        public JobVertexID getJobVertexID() {
            return jobVertexID;
        }

        @JsonIgnore
        public String getName() {
            return name;
        }

        @JsonIgnore
        public int getParallelism() {
            return parallelism;
        }

        @JsonIgnore
        public ExecutionState getExecutionState() {
            return executionState;
        }

        @JsonIgnore
        public long getStartTime() {
            return startTime;
        }

        @JsonIgnore
        public long getEndTime() {
            return endTime;
        }

        @JsonIgnore
        public long getDuration() {
            return duration;
        }

        @JsonIgnore
        public Map<ExecutionState, Integer> getTasksPerState() {
            return tasksPerState;
        }

        @JsonIgnore
        public IOMetricsInfo getJobVertexMetrics() {
            return jobVertexMetrics;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            JobVertexDetailsInfo that = (JobVertexDetailsInfo) o;
            return parallelism == that.parallelism
                    && startTime == that.startTime
                    && endTime == that.endTime
                    && duration == that.duration
                    && Objects.equals(jobVertexID, that.jobVertexID)
                    && Objects.equals(name, that.name)
                    && executionState == that.executionState
                    && Objects.equals(tasksPerState, that.tasksPerState)
                    && Objects.equals(jobVertexMetrics, that.jobVertexMetrics);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    jobVertexID,
                    name,
                    parallelism,
                    executionState,
                    startTime,
                    endTime,
                    duration,
                    tasksPerState,
                    jobVertexMetrics);
        }
    }
}
