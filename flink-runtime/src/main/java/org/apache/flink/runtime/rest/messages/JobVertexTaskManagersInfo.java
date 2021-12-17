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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.job.JobVertexTaskManagersHandler;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobVertexIDSerializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Response type of the {@link JobVertexTaskManagersHandler}. */
public class JobVertexTaskManagersInfo implements ResponseBody {
    public static final String VERTEX_TASK_FIELD_ID = "id";
    public static final String VERTEX_TASK_FIELD_NAME = "name";
    public static final String VERTEX_TASK_FIELD_NOW = "now";
    public static final String VERTEX_TASK_FIELD_TASK_MANAGERS = "taskmanagers";

    @JsonProperty(VERTEX_TASK_FIELD_ID)
    @JsonSerialize(using = JobVertexIDSerializer.class)
    private final JobVertexID jobVertexID;

    @JsonProperty(VERTEX_TASK_FIELD_NAME)
    private final String name;

    @JsonProperty(VERTEX_TASK_FIELD_NOW)
    private final long now;

    @JsonProperty(VERTEX_TASK_FIELD_TASK_MANAGERS)
    private Collection<TaskManagersInfo> taskManagerInfos;

    @JsonCreator
    public JobVertexTaskManagersInfo(
            @JsonDeserialize(using = JobVertexIDDeserializer.class)
                    @JsonProperty(VERTEX_TASK_FIELD_ID)
                    JobVertexID jobVertexID,
            @JsonProperty(VERTEX_TASK_FIELD_NAME) String name,
            @JsonProperty(VERTEX_TASK_FIELD_NOW) long now,
            @JsonProperty(VERTEX_TASK_FIELD_TASK_MANAGERS)
                    Collection<TaskManagersInfo> taskManagerInfos) {
        this.jobVertexID = checkNotNull(jobVertexID);
        this.name = checkNotNull(name);
        this.now = now;
        this.taskManagerInfos = checkNotNull(taskManagerInfos);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobVertexTaskManagersInfo that = (JobVertexTaskManagersInfo) o;
        return Objects.equals(jobVertexID, that.jobVertexID)
                && Objects.equals(name, that.name)
                && now == that.now
                && Objects.equals(taskManagerInfos, that.taskManagerInfos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobVertexID, name, now, taskManagerInfos);
    }

    // ---------------------------------------------------
    // Static inner classes
    // ---------------------------------------------------

    /** Detailed information about task managers. */
    public static class TaskManagersInfo {
        public static final String TASK_MANAGERS_FIELD_HOST = "host";
        public static final String TASK_MANAGERS_FIELD_STATUS = "status";
        public static final String TASK_MANAGERS_FIELD_START_TIME = "start-time";
        public static final String TASK_MANAGERS_FIELD_END_TIME = "end-time";
        public static final String TASK_MANAGERS_FIELD_DURATION = "duration";
        public static final String TASK_MANAGERS_FIELD_METRICS = "metrics";
        public static final String TASK_MANAGERS_FIELD_STATUS_COUNTS = "status-counts";
        public static final String TASK_MANAGERS_FIELD_TASKMANAGER_ID = "taskmanager-id";

        @JsonProperty(TASK_MANAGERS_FIELD_HOST)
        private final String host;

        @JsonProperty(TASK_MANAGERS_FIELD_STATUS)
        private final ExecutionState status;

        @JsonProperty(TASK_MANAGERS_FIELD_START_TIME)
        private final long startTime;

        @JsonProperty(TASK_MANAGERS_FIELD_END_TIME)
        private final long endTime;

        @JsonProperty(TASK_MANAGERS_FIELD_DURATION)
        private final long duration;

        @JsonProperty(TASK_MANAGERS_FIELD_METRICS)
        private final IOMetricsInfo metrics;

        @JsonProperty(TASK_MANAGERS_FIELD_STATUS_COUNTS)
        private final Map<ExecutionState, Integer> statusCounts;

        @JsonProperty(TASK_MANAGERS_FIELD_TASKMANAGER_ID)
        private final String taskmanagerId;

        @JsonCreator
        public TaskManagersInfo(
                @JsonProperty(TASK_MANAGERS_FIELD_HOST) String host,
                @JsonProperty(TASK_MANAGERS_FIELD_STATUS) ExecutionState status,
                @JsonProperty(TASK_MANAGERS_FIELD_START_TIME) long startTime,
                @JsonProperty(TASK_MANAGERS_FIELD_END_TIME) long endTime,
                @JsonProperty(TASK_MANAGERS_FIELD_DURATION) long duration,
                @JsonProperty(TASK_MANAGERS_FIELD_METRICS) IOMetricsInfo metrics,
                @JsonProperty(TASK_MANAGERS_FIELD_STATUS_COUNTS)
                        Map<ExecutionState, Integer> statusCounts,
                @JsonProperty(TASK_MANAGERS_FIELD_TASKMANAGER_ID) String taskmanagerId) {
            this.host = checkNotNull(host);
            this.status = checkNotNull(status);
            this.startTime = startTime;
            this.endTime = endTime;
            this.duration = duration;
            this.metrics = checkNotNull(metrics);
            this.statusCounts = checkNotNull(statusCounts);
            this.taskmanagerId = taskmanagerId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TaskManagersInfo that = (TaskManagersInfo) o;
            return Objects.equals(host, that.host)
                    && Objects.equals(status, that.status)
                    && startTime == that.startTime
                    && endTime == that.endTime
                    && duration == that.duration
                    && Objects.equals(metrics, that.metrics)
                    && Objects.equals(statusCounts, that.statusCounts)
                    && Objects.equals(taskmanagerId, that.taskmanagerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    host,
                    status,
                    startTime,
                    endTime,
                    duration,
                    metrics,
                    statusCounts,
                    taskmanagerId);
        }
    }
}
