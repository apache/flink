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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Response body for Top N metrics aggregation. */
public class TopNMetricsResponseBody implements ResponseBody {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_TOP_CPU_CONSUMERS = "topCpuConsumers";
    public static final String FIELD_NAME_TOP_BACKPRESSURE_OPERATORS = "topBackpressureOperators";
    public static final String FIELD_NAME_TOP_GC_INTENSIVE_TASKS = "topGcIntensiveTasks";
    public static final String FIELD_NAME_TOP_BUSY_OPERATORS = "topBusyOperators";
    public static final String FIELD_NAME_TOP_LAGGING_SOURCES = "topLaggingSources";

    @JsonProperty(FIELD_NAME_TOP_CPU_CONSUMERS)
    private final List<CpuConsumerInfo> topCpuConsumers;

    @JsonProperty(FIELD_NAME_TOP_BACKPRESSURE_OPERATORS)
    private final List<BackpressureOperatorInfo> topBackpressureOperators;

    @JsonProperty(FIELD_NAME_TOP_GC_INTENSIVE_TASKS)
    private final List<GcTaskInfo> topGcIntensiveTasks;

    @JsonProperty(FIELD_NAME_TOP_BUSY_OPERATORS)
    private final List<BusyOperatorInfo> topBusyOperators;

    @JsonProperty(FIELD_NAME_TOP_LAGGING_SOURCES)
    private final List<SourceLagInfo> topLaggingSources;

    @JsonCreator
    public TopNMetricsResponseBody(
            @JsonProperty(FIELD_NAME_TOP_CPU_CONSUMERS) List<CpuConsumerInfo> topCpuConsumers,
            @JsonProperty(FIELD_NAME_TOP_BACKPRESSURE_OPERATORS)
                    List<BackpressureOperatorInfo> topBackpressureOperators,
            @JsonProperty(FIELD_NAME_TOP_GC_INTENSIVE_TASKS) List<GcTaskInfo> topGcIntensiveTasks,
            @JsonProperty(FIELD_NAME_TOP_BUSY_OPERATORS) List<BusyOperatorInfo> topBusyOperators,
            @JsonProperty(FIELD_NAME_TOP_LAGGING_SOURCES) List<SourceLagInfo> topLaggingSources) {
        this.topCpuConsumers = topCpuConsumers == null ? Collections.emptyList() : topCpuConsumers;
        this.topBackpressureOperators =
                topBackpressureOperators == null
                        ? Collections.emptyList()
                        : topBackpressureOperators;
        this.topGcIntensiveTasks =
                topGcIntensiveTasks == null ? Collections.emptyList() : topGcIntensiveTasks;
        this.topBusyOperators =
                topBusyOperators == null ? Collections.emptyList() : topBusyOperators;
        this.topLaggingSources =
                topLaggingSources == null ? Collections.emptyList() : topLaggingSources;
    }

    public List<CpuConsumerInfo> getTopCpuConsumers() {
        return topCpuConsumers;
    }

    public List<BackpressureOperatorInfo> getTopBackpressureOperators() {
        return topBackpressureOperators;
    }

    public List<GcTaskInfo> getTopGcIntensiveTasks() {
        return topGcIntensiveTasks;
    }

    public List<BusyOperatorInfo> getTopBusyOperators() {
        return topBusyOperators;
    }

    public List<SourceLagInfo> getTopLaggingSources() {
        return topLaggingSources;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopNMetricsResponseBody that = (TopNMetricsResponseBody) o;
        return Objects.equals(topCpuConsumers, that.topCpuConsumers)
                && Objects.equals(topBackpressureOperators, that.topBackpressureOperators)
                && Objects.equals(topGcIntensiveTasks, that.topGcIntensiveTasks)
                && Objects.equals(topBusyOperators, that.topBusyOperators)
                && Objects.equals(topLaggingSources, that.topLaggingSources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                topCpuConsumers,
                topBackpressureOperators,
                topGcIntensiveTasks,
                topBusyOperators,
                topLaggingSources);
    }

    /** Information about CPU-intensive subtasks. */
    public static class CpuConsumerInfo {

        public static final String FIELD_NAME_SUBTASK_ID = "subtaskId";
        public static final String FIELD_NAME_TASK_NAME = "taskName";
        public static final String FIELD_NAME_OPERATOR_NAME = "operatorName";
        public static final String FIELD_NAME_CPU_PERCENTAGE = "cpuPercentage";
        public static final String FIELD_NAME_TASKMANAGER_ID = "taskManagerId";

        /**
         * {@code null} for TaskManager-scoped CPU readings (e.g. aggregated process CPU load of the
         * TaskManager JVM); otherwise the originating subtask index.
         */
        @JsonProperty(FIELD_NAME_SUBTASK_ID)
        private final Integer subtaskId;

        @JsonProperty(FIELD_NAME_TASK_NAME)
        private final String taskName;

        @JsonProperty(FIELD_NAME_OPERATOR_NAME)
        private final String operatorName;

        @JsonProperty(FIELD_NAME_CPU_PERCENTAGE)
        private final double cpuPercentage;

        @JsonProperty(FIELD_NAME_TASKMANAGER_ID)
        private final String taskManagerId;

        @JsonCreator
        public CpuConsumerInfo(
                @JsonProperty(FIELD_NAME_SUBTASK_ID) Integer subtaskId,
                @JsonProperty(FIELD_NAME_TASK_NAME) String taskName,
                @JsonProperty(FIELD_NAME_OPERATOR_NAME) String operatorName,
                @JsonProperty(FIELD_NAME_CPU_PERCENTAGE) double cpuPercentage,
                @JsonProperty(FIELD_NAME_TASKMANAGER_ID) String taskManagerId) {
            this.subtaskId = subtaskId;
            this.taskName = taskName;
            this.operatorName = operatorName;
            this.cpuPercentage = cpuPercentage;
            this.taskManagerId = taskManagerId;
        }

        public Integer getSubtaskId() {
            return subtaskId;
        }

        public String getTaskName() {
            return taskName;
        }

        public String getOperatorName() {
            return operatorName;
        }

        public double getCpuPercentage() {
            return cpuPercentage;
        }

        public String getTaskManagerId() {
            return taskManagerId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CpuConsumerInfo that = (CpuConsumerInfo) o;
            return Double.compare(that.cpuPercentage, cpuPercentage) == 0
                    && Objects.equals(subtaskId, that.subtaskId)
                    && Objects.equals(taskName, that.taskName)
                    && Objects.equals(operatorName, that.operatorName)
                    && Objects.equals(taskManagerId, that.taskManagerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subtaskId, taskName, operatorName, cpuPercentage, taskManagerId);
        }
    }

    /** Information about backpressured operators. */
    public static class BackpressureOperatorInfo {

        public static final String FIELD_NAME_OPERATOR_ID = "operatorId";
        public static final String FIELD_NAME_OPERATOR_NAME = "operatorName";
        public static final String FIELD_NAME_BACKPRESSURE_RATIO = "backpressureRatio";
        public static final String FIELD_NAME_SUBTASK_ID = "subtaskId";

        @JsonProperty(FIELD_NAME_OPERATOR_ID)
        private final String operatorId;

        @JsonProperty(FIELD_NAME_OPERATOR_NAME)
        private final String operatorName;

        @JsonProperty(FIELD_NAME_BACKPRESSURE_RATIO)
        private final double backpressureRatio;

        @JsonProperty(FIELD_NAME_SUBTASK_ID)
        private final int subtaskId;

        @JsonCreator
        public BackpressureOperatorInfo(
                @JsonProperty(FIELD_NAME_OPERATOR_ID) String operatorId,
                @JsonProperty(FIELD_NAME_OPERATOR_NAME) String operatorName,
                @JsonProperty(FIELD_NAME_BACKPRESSURE_RATIO) double backpressureRatio,
                @JsonProperty(FIELD_NAME_SUBTASK_ID) int subtaskId) {
            this.operatorId = operatorId;
            this.operatorName = operatorName;
            this.backpressureRatio = backpressureRatio;
            this.subtaskId = subtaskId;
        }

        public String getOperatorId() {
            return operatorId;
        }

        public String getOperatorName() {
            return operatorName;
        }

        public double getBackpressureRatio() {
            return backpressureRatio;
        }

        public int getSubtaskId() {
            return subtaskId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BackpressureOperatorInfo that = (BackpressureOperatorInfo) o;
            return Double.compare(that.backpressureRatio, backpressureRatio) == 0
                    && subtaskId == that.subtaskId
                    && Objects.equals(operatorId, that.operatorId)
                    && Objects.equals(operatorName, that.operatorName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(operatorId, operatorName, backpressureRatio, subtaskId);
        }
    }

    /** Information about GC-intensive tasks. */
    public static class GcTaskInfo {

        public static final String FIELD_NAME_TASK_ID = "taskId";
        public static final String FIELD_NAME_TASK_NAME = "taskName";
        public static final String FIELD_NAME_GC_TIME_PERCENTAGE = "gcTimePercentage";
        public static final String FIELD_NAME_TASKMANAGER_ID = "taskManagerId";

        @JsonProperty(FIELD_NAME_TASK_ID)
        private final String taskId;

        @JsonProperty(FIELD_NAME_TASK_NAME)
        private final String taskName;

        @JsonProperty(FIELD_NAME_GC_TIME_PERCENTAGE)
        private final double gcTimePercentage;

        @JsonProperty(FIELD_NAME_TASKMANAGER_ID)
        private final String taskManagerId;

        @JsonCreator
        public GcTaskInfo(
                @JsonProperty(FIELD_NAME_TASK_ID) String taskId,
                @JsonProperty(FIELD_NAME_TASK_NAME) String taskName,
                @JsonProperty(FIELD_NAME_GC_TIME_PERCENTAGE) double gcTimePercentage,
                @JsonProperty(FIELD_NAME_TASKMANAGER_ID) String taskManagerId) {
            this.taskId = taskId;
            this.taskName = taskName;
            this.gcTimePercentage = gcTimePercentage;
            this.taskManagerId = taskManagerId;
        }

        public String getTaskId() {
            return taskId;
        }

        public String getTaskName() {
            return taskName;
        }

        public double getGcTimePercentage() {
            return gcTimePercentage;
        }

        public String getTaskManagerId() {
            return taskManagerId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GcTaskInfo that = (GcTaskInfo) o;
            return Double.compare(that.gcTimePercentage, gcTimePercentage) == 0
                    && Objects.equals(taskId, that.taskId)
                    && Objects.equals(taskName, that.taskName)
                    && Objects.equals(taskManagerId, that.taskManagerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskId, taskName, gcTimePercentage, taskManagerId);
        }
    }

    /**
     * Information about "busy" subtasks, scored by {@code busyTimeMsPerSecond} (range [0, 1000],
     * exposed here as a ratio in [0, 1]). Structurally parallel to {@link BackpressureOperatorInfo}
     * so the UI can render them side-by-side.
     */
    public static class BusyOperatorInfo {

        public static final String FIELD_NAME_OPERATOR_ID = "operatorId";
        public static final String FIELD_NAME_OPERATOR_NAME = "operatorName";
        public static final String FIELD_NAME_BUSY_RATIO = "busyRatio";
        public static final String FIELD_NAME_SUBTASK_ID = "subtaskId";

        @JsonProperty(FIELD_NAME_OPERATOR_ID)
        private final String operatorId;

        @JsonProperty(FIELD_NAME_OPERATOR_NAME)
        private final String operatorName;

        @JsonProperty(FIELD_NAME_BUSY_RATIO)
        private final double busyRatio;

        @JsonProperty(FIELD_NAME_SUBTASK_ID)
        private final int subtaskId;

        @JsonCreator
        public BusyOperatorInfo(
                @JsonProperty(FIELD_NAME_OPERATOR_ID) String operatorId,
                @JsonProperty(FIELD_NAME_OPERATOR_NAME) String operatorName,
                @JsonProperty(FIELD_NAME_BUSY_RATIO) double busyRatio,
                @JsonProperty(FIELD_NAME_SUBTASK_ID) int subtaskId) {
            this.operatorId = operatorId;
            this.operatorName = operatorName;
            this.busyRatio = busyRatio;
            this.subtaskId = subtaskId;
        }

        public String getOperatorId() {
            return operatorId;
        }

        public String getOperatorName() {
            return operatorName;
        }

        public double getBusyRatio() {
            return busyRatio;
        }

        public int getSubtaskId() {
            return subtaskId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BusyOperatorInfo that = (BusyOperatorInfo) o;
            return Double.compare(that.busyRatio, busyRatio) == 0
                    && subtaskId == that.subtaskId
                    && Objects.equals(operatorId, that.operatorId)
                    && Objects.equals(operatorName, that.operatorName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(operatorId, operatorName, busyRatio, subtaskId);
        }
    }

    /**
     * Information about lagging source vertices. A "source vertex" here means any vertex that
     * exposes at least one of {@code pendingRecords}, {@code currentFetchEventTimeLag}, or {@code
     * currentEmitEventTimeLag} on any of its subtasks; true input-topology detection is not
     * available in the metric store.
     *
     * <p>{@link #pendingRecords} is the cluster-level sum across subtasks (unit: records; {@code
     * null} if unavailable). {@link #maxFetchEventTimeLagMs} and {@link #maxEmitEventTimeLagMs} are
     * the maxima across subtasks of the vertex (unit: milliseconds; {@code null} if unavailable).
     */
    public static class SourceLagInfo {

        public static final String FIELD_NAME_VERTEX_ID = "vertexId";
        public static final String FIELD_NAME_VERTEX_NAME = "vertexName";
        public static final String FIELD_NAME_PENDING_RECORDS = "pendingRecords";
        public static final String FIELD_NAME_MAX_FETCH_EVENT_TIME_LAG_MS =
                "maxFetchEventTimeLagMs";
        public static final String FIELD_NAME_MAX_EMIT_EVENT_TIME_LAG_MS = "maxEmitEventTimeLagMs";

        @JsonProperty(FIELD_NAME_VERTEX_ID)
        private final String vertexId;

        @JsonProperty(FIELD_NAME_VERTEX_NAME)
        private final String vertexName;

        /** {@code null} when the underlying source does not report this metric. */
        @JsonProperty(FIELD_NAME_PENDING_RECORDS)
        private final Double pendingRecords;

        /** {@code null} when the underlying source does not report this metric. */
        @JsonProperty(FIELD_NAME_MAX_FETCH_EVENT_TIME_LAG_MS)
        private final Double maxFetchEventTimeLagMs;

        /** {@code null} when the underlying source does not report this metric. */
        @JsonProperty(FIELD_NAME_MAX_EMIT_EVENT_TIME_LAG_MS)
        private final Double maxEmitEventTimeLagMs;

        @JsonCreator
        public SourceLagInfo(
                @JsonProperty(FIELD_NAME_VERTEX_ID) String vertexId,
                @JsonProperty(FIELD_NAME_VERTEX_NAME) String vertexName,
                @JsonProperty(FIELD_NAME_PENDING_RECORDS) Double pendingRecords,
                @JsonProperty(FIELD_NAME_MAX_FETCH_EVENT_TIME_LAG_MS) Double maxFetchEventTimeLagMs,
                @JsonProperty(FIELD_NAME_MAX_EMIT_EVENT_TIME_LAG_MS) Double maxEmitEventTimeLagMs) {
            this.vertexId = vertexId;
            this.vertexName = vertexName;
            this.pendingRecords = pendingRecords;
            this.maxFetchEventTimeLagMs = maxFetchEventTimeLagMs;
            this.maxEmitEventTimeLagMs = maxEmitEventTimeLagMs;
        }

        public String getVertexId() {
            return vertexId;
        }

        public String getVertexName() {
            return vertexName;
        }

        /** May be {@code null} when {@code pendingRecords} is not reported by the source. */
        public Double getPendingRecords() {
            return pendingRecords;
        }

        /** May be {@code null} when {@code fetchEventTimeLag} is not reported by the source. */
        public Double getMaxFetchEventTimeLagMs() {
            return maxFetchEventTimeLagMs;
        }

        /** May be {@code null} when {@code emitEventTimeLag} is not reported by the source. */
        public Double getMaxEmitEventTimeLagMs() {
            return maxEmitEventTimeLagMs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SourceLagInfo that = (SourceLagInfo) o;
            return Objects.equals(pendingRecords, that.pendingRecords)
                    && Objects.equals(maxFetchEventTimeLagMs, that.maxFetchEventTimeLagMs)
                    && Objects.equals(maxEmitEventTimeLagMs, that.maxEmitEventTimeLagMs)
                    && Objects.equals(vertexId, that.vertexId)
                    && Objects.equals(vertexName, that.vertexName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    vertexId,
                    vertexName,
                    pendingRecords,
                    maxFetchEventTimeLagMs,
                    maxEmitEventTimeLagMs);
        }
    }
}
