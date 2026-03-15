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

package org.apache.flink.runtime.rest.handler.legacy.messages;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/** Response body for Top N metrics aggregation. */
public class TopNMetricsResponseBody implements ResponseBody {

    private static final long serialVersionUID = 1L;

    public static final String FIELD_NAME_TOP_CPU_CONSUMERS = "topCpuConsumers";
    public static final String FIELD_NAME_TOP_BACKPRESSURE_OPERATORS = "topBackpressureOperators";
    public static final String FIELD_NAME_TOP_GC_INTENSIVE_TASKS = "topGcIntensiveTasks";

    @JsonProperty(FIELD_NAME_TOP_CPU_CONSUMERS)
    private final List<CpuConsumerInfo> topCpuConsumers;

    @JsonProperty(FIELD_NAME_TOP_BACKPRESSURE_OPERATORS)
    private final List<BackpressureOperatorInfo> topBackpressureOperators;

    @JsonProperty(FIELD_NAME_TOP_GC_INTENSIVE_TASKS)
    private final List<GcTaskInfo> topGcIntensiveTasks;

    @JsonCreator
    public TopNMetricsResponseBody(
            @JsonProperty(FIELD_NAME_TOP_CPU_CONSUMERS)
                    List<CpuConsumerInfo> topCpuConsumers,
            @JsonProperty(FIELD_NAME_TOP_BACKPRESSURE_OPERATORS)
                    List<BackpressureOperatorInfo> topBackpressureOperators,
            @JsonProperty(FIELD_NAME_TOP_GC_INTENSIVE_TASKS)
                    List<GcTaskInfo> topGcIntensiveTasks) {
        this.topCpuConsumers = topCpuConsumers;
        this.topBackpressureOperators = topBackpressureOperators;
        this.topGcIntensiveTasks = topGcIntensiveTasks;
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
                && Objects.equals(topGcIntensiveTasks, that.topGcIntensiveTasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topCpuConsumers, topBackpressureOperators, topGcIntensiveTasks);
    }

    /** Information about CPU-intensive subtasks. */
    public static class CpuConsumerInfo {

        public static final String FIELD_NAME_SUBTASK_ID = "subtaskId";
        public static final String FIELD_NAME_TASK_NAME = "taskName";
        public static final String FIELD_NAME_OPERATOR_NAME = "operatorName";
        public static final String FIELD_NAME_CPU_PERCENTAGE = "cpuPercentage";
        public static final String FIELD_NAME_TASKMANAGER_ID = "taskManagerId";

        @JsonProperty(FIELD_NAME_SUBTASK_ID)
        private final int subtaskId;

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
                @JsonProperty(FIELD_NAME_SUBTASK_ID) int subtaskId,
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

        public int getSubtaskId() {
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
            return subtaskId == that.subtaskId
                    && Double.compare(that.cpuPercentage, cpuPercentage) == 0
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
}
