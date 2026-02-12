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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.job.metrics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Response body for Top N metrics. */
public class TopNMetricsResponseBody implements ResponseBody {

    private static final String FIELD_NAME_TOP_CPU_CONSUMERS = "topCpuConsumers";
    private static final String FIELD_NAME_TOP_BACKPRESSURE_OPERATORS = "topBackpressureOperators";
    private static final String FIELD_NAME_TOP_GC_INTENSIVE_TASKS = "topGcIntensiveTasks";

    @JsonProperty(FIELD_NAME_TOP_CPU_CONSUMERS)
    private final List<CpuConsumer> topCpuConsumers;

    @JsonProperty(FIELD_NAME_TOP_BACKPRESSURE_OPERATORS)
    private final List<BackpressureOperator> topBackpressureOperators;

    @JsonProperty(FIELD_NAME_TOP_GC_INTENSIVE_TASKS)
    private final List<GcIntensiveTask> topGcIntensiveTasks;

    @JsonCreator
    public TopNMetricsResponseBody(
            @JsonProperty(FIELD_NAME_TOP_CPU_CONSUMERS) List<CpuConsumer> topCpuConsumers,
            @JsonProperty(FIELD_NAME_TOP_BACKPRESSURE_OPERATORS) List<BackpressureOperator> topBackpressureOperators,
            @JsonProperty(FIELD_NAME_TOP_GC_INTENSIVE_TASKS) List<GcIntensiveTask> topGcIntensiveTasks) {
        this.topCpuConsumers = topCpuConsumers != null ? topCpuConsumers : Collections.emptyList();
        this.topBackpressureOperators =
                topBackpressureOperators != null ? topBackpressureOperators : Collections.emptyList();
        this.topGcIntensiveTasks =
                topGcIntensiveTasks != null ? topGcIntensiveTasks : Collections.emptyList();
    }

    @JsonProperty(FIELD_NAME_TOP_CPU_CONSUMERS)
    public List<CpuConsumer> getTopCpuConsumers() {
        return topCpuConsumers;
    }

    @JsonProperty(FIELD_NAME_TOP_BACKPRESSURE_OPERATORS)
    public List<BackpressureOperator> getTopBackpressureOperators() {
        return topBackpressureOperators;
    }

    @JsonProperty(FIELD_NAME_TOP_GC_INTENSIVE_TASKS)
    public List<GcIntensiveTask> getTopGcIntensiveTasks() {
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

    /** CPU consumer metrics. */
    public static class CpuConsumer {
        private static final String FIELD_NAME_SUBTASK_ID = "subtaskId";
        private static final String FIELD_NAME_TASK_NAME = "taskName";
        private static final String FIELD_NAME_CPU_PERCENTAGE = "cpuPercentage";
        private static final String FIELD_NAME_TASK_MANAGER_ID = "taskManagerId";

        @JsonProperty(FIELD_NAME_SUBTASK_ID)
        private final int subtaskId;

        @JsonProperty(FIELD_NAME_TASK_NAME)
        private final String taskName;

        @JsonProperty(FIELD_NAME_CPU_PERCENTAGE)
        private final double cpuPercentage;

        @JsonProperty(FIELD_NAME_TASK_MANAGER_ID)
        private final String taskManagerId;

        @JsonCreator
        public CpuConsumer(
                @JsonProperty(FIELD_NAME_SUBTASK_ID) int subtaskId,
                @JsonProperty(FIELD_NAME_TASK_NAME) String taskName,
                @JsonProperty(FIELD_NAME_CPU_PERCENTAGE) double cpuPercentage,
                @JsonProperty(FIELD_NAME_TASK_MANAGER_ID) String taskManagerId) {
            this.subtaskId = subtaskId;
            this.taskName = taskName;
            this.cpuPercentage = cpuPercentage;
            this.taskManagerId = taskManagerId;
        }

        public int getSubtaskId() {
            return subtaskId;
        }

        public String getTaskName() {
            return taskName;
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
            CpuConsumer that = (CpuConsumer) o;
            return subtaskId == that.subtaskId
                    && Double.compare(that.cpuPercentage, cpuPercentage) == 0
                    && Objects.equals(taskName, that.taskName)
                    && Objects.equals(taskManagerId, that.taskManagerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(subtaskId, taskName, cpuPercentage, taskManagerId);
        }
    }

    /** Backpressure operator metrics. */
    public static class BackpressureOperator {
        private static final String FIELD_NAME_OPERATOR_ID = "operatorId";
        private static final String FIELD_NAME_OPERATOR_NAME = "operatorName";
        private static final String FIELD_NAME_BACKPRESSURE_RATIO = "backpressureRatio";
        private static final String FIELD_NAME_SUBTASK_ID = "subtaskId";

        @JsonProperty(FIELD_NAME_OPERATOR_ID)
        private final String operatorId;

        @JsonProperty(FIELD_NAME_OPERATOR_NAME)
        private final String operatorName;

        @JsonProperty(FIELD_NAME_BACKPRESSURE_RATIO)
        private final double backpressureRatio;

        @JsonProperty(FIELD_NAME_SUBTASK_ID)
        private final int subtaskId;

        @JsonCreator
        public BackpressureOperator(
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
            BackpressureOperator that = (BackpressureOperator) o;
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

    /** GC intensive task metrics. */
    public static class GcIntensiveTask {
        private static final String FIELD_NAME_TASK_NAME = "taskName";
        private static final String FIELD_NAME_GC_TIME_PERCENTAGE = "gcTimePercentage";
        private static final String FIELD_NAME_TASK_MANAGER_ID = "taskManagerId";

        @JsonProperty(FIELD_NAME_TASK_NAME)
        private final String taskName;

        @JsonProperty(FIELD_NAME_GC_TIME_PERCENTAGE)
        private final double gcTimePercentage;

        @JsonProperty(FIELD_NAME_TASK_MANAGER_ID)
        private final String taskManagerId;

        @JsonCreator
        public GcIntensiveTask(
                @JsonProperty(FIELD_NAME_TASK_NAME) String taskName,
                @JsonProperty(FIELD_NAME_GC_TIME_PERCENTAGE) double gcTimePercentage,
                @JsonProperty(FIELD_NAME_TASK_MANAGER_ID) String taskManagerId) {
            this.taskName = taskName;
            this.gcTimePercentage = gcTimePercentage;
            this.taskManagerId = taskManagerId;
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
            GcIntensiveTask that = (GcIntensiveTask) o;
            return Double.compare(that.gcTimePercentage, gcTimePercentage) == 0
                    && Objects.equals(taskName, that.taskName)
                    && Objects.equals(taskManagerId, that.taskManagerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(taskName, gcTimePercentage, taskManagerId);
        }
    }
}
