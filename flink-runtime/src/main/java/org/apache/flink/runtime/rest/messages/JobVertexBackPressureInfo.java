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

import org.apache.flink.runtime.rest.handler.job.JobVertexBackPressureHandler;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import io.swagger.v3.oas.annotations.Hidden;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Response type of the {@link JobVertexBackPressureHandler}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobVertexBackPressureInfo implements ResponseBody {

    public static final String FIELD_NAME_STATUS = "status";
    public static final String DEPRECATED_FIELD_NAME_BACKPRESSURE_LEVEL = "backpressure-level";
    public static final String FIELD_NAME_BACKPRESSURE_LEVEL = "backpressureLevel";
    public static final String FIELD_NAME_END_TIMESTAMP = "end-timestamp";
    public static final String FIELD_NAME_SUBTASKS = "subtasks";

    /** Immutable singleton instance denoting that the back pressure stats are not available. */
    private static final JobVertexBackPressureInfo DEPRECATED_JOB_VERTEX_BACK_PRESSURE_INFO =
            new JobVertexBackPressureInfo(VertexBackPressureStatus.DEPRECATED, null, null, null);

    @JsonProperty(FIELD_NAME_STATUS)
    private final VertexBackPressureStatus status;

    @JsonProperty(FIELD_NAME_BACKPRESSURE_LEVEL)
    @JsonAlias(DEPRECATED_FIELD_NAME_BACKPRESSURE_LEVEL)
    private final VertexBackPressureLevel backpressureLevel;

    @JsonProperty(FIELD_NAME_END_TIMESTAMP)
    private final Long endTimestamp;

    @JsonProperty(FIELD_NAME_SUBTASKS)
    private final List<SubtaskBackPressureInfo> subtasks;

    @JsonCreator
    public JobVertexBackPressureInfo(
            @JsonProperty(FIELD_NAME_STATUS) VertexBackPressureStatus status,
            @JsonProperty(FIELD_NAME_BACKPRESSURE_LEVEL) VertexBackPressureLevel backpressureLevel,
            @JsonProperty(FIELD_NAME_END_TIMESTAMP) Long endTimestamp,
            @JsonProperty(FIELD_NAME_SUBTASKS) List<SubtaskBackPressureInfo> subtasks) {
        this.status = status;
        this.backpressureLevel = backpressureLevel;
        this.endTimestamp = endTimestamp;
        this.subtasks = subtasks;
    }

    public static JobVertexBackPressureInfo deprecated() {
        return DEPRECATED_JOB_VERTEX_BACK_PRESSURE_INFO;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobVertexBackPressureInfo that = (JobVertexBackPressureInfo) o;
        return Objects.equals(status, that.status)
                && Objects.equals(backpressureLevel, that.backpressureLevel)
                && Objects.equals(endTimestamp, that.endTimestamp)
                && Objects.equals(subtasks, that.subtasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, backpressureLevel, endTimestamp, subtasks);
    }

    public VertexBackPressureStatus getStatus() {
        return status;
    }

    @Nullable
    @Hidden
    @JsonProperty(DEPRECATED_FIELD_NAME_BACKPRESSURE_LEVEL)
    public VertexBackPressureLevel getBackpressureLevel() {
        return backpressureLevel;
    }

    @Nullable
    public Long getEndTimestamp() {
        return endTimestamp;
    }

    @Nullable
    public List<SubtaskBackPressureInfo> getSubtasks() {
        return subtasks == null ? null : Collections.unmodifiableList(subtasks);
    }

    // ---------------------------------------------------------------------------------
    // Static helper classes
    // ---------------------------------------------------------------------------------

    /** Nested class to encapsulate the sub tasks back pressure. */
    public static final class SubtaskBackPressureInfo {

        public static final String FIELD_NAME_SUBTASK = "subtask";
        public static final String FIELD_NAME_ATTEMPT_NUMBER = "attempt-number";
        public static final String DEPRECATED_FIELD_NAME_BACKPRESSURE_LEVEL = "backpressure-level";
        public static final String FIELD_NAME_BACKPRESSURE_LEVEL = "backpressureLevel";
        public static final String FIELD_NAME_BACK_PRESSURED_RATIO = "ratio";
        public static final String FIELD_NAME_IDLE_RATIO = "idleRatio";
        public static final String FIELD_NAME_BUSY_RATIO = "busyRatio";
        public static final String FIELD_NAME_OTHER_CONCURRENT_ATTEMPTS =
                "other-concurrent-attempts";

        @JsonProperty(FIELD_NAME_SUBTASK)
        private final int subtask;

        @JsonProperty(FIELD_NAME_ATTEMPT_NUMBER)
        @JsonInclude(Include.NON_NULL)
        @Nullable
        private final Integer attemptNumber;

        @JsonProperty(FIELD_NAME_BACKPRESSURE_LEVEL)
        @JsonAlias(DEPRECATED_FIELD_NAME_BACKPRESSURE_LEVEL)
        private final VertexBackPressureLevel backpressureLevel;

        @JsonProperty(FIELD_NAME_BACK_PRESSURED_RATIO)
        private final double backPressuredRatio;

        @JsonProperty(FIELD_NAME_IDLE_RATIO)
        private final double idleRatio;

        @JsonProperty(FIELD_NAME_BUSY_RATIO)
        private final double busyRatio;

        @JsonProperty(FIELD_NAME_OTHER_CONCURRENT_ATTEMPTS)
        @JsonInclude(Include.NON_EMPTY)
        @Nullable
        private final List<SubtaskBackPressureInfo> otherConcurrentAttempts;

        // otherConcurrentAttempts and attemptNumber are Nullable since Jackson will assign null if
        // the fields are absent while parsing
        public SubtaskBackPressureInfo(
                @JsonProperty(FIELD_NAME_SUBTASK) int subtask,
                @JsonProperty(FIELD_NAME_ATTEMPT_NUMBER) @Nullable Integer attemptNumber,
                @JsonProperty(FIELD_NAME_BACKPRESSURE_LEVEL)
                        VertexBackPressureLevel backpressureLevel,
                @JsonProperty(FIELD_NAME_BACK_PRESSURED_RATIO) double backPressuredRatio,
                @JsonProperty(FIELD_NAME_IDLE_RATIO) double idleRatio,
                @JsonProperty(FIELD_NAME_BUSY_RATIO) double busyRatio,
                @JsonProperty(FIELD_NAME_OTHER_CONCURRENT_ATTEMPTS) @Nullable
                        List<SubtaskBackPressureInfo> otherConcurrentAttempts) {
            this.subtask = subtask;
            this.attemptNumber = attemptNumber;
            this.backpressureLevel = checkNotNull(backpressureLevel);
            this.backPressuredRatio = backPressuredRatio;
            this.idleRatio = idleRatio;
            this.busyRatio = busyRatio;
            this.otherConcurrentAttempts = otherConcurrentAttempts;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SubtaskBackPressureInfo that = (SubtaskBackPressureInfo) o;
            return subtask == that.subtask
                    && Objects.equals(attemptNumber, that.attemptNumber)
                    && backPressuredRatio == that.backPressuredRatio
                    && idleRatio == that.idleRatio
                    && busyRatio == that.busyRatio
                    && Objects.equals(backpressureLevel, that.backpressureLevel)
                    && Objects.equals(otherConcurrentAttempts, that.otherConcurrentAttempts);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    subtask,
                    attemptNumber,
                    backpressureLevel,
                    backPressuredRatio,
                    idleRatio,
                    busyRatio,
                    otherConcurrentAttempts);
        }

        public int getSubtask() {
            return subtask;
        }

        @Hidden
        @JsonProperty(DEPRECATED_FIELD_NAME_BACKPRESSURE_LEVEL)
        public VertexBackPressureLevel getBackpressureLevel() {
            return backpressureLevel;
        }

        public double getBackPressuredRatio() {
            return backPressuredRatio;
        }

        public double getIdleRatio() {
            return idleRatio;
        }

        public double getBusyRatio() {
            return busyRatio;
        }

        @Nullable
        public Integer getAttemptNumber() {
            return attemptNumber;
        }

        @Nullable
        public List<SubtaskBackPressureInfo> getOtherConcurrentAttempts() {
            return otherConcurrentAttempts;
        }
    }

    /** Status of vertex back-pressure. */
    public enum VertexBackPressureStatus {
        DEPRECATED("deprecated"),
        OK("ok");

        private final String status;

        VertexBackPressureStatus(String status) {
            this.status = status;
        }

        @JsonValue
        @Override
        public String toString() {
            return status;
        }
    }

    /** Level of vertex back-pressure. */
    public enum VertexBackPressureLevel {
        OK("ok"),
        LOW("low"),
        HIGH("high");

        private final String level;

        VertexBackPressureLevel(String level) {
            this.level = level;
        }

        @JsonValue
        @Override
        public String toString() {
            return level;
        }
    }
}
