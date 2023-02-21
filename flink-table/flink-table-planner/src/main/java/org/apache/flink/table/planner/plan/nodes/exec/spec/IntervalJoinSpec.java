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

package org.apache.flink.table.planner.plan.nodes.exec.spec;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * IntervalJoinSpec describes how two tables will be joined in interval join.
 *
 * <p>This class corresponds to {@link org.apache.calcite.rel.core.Join} rel node. the join
 * condition is splitted into two part: WindowBounds and JoinSpec: 1. WindowBounds contains the time
 * range condition. 2. JoinSpec contains rest of the join condition except windowBounds.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IntervalJoinSpec {
    public static final String FIELD_NAME_WINDOW_BOUNDS = "windowBounds";
    public static final String FIELD_NAME_JOIN_SPEC = "joinSpec";

    @JsonProperty(FIELD_NAME_WINDOW_BOUNDS)
    private final WindowBounds windowBounds;

    @JsonProperty(FIELD_NAME_JOIN_SPEC)
    private final JoinSpec joinSpec;

    @JsonCreator
    public IntervalJoinSpec(
            @JsonProperty(FIELD_NAME_JOIN_SPEC) JoinSpec joinSpec,
            @JsonProperty(FIELD_NAME_WINDOW_BOUNDS) WindowBounds windowBounds) {
        this.windowBounds = windowBounds;
        this.joinSpec = joinSpec;
    }

    @JsonIgnore
    public WindowBounds getWindowBounds() {
        return windowBounds;
    }

    @JsonIgnore
    public JoinSpec getJoinSpec() {
        return joinSpec;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IntervalJoinSpec that = (IntervalJoinSpec) o;
        return Objects.equals(windowBounds, that.windowBounds)
                && Objects.equals(joinSpec, that.joinSpec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowBounds, joinSpec);
    }

    /** WindowBounds describes the time range condition of a Interval Join. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class WindowBounds {
        public static final String FIELD_NAME_IS_EVENT_TIME = "isEventTime";
        public static final String FIELD_NAME_LEFT_LOWER_BOUND = "leftLowerBound";
        public static final String FIELD_NAME_LEFT_UPPER_BOUND = "leftUpperBound";
        public static final String FIELD_NAME_LEFT_TIME_IDX = "leftTimeIndex";
        public static final String FIELD_NAME_RIGHT_TIME_IDX = "rightTimeIndex";

        @JsonProperty(FIELD_NAME_IS_EVENT_TIME)
        private final boolean isEventTime;

        @JsonProperty(FIELD_NAME_LEFT_LOWER_BOUND)
        private final long leftLowerBound;

        @JsonProperty(FIELD_NAME_LEFT_UPPER_BOUND)
        private final long leftUpperBound;

        @JsonProperty(FIELD_NAME_LEFT_TIME_IDX)
        private final int leftTimeIdx;

        @JsonProperty(FIELD_NAME_RIGHT_TIME_IDX)
        private final int rightTimeIdx;

        @JsonCreator
        public WindowBounds(
                @JsonProperty(FIELD_NAME_IS_EVENT_TIME) boolean isEventTime,
                @JsonProperty(FIELD_NAME_LEFT_LOWER_BOUND) long leftLowerBound,
                @JsonProperty(FIELD_NAME_LEFT_UPPER_BOUND) long leftUpperBound,
                @JsonProperty(FIELD_NAME_LEFT_TIME_IDX) int leftTimeIdx,
                @JsonProperty(FIELD_NAME_RIGHT_TIME_IDX) int rightTimeIdx) {
            this.isEventTime = isEventTime;
            this.leftLowerBound = leftLowerBound;
            this.leftUpperBound = leftUpperBound;
            this.leftTimeIdx = leftTimeIdx;
            this.rightTimeIdx = rightTimeIdx;
        }

        @JsonIgnore
        public boolean isEventTime() {
            return isEventTime;
        }

        @JsonIgnore
        public long getLeftLowerBound() {
            return leftLowerBound;
        }

        @JsonIgnore
        public long getLeftUpperBound() {
            return leftUpperBound;
        }

        @JsonIgnore
        public int getLeftTimeIdx() {
            return leftTimeIdx;
        }

        @JsonIgnore
        public int getRightTimeIdx() {
            return rightTimeIdx;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WindowBounds that = (WindowBounds) o;
            return isEventTime == that.isEventTime
                    && leftLowerBound == that.leftLowerBound
                    && leftUpperBound == that.leftUpperBound
                    && leftTimeIdx == that.leftTimeIdx
                    && rightTimeIdx == that.rightTimeIdx;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    isEventTime, leftLowerBound, leftUpperBound, leftTimeIdx, rightTimeIdx);
        }
    }
}
