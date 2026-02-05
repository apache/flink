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

package org.apache.flink.table.planner.plan.logical;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.TimeUtils.formatWithHighestUnit;

/** Logical representation of a cumulative window specification. */
@JsonTypeName("CumulativeWindow")
public class CumulativeWindowSpec implements WindowSpec {
    public static final String FIELD_NAME_MAX_SIZE = "maxSize";
    public static final String FIELD_NAME_STEP = "step";
    public static final String FIELD_NAME_OFFSET = "offset";
    public static final String FIELD_NAME_EMIT_ONLY_ON_UPDATE = "emitOnlyOnUpdate";

    @JsonProperty(FIELD_NAME_MAX_SIZE)
    private final Duration maxSize;

    @JsonProperty(FIELD_NAME_STEP)
    private final Duration step;

    @JsonProperty(FIELD_NAME_OFFSET)
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    @Nullable
    private final Duration offset;

    @JsonProperty(FIELD_NAME_EMIT_ONLY_ON_UPDATE)
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    @Nullable
    private final Boolean emitOnlyOnUpdate;

    @JsonCreator
    public CumulativeWindowSpec(
            @JsonProperty(FIELD_NAME_MAX_SIZE) Duration maxSize,
            @JsonProperty(FIELD_NAME_STEP) Duration step,
            @JsonProperty(FIELD_NAME_OFFSET) @Nullable Duration offset,
            @JsonProperty(FIELD_NAME_EMIT_ONLY_ON_UPDATE) @Nullable Boolean emitOnlyOnUpdate) {
        this.maxSize = checkNotNull(maxSize);
        this.step = checkNotNull(step);
        this.offset = offset;
        this.emitOnlyOnUpdate = emitOnlyOnUpdate;
    }

    /**
     * Creates a new CumulativeWindowSpec with the given parameters.
     *
     * <p>This constructor is provided for backward compatibility.
     */
    public CumulativeWindowSpec(Duration maxSize, Duration step, @Nullable Duration offset) {
        this(maxSize, step, offset, null);
    }

    @Override
    public String toSummaryString(String windowing, String[] inputFieldNames) {
        if (offset == null) {
            return String.format(
                    "CUMULATE(%s, max_size=[%s], step=[%s])",
                    windowing, formatWithHighestUnit(maxSize), formatWithHighestUnit(step));
        } else {
            if (emitOnlyOnUpdate != null) {
                return String.format(
                        "CUMULATE(%s, max_size=[%s], step=[%s], offset=[%s], emitOnlyOnUpdate=[%s])",
                        windowing,
                        formatWithHighestUnit(maxSize),
                        formatWithHighestUnit(step),
                        formatWithHighestUnit(offset),
                        emitOnlyOnUpdate);
            }
            return String.format(
                    "CUMULATE(%s, max_size=[%s], step=[%s], offset=[%s])",
                    windowing,
                    formatWithHighestUnit(maxSize),
                    formatWithHighestUnit(step),
                    formatWithHighestUnit(offset));
        }
    }

    public Duration getMaxSize() {
        return maxSize;
    }

    public Duration getStep() {
        return step;
    }

    public Duration getOffset() {
        return offset;
    }

    /**
     * Returns whether this cumulative window only emits results when new data arrives within the
     * step interval.
     *
     * <p>When enabled, no output is produced for step intervals without new data. This is useful
     * for reducing unnecessary outputs in scenarios where data arrives sparsely.
     *
     * @return true if emit-only-on-update mode is enabled, false or null otherwise
     */
    @Nullable
    public Boolean getEmitOnlyOnUpdate() {
        return emitOnlyOnUpdate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CumulativeWindowSpec that = (CumulativeWindowSpec) o;
        return maxSize.equals(that.maxSize)
                && step.equals(that.step)
                && Objects.equals(offset, that.offset)
                && Objects.equals(emitOnlyOnUpdate, that.emitOnlyOnUpdate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(CumulativeWindowSpec.class, maxSize, step, offset, emitOnlyOnUpdate);
    }

    @Override
    public String toString() {
        if (offset == null) {
            return String.format(
                    "CUMULATE(max_size=[%s], step=[%s])",
                    formatWithHighestUnit(maxSize), formatWithHighestUnit(step));
        } else {
            if (emitOnlyOnUpdate != null) {
                return String.format(
                        "CUMULATE(max_size=[%s], step=[%s], offset=[%s], emitOnlyOnUpdate=[%s])",
                        formatWithHighestUnit(maxSize),
                        formatWithHighestUnit(step),
                        formatWithHighestUnit(offset),
                        emitOnlyOnUpdate);
            }
            return String.format(
                    "CUMULATE(max_size=[%s], step=[%s], offset=[%s])",
                    formatWithHighestUnit(maxSize),
                    formatWithHighestUnit(step),
                    formatWithHighestUnit(offset));
        }
    }

    @Override
    public boolean isAlignedWindow() {
        return true;
    }
}
