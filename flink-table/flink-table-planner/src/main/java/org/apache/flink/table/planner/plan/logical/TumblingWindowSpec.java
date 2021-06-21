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

/** Logical representation of a tumbling window specification. */
@JsonTypeName("TumblingWindow")
public class TumblingWindowSpec implements WindowSpec {
    public static final String FIELD_NAME_SIZE = "size";
    public static final String FIELD_NAME_OFFSET = "offset";

    @JsonProperty(FIELD_NAME_SIZE)
    private final Duration size;

    @JsonProperty(FIELD_NAME_OFFSET)
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    @Nullable
    private final Duration offset;

    @JsonCreator
    public TumblingWindowSpec(
            @JsonProperty(FIELD_NAME_SIZE) Duration size,
            @JsonProperty(FIELD_NAME_OFFSET) @Nullable Duration offset) {
        this.size = checkNotNull(size);
        this.offset = offset;
    }

    @Override
    public String toSummaryString(String windowing) {
        if (offset == null) {
            return String.format("TUMBLE(%s, size=[%s])", windowing, formatWithHighestUnit(size));
        } else {
            return String.format(
                    "TUMBLE(%s, size=[%s], offset=[%s])",
                    windowing, formatWithHighestUnit(size), formatWithHighestUnit(offset));
        }
    }

    public Duration getSize() {
        return size;
    }

    public Duration getOffset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TumblingWindowSpec that = (TumblingWindowSpec) o;
        return size.equals(that.size) && Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(TumblingWindowSpec.class, size, offset);
    }

    @Override
    public String toString() {
        if (offset == null) {
            return String.format("TUMBLE(size=[%s])", formatWithHighestUnit(size));
        } else {
            return String.format(
                    "TUMBLE(size=[%s], offset=[%s])",
                    formatWithHighestUnit(size), formatWithHighestUnit(offset));
        }
    }
}
