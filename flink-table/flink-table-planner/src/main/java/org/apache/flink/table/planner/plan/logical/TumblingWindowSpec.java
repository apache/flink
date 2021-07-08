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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.TimeUtils.formatWithHighestUnit;

/** Logical representation of a tumbling window specification. */
@JsonTypeName("TumblingWindow")
public class TumblingWindowSpec implements WindowSpec {
    public static final String FIELD_NAME_SIZE = "size";

    @JsonProperty(FIELD_NAME_SIZE)
    private final Duration size;

    @JsonCreator
    public TumblingWindowSpec(@JsonProperty(FIELD_NAME_SIZE) Duration size) {
        this.size = checkNotNull(size);
    }

    @Override
    public String toSummaryString(String windowing) {
        return String.format("TUMBLE(%s, size=[%s])", windowing, formatWithHighestUnit(size));
    }

    public Duration getSize() {
        return size;
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
        return size.equals(that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(TumblingWindowSpec.class, size);
    }

    @Override
    public String toString() {
        return String.format("TUMBLE(size=[%s])", formatWithHighestUnit(size));
    }
}
