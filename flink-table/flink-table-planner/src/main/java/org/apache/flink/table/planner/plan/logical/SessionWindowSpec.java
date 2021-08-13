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

/** Logical representation of a session window specification. */
@JsonTypeName("SessionWindow")
public class SessionWindowSpec implements WindowSpec {
    public static final String FIELD_NAME_GAP = "gap";

    @JsonProperty(FIELD_NAME_GAP)
    private final Duration gap;

    @JsonCreator
    public SessionWindowSpec(@JsonProperty(FIELD_NAME_GAP) Duration gap) {
        this.gap = checkNotNull(gap);
    }

    @Override
    public String toSummaryString(String windowing) {
        return String.format("SESSION(%s, gap=[%s])", windowing, formatWithHighestUnit(gap));
    }

    public Duration getGap() {
        return gap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SessionWindowSpec that = (SessionWindowSpec) o;
        return gap.equals(that.gap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(SessionWindowSpec.class, gap);
    }

    @Override
    public String toString() {
        return String.format("SESSION(gap=[%s])", formatWithHighestUnit(gap));
    }
}
