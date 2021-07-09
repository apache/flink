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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.checkpoint.StatsSummary;
import org.apache.flink.runtime.checkpoint.StatsSummarySnapshot;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/** Transfer object for {@link StatsSummary statistics summary}. */
public final class StatsSummaryDto {

    public static final String FIELD_NAME_MINIMUM = "min";

    public static final String FIELD_NAME_MAXIMUM = "max";

    public static final String FIELD_NAME_AVERAGE = "avg";

    @JsonProperty(FIELD_NAME_MINIMUM)
    private final long minimum;

    @JsonProperty(FIELD_NAME_MAXIMUM)
    private final long maximum;

    @JsonProperty(FIELD_NAME_AVERAGE)
    private final long average;

    public static StatsSummaryDto valueOf(StatsSummary s) {
        return valueOf(s.createSnapshot());
    }

    public static StatsSummaryDto valueOf(StatsSummarySnapshot snapshot) {
        return new StatsSummaryDto(
                snapshot.getMinimum(), snapshot.getMaximum(), snapshot.getAverage());
    }

    @JsonCreator
    public StatsSummaryDto(
            @JsonProperty(FIELD_NAME_MINIMUM) long minimum,
            @JsonProperty(FIELD_NAME_MAXIMUM) long maximum,
            @JsonProperty(FIELD_NAME_AVERAGE) long average) {
        this.minimum = minimum;
        this.maximum = maximum;
        this.average = average;
    }

    public long getMinimum() {
        return minimum;
    }

    public long getMaximum() {
        return maximum;
    }

    public long getAverage() {
        return average;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StatsSummaryDto that = (StatsSummaryDto) o;
        return minimum == that.minimum && maximum == that.maximum && average == that.average;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minimum, maximum, average);
    }
}
