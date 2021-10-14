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

    public static final String FIELD_NAME_P50 = "p50";

    public static final String FIELD_NAME_P90 = "p90";

    public static final String FIELD_NAME_P95 = "p95";

    public static final String FIELD_NAME_P99 = "p99";

    public static final String FIELD_NAME_P999 = "p999";

    @JsonProperty(FIELD_NAME_MINIMUM)
    private final long minimum;

    @JsonProperty(FIELD_NAME_MAXIMUM)
    private final long maximum;

    @JsonProperty(FIELD_NAME_AVERAGE)
    private final long average;

    @JsonProperty(FIELD_NAME_P50)
    private final double p50;

    @JsonProperty(FIELD_NAME_P90)
    private final double p90;

    @JsonProperty(FIELD_NAME_P95)
    private final double p95;

    @JsonProperty(FIELD_NAME_P99)
    private final double p99;

    @JsonProperty(FIELD_NAME_P999)
    private final double p999;

    public static StatsSummaryDto valueOf(StatsSummary s) {
        return valueOf(s.createSnapshot());
    }

    public static StatsSummaryDto valueOf(StatsSummarySnapshot snapshot) {
        return new StatsSummaryDto(
                snapshot.getMinimum(),
                snapshot.getMaximum(),
                snapshot.getAverage(),
                snapshot.getQuantile(.50d),
                snapshot.getQuantile(.90d),
                snapshot.getQuantile(.95d),
                snapshot.getQuantile(.99d),
                snapshot.getQuantile(.999d));
    }

    @JsonCreator
    public StatsSummaryDto(
            @JsonProperty(FIELD_NAME_MINIMUM) long minimum,
            @JsonProperty(FIELD_NAME_MAXIMUM) long maximum,
            @JsonProperty(FIELD_NAME_AVERAGE) long average,
            @JsonProperty(FIELD_NAME_P50) double p50,
            @JsonProperty(FIELD_NAME_P90) double p90,
            @JsonProperty(FIELD_NAME_P95) double p95,
            @JsonProperty(FIELD_NAME_P99) double p99,
            @JsonProperty(FIELD_NAME_P999) double p999) {
        this.minimum = minimum;
        this.maximum = maximum;
        this.average = average;
        this.p50 = p50;
        this.p90 = p90;
        this.p95 = p95;
        this.p99 = p99;
        this.p999 = p999;
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

    public double getP50() {
        return p50;
    }

    public double getP90() {
        return p90;
    }

    public double getP95() {
        return p95;
    }

    public double getP99() {
        return p99;
    }

    public double getP999() {
        return p999;
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
        return minimum == that.minimum
                && maximum == that.maximum
                && average == that.average
                && p50 == that.p50
                && p90 == that.p90
                && p95 == that.p95
                && p99 == that.p99
                && p999 == that.p999;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minimum, maximum, average, p50, p90, p95, p99, p999);
    }
}
