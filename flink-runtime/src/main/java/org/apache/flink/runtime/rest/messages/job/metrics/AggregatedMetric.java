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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Response type for aggregated metrics. Contains the metric name and optionally the sum, average,
 * minimum and maximum.
 */
public class AggregatedMetric {

    private static final String FIELD_NAME_ID = "id";

    private static final String FIELD_NAME_MIN = "min";

    private static final String FIELD_NAME_MAX = "max";

    private static final String FIELD_NAME_AVG = "avg";

    private static final String FIELD_NAME_SUM = "sum";

    private static final String FIELD_NAME_SKEW = "skew";

    private static final String FIELD_NAME_P50 = "p50";

    private static final String FIELD_NAME_P90 = "p90";

    private static final String FIELD_NAME_P99 = "p99";

    @JsonProperty(value = FIELD_NAME_ID, required = true)
    private final String id;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_MIN)
    private final Double min;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_MAX)
    private final Double max;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_AVG)
    private final Double avg;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_SUM)
    private final Double sum;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_SKEW)
    private final Double skew;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_P50)
    private final Double p50;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_P90)
    private final Double p90;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(FIELD_NAME_P99)
    private final Double p99;

    @JsonCreator
    public AggregatedMetric(
            final @JsonProperty(value = FIELD_NAME_ID, required = true) String id,
            final @Nullable @JsonProperty(FIELD_NAME_MIN) Double min,
            final @Nullable @JsonProperty(FIELD_NAME_MAX) Double max,
            final @Nullable @JsonProperty(FIELD_NAME_AVG) Double avg,
            final @Nullable @JsonProperty(FIELD_NAME_SUM) Double sum,
            final @Nullable @JsonProperty(FIELD_NAME_SKEW) Double skew,
            final @Nullable @JsonProperty(FIELD_NAME_P50) Double p50,
            final @Nullable @JsonProperty(FIELD_NAME_P90) Double p90,
            final @Nullable @JsonProperty(FIELD_NAME_P99) Double p99) {

        this.id = requireNonNull(id, "id must not be null");
        this.min = min;
        this.max = max;
        this.avg = avg;
        this.sum = sum;
        this.skew = skew;
        this.p50 = p50;
        this.p90 = p90;
        this.p99 = p99;
    }

    public AggregatedMetric(final @JsonProperty(value = FIELD_NAME_ID, required = true) String id) {
        this(id, null, null, null, null, null, null, null, null);
    }

    @JsonIgnore
    public String getId() {
        return id;
    }

    @JsonIgnore
    public Double getMin() {
        return min;
    }

    @JsonIgnore
    public Double getMax() {
        return max;
    }

    @JsonIgnore
    public Double getSum() {
        return sum;
    }

    @JsonIgnore
    public Double getAvg() {
        return avg;
    }

    @JsonIgnore
    public Double getSkew() {
        return skew;
    }

    @JsonIgnore
    public Double getP50() {
        return p50;
    }

    @JsonIgnore
    public Double getP90() {
        return p90;
    }

    @JsonIgnore
    public Double getP99() {
        return p99;
    }

    @Override
    public String toString() {
        return "AggregatedMetric{"
                + "id='"
                + id
                + '\''
                + ", min='"
                + min
                + '\''
                + ", max='"
                + max
                + '\''
                + ", avg='"
                + avg
                + '\''
                + ", sum='"
                + sum
                + '\''
                + ", skew='"
                + skew
                + '\''
                + ", p50='"
                + p50
                + '\''
                + ", p90='"
                + p90
                + '\''
                + ", p99='"
                + p99
                + '\''
                + '}';
    }
}
