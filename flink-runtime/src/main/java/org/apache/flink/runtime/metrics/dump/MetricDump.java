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

package org.apache.flink.runtime.metrics.dump;

import org.apache.flink.util.Preconditions;

/** A container for a dumped metric that contains the scope, name and value(s) of the metric. */
public abstract class MetricDump {
    /** Categories to be returned by {@link MetricDump#getCategory()} to avoid instanceof checks. */
    public static final byte METRIC_CATEGORY_COUNTER = 0;

    public static final byte METRIC_CATEGORY_GAUGE = 1;
    public static final byte METRIC_CATEGORY_HISTOGRAM = 2;
    public static final byte METRIC_CATEGORY_METER = 3;

    /** The scope information for the stored metric. */
    public final QueryScopeInfo scopeInfo;
    /** The name of the stored metric. */
    public final String name;

    private MetricDump(QueryScopeInfo scopeInfo, String name) {
        this.scopeInfo = Preconditions.checkNotNull(scopeInfo);
        this.name = Preconditions.checkNotNull(name);
    }

    /**
     * Returns the category for this MetricDump.
     *
     * @return category
     */
    public abstract byte getCategory();

    @Override
    public String toString() {
        return "MetricDump{"
                + "scopeInfo="
                + scopeInfo
                + ", name='"
                + name
                + '\''
                + ", category='"
                + getCategory()
                + '\''
                + '}';
    }

    /** Container for the value of a {@link org.apache.flink.metrics.Counter}. */
    public static class CounterDump extends MetricDump {
        public final long count;

        public CounterDump(QueryScopeInfo scopeInfo, String name, long count) {
            super(scopeInfo, name);
            this.count = count;
        }

        @Override
        public byte getCategory() {
            return METRIC_CATEGORY_COUNTER;
        }
    }

    /** Container for the value of a {@link org.apache.flink.metrics.Gauge} as a string. */
    public static class GaugeDump extends MetricDump {
        public final String value;

        public GaugeDump(QueryScopeInfo scopeInfo, String name, String value) {
            super(scopeInfo, name);
            this.value = Preconditions.checkNotNull(value);
        }

        @Override
        public byte getCategory() {
            return METRIC_CATEGORY_GAUGE;
        }
    }

    /** Container for the values of a {@link org.apache.flink.metrics.Histogram}. */
    public static class HistogramDump extends MetricDump {
        public final long min;
        public final long max;
        public final double mean;
        public final double median;
        public final double stddev;
        public final double p75;
        public final double p90;
        public final double p95;
        public final double p98;
        public final double p99;
        public final double p999;

        public HistogramDump(
                QueryScopeInfo scopeInfo,
                String name,
                long min,
                long max,
                double mean,
                double median,
                double stddev,
                double p75,
                double p90,
                double p95,
                double p98,
                double p99,
                double p999) {

            super(scopeInfo, name);
            this.min = min;
            this.max = max;
            this.mean = mean;
            this.median = median;
            this.stddev = stddev;
            this.p75 = p75;
            this.p90 = p90;
            this.p95 = p95;
            this.p98 = p98;
            this.p99 = p99;
            this.p999 = p999;
        }

        @Override
        public byte getCategory() {
            return METRIC_CATEGORY_HISTOGRAM;
        }
    }

    /** Container for the rate of a {@link org.apache.flink.metrics.Meter}. */
    public static class MeterDump extends MetricDump {
        public final double rate;

        public MeterDump(QueryScopeInfo scopeInfo, String name, double rate) {
            super(scopeInfo, name);
            this.rate = rate;
        }

        @Override
        public byte getCategory() {
            return METRIC_CATEGORY_METER;
        }
    }
}
