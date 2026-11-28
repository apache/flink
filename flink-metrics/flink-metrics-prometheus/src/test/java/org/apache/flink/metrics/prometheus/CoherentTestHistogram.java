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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

/**
 * A histogram whose minimum is below every quantile and every quantile below its maximum.
 *
 * <p>The shared {@link org.apache.flink.metrics.util.TestHistogram} returns an ordinal value per
 * accessor, so its minimum is 7 and its maximum 6.
 */
class CoherentTestHistogram implements Histogram {

    private static final long COUNT = 42;
    private static final long MIN = 1;
    private static final long MAX = 98;

    static double expectedQuantile(double quantile) {
        return 2 + quantile * 95;
    }

    static long expectedCount() {
        return COUNT;
    }

    static long expectedMin() {
        return MIN;
    }

    static long expectedMax() {
        return MAX;
    }

    @Override
    public void update(long value) {}

    @Override
    public long getCount() {
        return COUNT;
    }

    @Override
    public HistogramStatistics getStatistics() {
        return new HistogramStatistics() {
            @Override
            public double getQuantile(double quantile) {
                return expectedQuantile(quantile);
            }

            @Override
            public long[] getValues() {
                return new long[0];
            }

            @Override
            public int size() {
                return (int) COUNT;
            }

            @Override
            public double getMean() {
                return (MIN + MAX) / 2.0;
            }

            @Override
            public double getStdDev() {
                return 1;
            }

            @Override
            public long getMax() {
                return MAX;
            }

            @Override
            public long getMin() {
                return MIN;
            }
        };
    }
}
