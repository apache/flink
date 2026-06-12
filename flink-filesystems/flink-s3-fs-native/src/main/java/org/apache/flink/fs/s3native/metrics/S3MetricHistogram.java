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

package org.apache.flink.fs.s3native.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * Minimal sliding-window {@link Histogram} used by {@link AwsSdkMetricBridge} for {@code
 * api_call_duration_ms}.
 *
 * <p>Backed by a fixed-size circular buffer holding the most recent {@code windowSize} samples
 * (default {@value #DEFAULT_WINDOW_SIZE}). This bounds memory regardless of request volume and
 * keeps {@link #update(long)} O(1); statistics are computed from a sorted snapshot of the window.
 *
 * <p>flink-s3-fs-native deliberately keeps a minimal dependency footprint and does not depend on
 * flink-runtime, so {@code DescriptiveStatisticsHistogram} is not available; this is a small
 * self-contained equivalent.
 */
@Internal
public class S3MetricHistogram implements Histogram {

    static final int DEFAULT_WINDOW_SIZE = 1024;

    private final long[] window;
    private int size; // number of valid samples currently in the window
    private int next; // next write position
    private long count; // total samples ever recorded

    public S3MetricHistogram() {
        this(DEFAULT_WINDOW_SIZE);
    }

    public S3MetricHistogram(int windowSize) {
        Preconditions.checkArgument(windowSize > 0, "windowSize must be positive");
        this.window = new long[windowSize];
    }

    @Override
    public synchronized void update(long value) {
        window[next] = value;
        next = (next + 1) % window.length;
        if (size < window.length) {
            size++;
        }
        count++;
    }

    @Override
    public synchronized long getCount() {
        return count;
    }

    @Override
    public synchronized HistogramStatistics getStatistics() {
        return new WindowStatistics(Arrays.copyOf(window, size));
    }

    private static final class WindowStatistics extends HistogramStatistics {

        private final long[] sorted;

        WindowStatistics(long[] values) {
            Arrays.sort(values);
            this.sorted = values;
        }

        @Override
        public double getQuantile(double quantile) {
            if (sorted.length == 0) {
                return 0.0;
            }
            double pos = quantile * (sorted.length + 1);
            if (pos < 1) {
                return sorted[0];
            }
            if (pos >= sorted.length) {
                return sorted[sorted.length - 1];
            }
            int lower = (int) pos;
            double frac = pos - lower;
            return sorted[lower - 1] + frac * (sorted[lower] - sorted[lower - 1]);
        }

        @Override
        public long[] getValues() {
            return sorted;
        }

        @Override
        public int size() {
            return sorted.length;
        }

        @Override
        public double getMean() {
            if (sorted.length == 0) {
                return 0.0;
            }
            long sum = 0;
            for (long v : sorted) {
                sum += v;
            }
            return (double) sum / sorted.length;
        }

        @Override
        public double getStdDev() {
            if (sorted.length == 0) {
                return 0.0;
            }
            final double mean = getMean();
            double sumSquares = 0.0;
            for (long v : sorted) {
                final double diff = v - mean;
                sumSquares += diff * diff;
            }
            return Math.sqrt(sumSquares / sorted.length);
        }

        @Override
        public long getMax() {
            return sorted.length == 0 ? 0L : sorted[sorted.length - 1];
        }

        @Override
        public long getMin() {
            return sorted.length == 0 ? 0L : sorted[0];
        }
    }
}
