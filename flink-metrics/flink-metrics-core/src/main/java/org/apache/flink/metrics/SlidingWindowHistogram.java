/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.util.Arrays;

/** A thread-safe histogram retaining a fixed-size window of the most recent values. */
@Internal
public class SlidingWindowHistogram implements Histogram, Serializable {

    private static final long serialVersionUID = 1L;

    public static final int DEFAULT_WINDOW_SIZE = 1024;

    private final long[] window;
    private int size;
    private int next;
    private long count;

    public SlidingWindowHistogram(int windowSize) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be positive");
        }
        this.window = new long[windowSize];
    }

    @Override
    public synchronized void update(long value) {
        window[next] = value;
        next = (next + 1) % window.length;
        size = Math.min(size + 1, window.length);
        count++;
    }

    @Override
    public synchronized long getCount() {
        return count;
    }

    @Override
    public synchronized HistogramStatistics getStatistics() {
        return new SlidingWindowStatistics(getValuesSnapshot());
    }

    protected synchronized long[] getValuesSnapshot() {
        return Arrays.copyOf(window, size);
    }

    private static final class SlidingWindowStatistics extends HistogramStatistics {

        private final long[] sortedValues;
        private final double mean;
        private final double standardDeviation;

        private SlidingWindowStatistics(long[] values) {
            Arrays.sort(values);
            this.sortedValues = values;
            this.mean = calculateMean(values);
            this.standardDeviation = calculateStandardDeviation(values, mean);
        }

        @Override
        public double getQuantile(double quantile) {
            if (sortedValues.length == 0) {
                return 0.0;
            }
            final double position = quantile * (sortedValues.length + 1);
            if (position < 1) {
                return sortedValues[0];
            }
            if (position >= sortedValues.length) {
                return sortedValues[sortedValues.length - 1];
            }
            final int lower = (int) position;
            final double fraction = position - lower;
            return sortedValues[lower - 1]
                    + fraction * (sortedValues[lower] - sortedValues[lower - 1]);
        }

        @Override
        public long[] getValues() {
            return Arrays.copyOf(sortedValues, sortedValues.length);
        }

        @Override
        public int size() {
            return sortedValues.length;
        }

        @Override
        public double getMean() {
            return mean;
        }

        @Override
        public double getStdDev() {
            return standardDeviation;
        }

        @Override
        public long getMax() {
            return sortedValues.length == 0 ? 0L : sortedValues[sortedValues.length - 1];
        }

        @Override
        public long getMin() {
            return sortedValues.length == 0 ? 0L : sortedValues[0];
        }

        private static double calculateMean(long[] values) {
            if (values.length == 0) {
                return 0.0;
            }
            double sum = 0.0;
            for (long value : values) {
                sum += value;
            }
            return sum / values.length;
        }

        private static double calculateStandardDeviation(long[] values, double mean) {
            if (values.length <= 1) {
                return 0.0;
            }
            double sum = 0.0;
            for (long value : values) {
                final double difference = value - mean;
                sum += difference * difference;
            }
            return Math.sqrt(sum / (values.length - 1));
        }
    }
}
