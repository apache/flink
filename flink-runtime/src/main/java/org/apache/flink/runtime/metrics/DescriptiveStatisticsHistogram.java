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

package org.apache.flink.runtime.metrics;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.SlidingWindowHistogram;

import java.io.Serializable;

/** Runtime compatibility alias for the shared sliding-window {@link Histogram}. */
public class DescriptiveStatisticsHistogram extends SlidingWindowHistogram implements Serializable {
    private static final long serialVersionUID = 1L;

    public DescriptiveStatisticsHistogram(int windowSize) {
        super(windowSize);
    }

    @Override
    public HistogramStatistics getStatistics() {
        final long[] values = getValuesSnapshot();
        final double[] doubleValues = new double[values.length];
        for (int i = 0; i < values.length; i++) {
            doubleValues[i] = values[i];
        }
        return new DescriptiveStatisticsHistogramStatistics(doubleValues);
    }

    /** Fixed-size array retained for compatibility with histogram statistics constructors. */
    static class CircularDoubleArray implements Serializable {
        private static final long serialVersionUID = 1L;
        private final double[] backingArray;
        private int nextPos = 0;
        private boolean fullSize = false;
        private long elementsSeen = 0;

        CircularDoubleArray(int windowSize) {
            this.backingArray = new double[windowSize];
        }

        synchronized void addValue(double value) {
            backingArray[nextPos] = value;
            ++elementsSeen;
            ++nextPos;
            if (nextPos == backingArray.length) {
                nextPos = 0;
                fullSize = true;
            }
        }

        synchronized double[] toUnsortedArray() {
            final int size = getSize();
            double[] result = new double[size];
            System.arraycopy(backingArray, 0, result, 0, result.length);
            return result;
        }

        private synchronized int getSize() {
            return fullSize ? backingArray.length : nextPos;
        }

        private synchronized long getElementsSeen() {
            return elementsSeen;
        }
    }
}
