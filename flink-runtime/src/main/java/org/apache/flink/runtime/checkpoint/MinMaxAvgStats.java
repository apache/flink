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

package org.apache.flink.runtime.checkpoint;

import java.io.Serializable;

/** Helper for keeping track of min/max/average summaries. */
public class MinMaxAvgStats implements Serializable {

    private static final long serialVersionUID = 1769601903483446707L;

    /** Current min value. */
    private long min;

    /** Current max value. */
    private long max;

    /** Sum of all added values. */
    private long sum;

    /** Count of added values. */
    private long count;

    MinMaxAvgStats() {}

    private MinMaxAvgStats(long min, long max, long sum, long count) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
    }

    /**
     * Adds the value to the stats if it is >= 0.
     *
     * @param value Value to add for min/max/avg stats..
     */
    void add(long value) {
        if (value >= 0) {
            if (count > 0) {
                min = Math.min(min, value);
                max = Math.max(max, value);
            } else {
                min = value;
                max = value;
            }

            count++;
            sum += value;
        }
    }

    /**
     * Returns a snapshot of the current state.
     *
     * @return A snapshot of the current state.
     */
    MinMaxAvgStats createSnapshot() {
        return new MinMaxAvgStats(min, max, sum, count);
    }

    /**
     * Returns the minimum seen value.
     *
     * @return The current minimum value.
     */
    public long getMinimum() {
        return min;
    }

    /**
     * Returns the maximum seen value.
     *
     * @return The current maximum value.
     */
    public long getMaximum() {
        return max;
    }

    /**
     * Returns the sum of all seen values.
     *
     * @return Sum of all values.
     */
    public long getSum() {
        return sum;
    }

    /**
     * Returns the count of all seen values.
     *
     * @return Count of all values.
     */
    public long getCount() {
        return count;
    }

    /**
     * Calculates the average over all seen values.
     *
     * @return Average over all seen values.
     */
    public long getAverage() {
        if (count == 0) {
            return 0;
        } else {
            return sum / count;
        }
    }
}
