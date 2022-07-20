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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.HistogramStatistics;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Immutable snapshot of {@link StatsSummary}. */
@Internal
public class StatsSummarySnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long min;
    private final long max;
    private final long sum;
    private final long count;
    @Nullable private final HistogramStatistics histogram;

    public StatsSummarySnapshot(
            long min, long max, long sum, long count, @Nullable HistogramStatistics histogram) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
        this.histogram = histogram;
    }

    public static StatsSummarySnapshot empty() {
        return new StatsSummarySnapshot(0, 0, 0, 0, null);
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

    /**
     * Returns the value for the given quantile based on the represented histogram statistics or
     * {@link Double#NaN} if the histogram was not built.
     *
     * @param quantile Quantile to calculate the value for
     * @return Value for the given quantile
     */
    public double getQuantile(double quantile) {
        return histogram == null ? Double.NaN : histogram.getQuantile(quantile);
    }
}
