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

package org.apache.flink.metrics;

/**
 * Histogram statistics represent the current snapshot of elements recorded in the histogram.
 *
 * <p>The histogram statistics allow to calculate values for quantiles, the mean, the standard
 * deviation, the minimum and the maximum.
 */
public abstract class HistogramStatistics {

    /**
     * Returns the value for the given quantile based on the represented histogram statistics.
     *
     * @param quantile Quantile to calculate the value for
     * @return Value for the given quantile
     */
    public abstract double getQuantile(double quantile);

    /**
     * Returns the elements of the statistics' sample.
     *
     * @return Elements of the statistics' sample
     */
    public abstract long[] getValues();

    /**
     * Returns the size of the statistics' sample.
     *
     * @return Size of the statistics' sample
     */
    public abstract int size();

    /**
     * Returns the mean of the histogram values.
     *
     * @return Mean of the histogram values
     */
    public abstract double getMean();

    /**
     * Returns the standard deviation of the distribution reflected by the histogram statistics.
     *
     * @return Standard deviation of histogram distribution
     */
    public abstract double getStdDev();

    /**
     * Returns the maximum value of the histogram.
     *
     * @return Maximum value of the histogram
     */
    public abstract long getMax();

    /**
     * Returns the minimum value of the histogram.
     *
     * @return Minimum value of the histogram
     */
    public abstract long getMin();
}
