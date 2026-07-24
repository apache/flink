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

package org.apache.flink.table.runtime.operators.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.util.Preconditions;

/**
 * Per-operator metrics for a single user-defined function, registered under {@code
 * <operator>.udf.<udfName>}: {@code udfProcessingTime} (a latency histogram) and {@code
 * udfExceptionCount} (a counter of exceptions escaping the function).
 *
 * <p>Timing is sampled: only one invocation out of every {@code sampleInterval} is measured, so a
 * hot function does not pay {@code System.nanoTime()} on every record. The sample decision advances
 * a plain {@code int} and is only ever taken on the task thread, so it needs no synchronization.
 * The two registered metrics are safe for the async completion thread to touch: the histogram
 * synchronizes internally and the exception counter is {@link ThreadSafeSimpleCounter}.
 */
public final class UdfMetrics {

    private static final int HISTORY_SIZE = 128;

    private final int sampleInterval;
    private final Histogram processingTime;
    private final Counter exceptionCount;

    // Sample counter; only touched on the task thread, hence a plain int with no synchronization.
    private int invocationCount = 0;

    private UdfMetrics(int sampleInterval, Histogram processingTime, Counter exceptionCount) {
        this.sampleInterval = sampleInterval;
        this.processingTime = processingTime;
        this.exceptionCount = exceptionCount;
    }

    /**
     * Registers the {@code udfProcessingTime} histogram and {@code udfExceptionCount} counter under
     * {@code <operatorMetricGroup>.udf.<udfName>}.
     *
     * @param sampleInterval measure one invocation out of every {@code sampleInterval}; must be
     *     {@code >= 1} ({@code 1} measures every invocation)
     */
    public static UdfMetrics register(
            MetricGroup operatorMetricGroup, String udfName, int sampleInterval) {
        Preconditions.checkArgument(
                sampleInterval >= 1,
                "UDF metric sample interval must be >= 1, but was %s.",
                sampleInterval);
        MetricGroup group = operatorMetricGroup.addGroup("udf", udfName);
        return new UdfMetrics(
                sampleInterval,
                group.histogram(
                        "udfProcessingTime", new DescriptiveStatisticsHistogram(HISTORY_SIZE)),
                group.counter("udfExceptionCount", new ThreadSafeSimpleCounter()));
    }

    /**
     * Returns {@code true} for the one invocation in every {@code sampleInterval} whose processing
     * time should be measured. Must be called once per invocation, on the task thread only.
     */
    public boolean shouldSample() {
        if (sampleInterval == 1) {
            return true;
        }
        invocationCount = (invocationCount + 1 < sampleInterval) ? invocationCount + 1 : 0;
        return invocationCount == 1;
    }

    /** Records an elapsed processing time (nanoseconds) for a sampled invocation. */
    public void update(long elapsedNanos) {
        processingTime.update(elapsedNanos);
    }

    /** Counts one exception escaping the user-defined function. */
    public void markException() {
        exceptionCount.inc();
    }
}
