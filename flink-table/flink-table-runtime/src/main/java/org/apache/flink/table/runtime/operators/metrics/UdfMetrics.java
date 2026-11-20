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

import org.apache.flink.shaded.guava33.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava33.com.google.common.cache.CacheBuilder;

import java.util.concurrent.ExecutionException;

/**
 * Per-operator metrics for a single user-defined function, registered under {@code
 * <operator>.udf.<udfName>}: {@code udfProcessingTime} (a latency histogram) and {@code
 * udfExceptionCount} (a counter of exceptions escaping the function).
 *
 * <p>Timing is sampled: only one invocation out of every {@code sampleInterval} is measured, so a
 * hot function does not pay {@code System.nanoTime()} on every record. The sample decision advances
 * a plain {@code int} without synchronization. Call sites that take it on an async completion
 * thread can race on it, which only shifts which invocation is timed. The two registered metrics
 * are safe to touch from any thread: the histogram synchronizes internally and the exception
 * counter is {@link ThreadSafeSimpleCounter}.
 */
public final class UdfMetrics {

    private static final int HISTORY_SIZE = 128;

    // One operator can host several generated classes that call the same function, and each of
    // them registers on the operator's metric group. The group rejects a second metric with the
    // same name, so all of them share the handle of the first registration, keyed by the
    // udf.<udfName> group instance. Weak keys are compared by identity and do not keep the group of
    // a finished task reachable; a restarted task registers on a new group and gets a new handle.
    private static final Cache<MetricGroup, UdfMetrics> HANDLES =
            CacheBuilder.newBuilder().weakKeys().build();

    // Counted in invocations, not in time: one call out of every sampleInterval is timed.
    private final int sampleInterval;
    private final Histogram processingTime;
    private final Counter exceptionCount;

    // Sample counter; a plain int without synchronization, see the class comment.
    private int invocationCount = 0;

    private UdfMetrics(int sampleInterval, Histogram processingTime, Counter exceptionCount) {
        this.sampleInterval = sampleInterval;
        this.processingTime = processingTime;
        this.exceptionCount = exceptionCount;
    }

    /**
     * Registers the {@code udfProcessingTime} histogram and {@code udfExceptionCount} counter under
     * {@code <operatorMetricGroup>.udf.<udfName>}, or returns the handle already registered there,
     * so every caller that registers one function on one operator metric group shares a handle.
     *
     * @param sampleInterval a number of invocations, not a duration: one call out of every {@code
     *     sampleInterval} is timed. Must be {@code >= 1} ({@code 1} measures every invocation).
     */
    public static UdfMetrics register(
            MetricGroup operatorMetricGroup, String udfName, int sampleInterval) {
        Preconditions.checkArgument(
                sampleInterval >= 1,
                "UDF metric sample interval must be >= 1, but was %s.",
                sampleInterval);
        MetricGroup group = operatorMetricGroup.addGroup("udf", udfName);
        try {
            return HANDLES.get(
                    group,
                    () ->
                            new UdfMetrics(
                                    sampleInterval,
                                    group.histogram(
                                            "udfProcessingTime",
                                            new DescriptiveStatisticsHistogram(HISTORY_SIZE)),
                                    group.counter(
                                            "udfExceptionCount", new ThreadSafeSimpleCounter())));
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                    "Could not register the metrics of user-defined function " + udfName + ".",
                    e.getCause());
        }
    }

    /**
     * Returns {@code true} for the one invocation in every {@code sampleInterval} whose processing
     * time should be measured. Must be called once per invocation. Not synchronized: concurrent
     * callers only shift which invocation is timed, see the class comment.
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
