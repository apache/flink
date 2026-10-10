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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * Encapsulates the built-in metrics for {@link PredictFunction} and {@link AsyncPredictFunction}.
 *
 * <p>Registered metrics (all counters; latency is optional):
 *
 * <ul>
 *   <li>{@code requests}: Total number of inference requests.
 *   <li>{@code requests_success}: Number of successful inference requests.
 *   <li>{@code requests_failure}: Number of failed inference requests. Together with {@code
 *       requests_success} this sums to {@code requests} regardless of whether the failure was
 *       signalled via a thrown {@link Throwable} or an exceptionally completed future.
 *   <li>{@code latency}: Histogram of inference latency in milliseconds. Only registered when the
 *       owning function returns a non-null {@link Histogram} from {@code createLatencyHistogram}.
 *       The histogram records <b>both</b> successful and failed requests; callers that need to
 *       separate the two should register their own histograms.
 *   <li>{@code rows_output}: Total number of output rows emitted by successful inference requests.
 *       A {@code null} result from a user {@code predict}/{@code asyncPredict} call is treated as a
 *       successful request with zero output rows.
 * </ul>
 */
@Internal
public class PredictFunctionMetrics {

    private final Counter requests;
    private final Counter requestsSuccess;
    private final Counter requestsFailure;
    @Nullable private final Histogram latency;
    private final Counter rowsOutput;

    public PredictFunctionMetrics(MetricGroup metricGroup, @Nullable Histogram latencyHistogram) {
        Preconditions.checkNotNull(metricGroup, "metricGroup must not be null");
        this.requests = metricGroup.counter("requests");
        this.requestsSuccess = metricGroup.counter("requests_success");
        this.requestsFailure = metricGroup.counter("requests_failure");
        this.latency = latencyHistogram;
        this.rowsOutput = metricGroup.counter("rows_output");
    }

    public void incRequests() {
        requests.inc();
    }

    public void incRequestsSuccess() {
        requestsSuccess.inc();
    }

    public void incRequestsFailure() {
        requestsFailure.inc();
    }

    public void incRowsOutput(long count) {
        rowsOutput.inc(count);
    }

    /**
     * Records the elapsed time since {@code startTime} (in milliseconds) into the latency
     * histogram, if one was provided. No-op otherwise.
     */
    public void recordLatency(long startTime) {
        if (latency != null) {
            latency.update(System.currentTimeMillis() - startTime);
        }
    }

    // ------------------------------------------------------------------------
    // Package-private accessors for testing.
    // ------------------------------------------------------------------------

    Counter getRequests() {
        return requests;
    }

    Counter getRequestsSuccess() {
        return requestsSuccess;
    }

    Counter getRequestsFailure() {
        return requestsFailure;
    }

    Counter getRowsOutput() {
        return rowsOutput;
    }

    @Nullable
    Histogram getLatency() {
        return latency;
    }
}
