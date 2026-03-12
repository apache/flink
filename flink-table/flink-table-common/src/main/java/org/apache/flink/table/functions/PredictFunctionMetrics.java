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

/**
 * Encapsulates the built-in metrics for {@link PredictFunction} and {@link AsyncPredictFunction}.
 *
 * <p>Registered metrics:
 *
 * <ul>
 *   <li>requests: Total number of inference requests
 *   <li>requests_success: Number of successful inference requests
 *   <li>requests_failure: Number of failed inference requests
 *   <li>latency: Histogram of inference latency in milliseconds (optional)
 *   <li>rows_output: Total number of output rows from inference
 * </ul>
 */
@Internal
public class PredictFunctionMetrics {

    private final Counter requests;
    private final Counter requestsSuccess;
    private final Counter requestsFailure;
    private final Histogram latency;
    private final Counter rowsOutput;

    public PredictFunctionMetrics(MetricGroup metricGroup, Histogram latencyHistogram) {
        requests = metricGroup.counter("requests");
        requestsSuccess = metricGroup.counter("requests_success");
        requestsFailure = metricGroup.counter("requests_failure");
        latency = latencyHistogram;
        rowsOutput = metricGroup.counter("rows_output");
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

    public void recordLatency(long startTime) {
        if (latency != null) {
            latency.update(System.currentTimeMillis() - startTime);
        }
    }
}
