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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;

/**
 * A wrapper class of {@link TableFunction} for synchronous model inference.
 *
 * <p>The output type of this table function is fixed as {@link RowData}.
 *
 * <p>This class provides built-in metrics for monitoring model inference performance, including:
 *
 * <ul>
 *   <li>inference_requests: Total number of inference requests
 *   <li>inference_requests_success: Number of successful inference requests
 *   <li>inference_requests_failure: Number of failed inference requests
 *   <li>inference_latency: Histogram of inference latency in milliseconds
 *   <li>inference_rows_output: Total number of output rows from inference
 * </ul>
 */
@PublicEvolving
public abstract class PredictFunction extends TableFunction<RowData> {

    // ------------------------------------------------------------------------
    // Metrics
    // ------------------------------------------------------------------------

    /** Counter for total inference requests. */
    private transient Counter inferenceRequests;

    /** Counter for successful inference requests. */
    private transient Counter inferenceRequestsSuccess;

    /** Counter for failed inference requests. */
    private transient Counter inferenceRequestsFailure;

    /** Histogram for inference latency in milliseconds. */
    private transient Histogram inferenceLatency;

    /** Counter for total output rows from inference. */
    private transient Counter inferenceRowsOutput;

    // ------------------------------------------------------------------------
    // Lifecycle Methods
    // ------------------------------------------------------------------------

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        initializeMetrics(context);
    }

    /**
     * Initialize metrics for monitoring model inference performance.
     *
     * <p>This method creates a metric group named "model_inference" and registers the following
     * metrics:
     *
     * <ul>
     *   <li>requests: Counter for total inference requests
     *   <li>requests_success: Counter for successful inference requests
     *   <li>requests_failure: Counter for failed inference requests
     *   <li>latency: Histogram for inference latency (milliseconds)
     *   <li>rows_output: Counter for total output rows
     * </ul>
     *
     * <p>Subclasses can override {@link #createLatencyHistogram(MetricGroup)} to provide a custom
     * histogram implementation.
     *
     * @param context The function context
     */
    protected void initializeMetrics(FunctionContext context) {
        MetricGroup metricGroup = context.getMetricGroup().addGroup("model_inference");

        inferenceRequests = metricGroup.counter("requests");
        inferenceRequestsSuccess = metricGroup.counter("requests_success");
        inferenceRequestsFailure = metricGroup.counter("requests_failure");
        inferenceLatency = createLatencyHistogram(metricGroup);
        inferenceRowsOutput = metricGroup.counter("rows_output");
    }

    /**
     * Create a histogram for tracking inference latency. Subclasses can override this method to
     * provide a custom histogram implementation.
     *
     * @param metricGroup The metric group to register the histogram
     * @return A Histogram instance, or null to disable latency tracking
     */
    protected Histogram createLatencyHistogram(MetricGroup metricGroup) {
        // Default: no histogram (return null)
        // Subclasses can override to provide implementation
        return null;
    }

    // ------------------------------------------------------------------------
    // Prediction Methods
    // ------------------------------------------------------------------------

    /**
     * Synchronously predict result based on input row.
     *
     * <p>Subclasses must implement this method to perform the actual inference logic.
     *
     * @param inputRow - A {@link RowData} that wraps input for predict function.
     * @return A collection of predicted results.
     */
    public abstract Collection<RowData> predict(RowData inputRow);

    /**
     * Records the inference latency metric.
     *
     * @param startTime The start time of the inference request in milliseconds
     */
    protected final void recordLatency(long startTime) {
        if (inferenceLatency != null) {
            long latency = System.currentTimeMillis() - startTime;
            inferenceLatency.update(latency);
        }
    }

    // ------------------------------------------------------------------------
    // Table Function Evaluation
    // ------------------------------------------------------------------------

    /** Invoke {@link #predict} with metrics tracking and handle exceptions. */
    public final void eval(Object... args) {
        GenericRowData argsData = GenericRowData.of(args);

        // Increment request counter
        if (inferenceRequests != null) {
            inferenceRequests.inc();
        }

        // Record start time
        long startTime = System.currentTimeMillis();

        try {
            Collection<RowData> results = predict(argsData);

            // Record success metrics
            recordLatency(startTime);
            if (inferenceRequestsSuccess != null) {
                inferenceRequestsSuccess.inc();
            }

            if (results == null) {
                return;
            }

            // Record output rows
            if (inferenceRowsOutput != null) {
                inferenceRowsOutput.inc(results.size());
            }

            results.forEach(this::collect);
        } catch (Exception e) {
            // Record failure metrics
            recordLatency(startTime);
            if (inferenceRequestsFailure != null) {
                inferenceRequestsFailure.inc();
            }

            throw new FlinkRuntimeException(
                    String.format("Failed to execute prediction with input row %s.", argsData), e);
        }
    }
}
