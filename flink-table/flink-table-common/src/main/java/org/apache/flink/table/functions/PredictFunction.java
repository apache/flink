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
 *   <li>requests: Total number of inference requests
 *   <li>requests_success: Number of successful inference requests
 *   <li>requests_failure: Number of failed inference requests
 *   <li>latency: Histogram of inference latency in milliseconds
 *   <li>rows_output: Total number of output rows from inference
 * </ul>
 */
@PublicEvolving
public abstract class PredictFunction extends TableFunction<RowData> {

    private transient PredictFunctionMetrics metrics;

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        MetricGroup metricGroup = context.getMetricGroup().addGroup("model_inference");
        metrics = new PredictFunctionMetrics(metricGroup, createLatencyHistogram(metricGroup));
    }

    /**
     * Create a histogram for tracking inference latency. Subclasses can override this method to
     * provide a custom histogram implementation.
     *
     * @param metricGroup The metric group to register the histogram
     * @return A Histogram instance, or null to disable latency tracking
     */
    protected Histogram createLatencyHistogram(MetricGroup metricGroup) {
        return null;
    }

    /**
     * Synchronously predict result based on input row.
     *
     * <p>Subclasses must implement this method to perform the actual inference logic.
     *
     * @param inputRow - A {@link RowData} that wraps input for predict function.
     * @return A collection of predicted results.
     */
    public abstract Collection<RowData> predict(RowData inputRow);

    /** Invoke {@link #predict} with metrics tracking and handle exceptions. */
    public final void eval(Object... args) {
        GenericRowData argsData = GenericRowData.of(args);
        metrics.incRequests();
        long startTime = System.currentTimeMillis();

        try {
            Collection<RowData> results = predict(argsData);
            metrics.recordLatency(startTime);
            metrics.incRequestsSuccess();

            if (results == null) {
                return;
            }
            metrics.incRowsOutput(results.size());
            results.forEach(this::collect);
        } catch (Exception e) {
            metrics.recordLatency(startTime);
            metrics.incRequestsFailure();
            throw new FlinkRuntimeException(
                    String.format("Failed to execute prediction with input row %s.", argsData), e);
        }
    }
}
