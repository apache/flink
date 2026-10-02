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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * A wrapper class of {@link TableFunction} for synchronous model inference.
 *
 * <p>The output type of this table function is fixed as {@link RowData}.
 *
 * <p>This class provides built-in metrics for monitoring model inference performance under the
 * {@code model_inference} metric group, including:
 *
 * <ul>
 *   <li>{@code requests}: Total number of inference requests.
 *   <li>{@code requests_success}: Number of successful inference requests.
 *   <li>{@code requests_failure}: Number of failed inference requests.
 *   <li>{@code latency}: Histogram of inference latency in milliseconds. <b>Only registered when a
 *       subclass overrides {@link #createLatencyHistogram(MetricGroup)} to return a non-null {@link
 *       Histogram};</b> the default implementation disables latency tracking. The histogram records
 *       both successful and failed requests.
 *   <li>{@code rows_output}: Total number of output rows. A {@code null} result is treated as a
 *       successful request with zero rows.
 * </ul>
 *
 * <p>The request-counting invariant {@code requests == requests_success + requests_failure} is
 * preserved even when the user {@link #predict} implementation throws an {@link Error} (such as
 * {@link OutOfMemoryError}).
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
     * <p>The default implementation returns {@code null}, which disables the {@code latency}
     * metric. Subclasses that want latency tracking should register a histogram on the given {@code
     * metricGroup} (for example, a {@code DropwizardHistogramWrapper}) and return it.
     *
     * @param metricGroup The metric group to register the histogram on.
     * @return A Histogram instance, or {@code null} to disable latency tracking.
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
     * @return A collection of predicted results. Returning {@code null} is permitted and is treated
     *     as a successful request that produced no output.
     */
    public abstract Collection<RowData> predict(RowData inputRow);

    /** Invoke {@link #predict} with metrics tracking and handle exceptions. */
    public final void eval(Object... args) {
        Preconditions.checkState(
                metrics != null,
                "open(FunctionContext) must be invoked before eval(...). "
                        + "If you override open(), make sure to call super.open(context).");

        GenericRowData argsData = GenericRowData.of(args);
        metrics.incRequests();
        long startTime = System.currentTimeMillis();

        Collection<RowData> results;
        try {
            results = predict(argsData);
        } catch (Throwable t) {
            metrics.recordLatency(startTime);
            metrics.incRequestsFailure();
            // Do not embed the raw input row in the exception message to avoid leaking
            // potentially sensitive payloads (PII, binary tensors, etc.) into logs. Emit the
            // arity only; the original cause is still chained for debugging.
            FlinkRuntimeException wrapped =
                    new FlinkRuntimeException(
                            String.format(
                                    "Failed to execute prediction (input arity=%d).",
                                    argsData.getArity()),
                            t);
            if (t instanceof Error) {
                // Surface JVM-level errors without wrapping so callers can observe them as-is.
                throw (Error) t;
            }
            throw wrapped;
        }

        metrics.recordLatency(startTime);
        metrics.incRequestsSuccess();

        if (results == null) {
            return;
        }
        metrics.incRowsOutput(results.size());
        results.forEach(this::collect);
    }

    @VisibleForTesting
    PredictFunctionMetrics getMetrics() {
        return metrics;
    }
}
