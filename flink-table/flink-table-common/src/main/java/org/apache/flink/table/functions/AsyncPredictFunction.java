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
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class of {@link AsyncTableFunction} for asynchronous model inference.
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
 * preserved regardless of whether the user {@link #asyncPredict} implementation signals failure by
 * throwing synchronously, returning a {@code null} future, or returning a future that completes
 * exceptionally.
 */
@PublicEvolving
public abstract class AsyncPredictFunction extends AsyncTableFunction<RowData> {

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
     * Asynchronously predict result based on input row.
     *
     * <p>Subclasses must implement this method to perform the actual inference logic.
     *
     * @param inputRow - A {@link RowData} that wraps input for predict function.
     * @return A collection of all predicted results. Returning a future that completes with {@code
     *     null} is permitted and is treated as a successful request with no output rows.
     */
    public abstract CompletableFuture<Collection<RowData>> asyncPredict(RowData inputRow);

    /**
     * Invokes {@link #asyncPredict} with metrics tracking, tolerating both synchronous failures
     * (thrown exceptions) and asynchronous failures (exceptionally completed futures). The returned
     * future is guaranteed to drive exactly one of the {@code requests_success} / {@code
     * requests_failure} counters.
     */
    private CompletableFuture<Collection<RowData>> asyncPredictWithMetrics(RowData inputRow) {
        metrics.incRequests();
        final long startTime = System.currentTimeMillis();

        CompletableFuture<Collection<RowData>> predictFuture;
        try {
            predictFuture = asyncPredict(inputRow);
            if (predictFuture == null) {
                throw new NullPointerException(
                        "asyncPredict(...) returned a null CompletableFuture; "
                                + "implementations must always return a non-null future.");
            }
        } catch (Throwable t) {
            metrics.recordLatency(startTime);
            metrics.incRequestsFailure();
            CompletableFuture<Collection<RowData>> failed = new CompletableFuture<>();
            failed.completeExceptionally(t);
            return failed;
        }

        return predictFuture.whenComplete(
                (result, exception) -> {
                    metrics.recordLatency(startTime);
                    if (exception != null) {
                        metrics.incRequestsFailure();
                    } else {
                        metrics.incRequestsSuccess();
                        if (result != null) {
                            metrics.incRowsOutput(result.size());
                        }
                    }
                });
    }

    /** Invokes {@link #asyncPredict} with metrics tracking and chains futures. */
    public void eval(CompletableFuture<Collection<RowData>> future, Object... args) {
        Preconditions.checkState(
                metrics != null,
                "open(FunctionContext) must be invoked before eval(...). "
                        + "If you override open(), make sure to call super.open(context).");

        GenericRowData argsData = GenericRowData.of(args);
        asyncPredictWithMetrics(argsData)
                .whenComplete(
                        (result, exception) -> {
                            if (exception != null) {
                                // Do not embed the raw input row in the exception message to avoid
                                // leaking potentially sensitive payloads into logs. Emit only the
                                // arity; the original cause is chained for debugging.
                                future.completeExceptionally(
                                        new FlinkRuntimeException(
                                                String.format(
                                                        "Failed to execute asynchronous prediction (input arity=%d).",
                                                        argsData.getArity()),
                                                exception));
                                return;
                            }
                            future.complete(result);
                        });
    }

    @VisibleForTesting
    PredictFunctionMetrics getMetrics() {
        return metrics;
    }

    @VisibleForTesting
    CompletableFuture<Collection<RowData>> asyncPredictWithMetricsForTesting(RowData inputRow) {
        return asyncPredictWithMetrics(inputRow);
    }
}
