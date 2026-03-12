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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class of {@link AsyncTableFunction} for asynchronous model inference.
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
     * @param metricGroup The metric group to register the histogram
     * @return A Histogram instance, or null to disable latency tracking
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
     * @return A collection of all predicted results.
     */
    public abstract CompletableFuture<Collection<RowData>> asyncPredict(RowData inputRow);

    /**
     * Wrapper method that tracks metrics around asyncPredict calls.
     *
     * <p>This method automatically tracks inference metrics including request count,
     * success/failure count, latency, and output row count.
     *
     * @param inputRow The input row data
     * @return A CompletableFuture containing the prediction results
     */
    protected final CompletableFuture<Collection<RowData>> asyncPredictWithMetrics(
            RowData inputRow) {
        metrics.incRequests();
        final long startTime = System.currentTimeMillis();

        return asyncPredict(inputRow)
                .whenComplete(
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
        GenericRowData argsData = GenericRowData.of(args);
        asyncPredictWithMetrics(argsData)
                .whenComplete(
                        (result, exception) -> {
                            if (exception != null) {
                                future.completeExceptionally(
                                        new TableException(
                                                String.format(
                                                        "Failed to execute asynchronously prediction with input row %s.",
                                                        argsData),
                                                exception));
                                return;
                            }
                            future.complete(result);
                        });
    }
}
