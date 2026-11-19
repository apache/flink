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

package org.apache.flink.streaming.api.functions.async;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;

import java.io.Serializable;
import java.util.List;

/**
 * A function to trigger Async I/O operations in batches.
 *
 * <p>For each batch of inputs, an async I/O operation can be triggered via {@link
 * #asyncInvokeBatch}, and once it has been done, the results can be collected by calling {@link
 * ResultFuture#complete}. This is particularly useful for high-latency inference workloads where
 * batching can significantly improve throughput.
 *
 * <p>Unlike {@link AsyncFunction} which processes one element at a time, this interface allows
 * processing multiple elements together, which is beneficial for scenarios like:
 *
 * <ul>
 *   <li>Machine learning model inference where batching improves GPU utilization
 *   <li>External service calls that support batch APIs
 *   <li>Database queries that can be batched for efficiency
 * </ul>
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * public class BatchInferenceFunction implements AsyncBatchFunction<String, String> {
 *
 *   public void asyncInvokeBatch(List<String> inputs, ResultFuture<String> resultFuture) {
 *     // Submit batch inference request
 *     CompletableFuture.supplyAsync(() -> {
 *         List<String> results = modelService.batchInference(inputs);
 *         return results;
 *     }).thenAccept(results -> resultFuture.complete(results));
 *   }
 * }
 * }</pre>
 *
 * @param <IN> The type of the input elements.
 * @param <OUT> The type of the returned elements.
 */
@PublicEvolving
public interface AsyncBatchFunction<IN, OUT> extends Function, Serializable {

    /**
     * Trigger async operation for a batch of stream inputs.
     *
     * <p>The implementation should process all inputs in the batch and complete the result future
     * with all corresponding outputs. The number of outputs does not need to match the number of
     * inputs - it depends on the specific use case.
     *
     * @param inputs a batch of elements coming from upstream tasks
     * @param resultFuture to be completed with the result data for the entire batch
     * @throws Exception in case of a user code error. An exception will make the task fail and
     *     trigger fail-over process.
     */
    void asyncInvokeBatch(List<IN> inputs, ResultFuture<OUT> resultFuture) throws Exception;

    // TODO: Add timeout handling in follow-up PR
    // TODO: Add open/close lifecycle methods in follow-up PR
}
