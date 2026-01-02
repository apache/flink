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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class of {@link AsyncTableFunction} for asynchronously looking up rows matching the
 * lookup keys from external systems in batches.
 *
 * <p>This function is designed for AI/ML inference scenarios and other high-latency lookup
 * workloads where batching can significantly improve throughput. Unlike {@link AsyncLookupFunction}
 * which processes one key at a time, this interface allows processing multiple keys together.
 *
 * <p>The output type of this table function is fixed as {@link RowData}.
 *
 * <p>Compared to {@link AsyncLookupFunction}, this interface is particularly beneficial for:
 *
 * <ul>
 *   <li>Machine learning model inference where batching improves GPU utilization
 *   <li>External service calls that support batch APIs
 *   <li>Database queries that can be batched for efficiency
 * </ul>
 *
 * <p>Note: This function is used as the runtime implementation of {@link LookupTableSource}s for
 * performing temporal joins with batch async semantics.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * public class BatchModelInferenceFunction extends AsyncBatchLookupFunction {
 *
 *     @Override
 *     public CompletableFuture<Collection<RowData>> asyncLookupBatch(List<RowData> keyRows) {
 *         return CompletableFuture.supplyAsync(() -> {
 *             // Convert keys to model input format
 *             List<float[]> features = keyRows.stream()
 *                 .map(this::extractFeatures)
 *                 .collect(Collectors.toList());
 *
 *             // Batch inference call
 *             List<float[]> predictions = modelService.batchPredict(features);
 *
 *             // Convert predictions to RowData
 *             return IntStream.range(0, keyRows.size())
 *                 .mapToObj(i -> createResultRow(keyRows.get(i), predictions.get(i)))
 *                 .collect(Collectors.toList());
 *         });
 *     }
 * }
 * }</pre>
 *
 * @see AsyncLookupFunction
 * @see LookupTableSource
 */
@PublicEvolving
public abstract class AsyncBatchLookupFunction extends AsyncTableFunction<RowData> {

    /**
     * Asynchronously lookup rows matching a batch of lookup keys.
     *
     * <p>The implementation should process all keys in the batch and return results for each key.
     * The returned collection contains all matching rows for all keys. The order of results does
     * not need to match the order of input keys.
     *
     * <p>Please note that the returning collection of RowData shouldn't be reused across
     * invocations.
     *
     * @param keyRows A list of {@link RowData} that wraps lookup keys, one per lookup request
     * @return A CompletableFuture containing a collection of all matching rows for all keys
     */
    public abstract CompletableFuture<Collection<RowData>> asyncLookupBatch(List<RowData> keyRows);

    /**
     * Invokes single key lookup by delegating to {@link #asyncLookupBatch} with a single-element
     * list.
     *
     * <p>This provides backward compatibility with the single-key async lookup interface. However,
     * for optimal performance, callers should use {@link #asyncLookupBatch} directly with batched
     * keys.
     *
     * @param keyRow A {@link RowData} that wraps lookup keys
     * @return A CompletableFuture containing all matching rows for the key
     */
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        return asyncLookupBatch(List.of(keyRow));
    }

    /**
     * The eval method bridges the AsyncTableFunction interface to the batch lookup interface.
     *
     * <p>This method is called by the Flink runtime for each lookup request. For batch processing,
     * the runtime should use the batch-aware operator which accumulates keys and calls {@link
     * #asyncLookupBatch} directly.
     */
    public final void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
        RowData keyRow = createKeyRow(keys);
        asyncLookup(keyRow)
                .whenComplete(
                        (result, exception) -> {
                            if (exception != null) {
                                future.completeExceptionally(
                                        new TableException(
                                                String.format(
                                                        "Failed to asynchronously lookup entries with key '%s'",
                                                        keyRow),
                                                exception));
                                return;
                            }
                            future.complete(result);
                        });
    }

    /**
     * Creates a RowData from the given key values.
     *
     * <p>Subclasses can override this method to provide custom key row creation logic.
     *
     * @param keys The key values
     * @return A RowData containing the key values
     */
    protected RowData createKeyRow(Object[] keys) {
        return org.apache.flink.table.data.GenericRowData.of(keys);
    }

    // ==================================================================================
    //  Configuration methods - to be used by the runtime for batching parameters
    // ==================================================================================

    // TODO: Add configuration methods for batch size, timeout, retry in follow-up PRs
    // These will be used by the runtime to configure the AsyncBatchWaitOperator
}
