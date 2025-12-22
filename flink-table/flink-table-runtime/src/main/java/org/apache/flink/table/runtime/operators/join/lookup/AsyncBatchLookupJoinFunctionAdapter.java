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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncBatchFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.FilterCondition;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import java.util.List;

/**
 * An adapter that wraps {@link AsyncBatchLookupJoinRunner} as an {@link AsyncBatchFunction}.
 *
 * <p>This adapter enables the Table API's batch async lookup join to use the streaming
 * {@link org.apache.flink.streaming.api.operators.async.AsyncBatchWaitOperator} for execution.
 *
 * <p>The adapter translates between:
 * <ul>
 *   <li>{@code AsyncBatchFunction<RowData, RowData>} - the streaming interface
 *   <li>{@code AsyncBatchLookupJoinRunner} - the Table API lookup join implementation
 * </ul>
 *
 * @see AsyncBatchLookupJoinRunner
 * @see AsyncBatchFunction
 */
@Internal
public class AsyncBatchLookupJoinFunctionAdapter
        implements AsyncBatchFunction<RowData, RowData>, RichFunction {

    private static final long serialVersionUID = 1L;

    private final AsyncBatchLookupJoinRunner runner;

    private transient RuntimeContext runtimeContext;

    /**
     * Creates an adapter with the given runner.
     *
     * @param runner The batch lookup join runner
     */
    public AsyncBatchLookupJoinFunctionAdapter(AsyncBatchLookupJoinRunner runner) {
        this.runner = runner;
    }

    /**
     * Creates an adapter with generated functions.
     *
     * @param generatedFetcher The generated batch lookup function
     * @param fetcherConverter The data structure converter
     * @param generatedResultFuture The generated result future
     * @param generatedPreFilterCondition The generated pre-filter condition
     * @param rightRowSerializer The right row serializer
     * @param isLeftOuterJoin Whether this is a left outer join
     */
    @SuppressWarnings("unchecked")
    public AsyncBatchLookupJoinFunctionAdapter(
            GeneratedFunction<?> generatedFetcher,
            DataStructureConverter<RowData, Object> fetcherConverter,
            GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture,
            GeneratedFunction<FilterCondition> generatedPreFilterCondition,
            RowDataSerializer rightRowSerializer,
            boolean isLeftOuterJoin) {
        this.runner =
                new AsyncBatchLookupJoinRunner(
                        (GeneratedFunction<
                                        org.apache.flink.table.functions.AsyncBatchLookupFunction>)
                                generatedFetcher,
                        fetcherConverter,
                        generatedResultFuture,
                        generatedPreFilterCondition,
                        rightRowSerializer,
                        isLeftOuterJoin);
    }

    @Override
    public void asyncInvokeBatch(List<RowData> inputs, ResultFuture<RowData> resultFuture)
            throws Exception {
        runner.asyncInvokeBatch(inputs, resultFuture);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ClassLoader userCodeClassLoader = runtimeContext.getUserCodeClassLoader();
        runner.open(DefaultOpenContext.INSTANCE, userCodeClassLoader);
    }

    @Override
    public void close() throws Exception {
        runner.close();
    }

    @Override
    public void setRuntimeContext(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    /**
     * Returns the underlying runner for testing.
     */
    public AsyncBatchLookupJoinRunner getRunner() {
        return runner;
    }
}
