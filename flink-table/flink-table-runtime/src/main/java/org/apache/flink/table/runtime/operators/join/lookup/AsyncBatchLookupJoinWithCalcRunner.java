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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The async batch join runner with an additional calculate function on the dimension table.
 *
 * <p>This runner extends AsyncBatchLookupJoinRunner to support additional computation (projection,
 * filtering) on the dimension table data after lookup. It combines the benefits of batch processing
 * with the flexibility of post-lookup transformations.
 *
 * <p>Use cases:
 *
 * <ul>
 *   <li>Filtering dimension table results based on conditions
 *   <li>Projecting only required columns from dimension table
 *   <li>Applying transformations to dimension table data
 * </ul>
 */
public class AsyncBatchLookupJoinWithCalcRunner extends AsyncBatchLookupJoinRunner {

    private static final long serialVersionUID = 8758670006385551407L;

    private final GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc;

    public AsyncBatchLookupJoinWithCalcRunner(
            GeneratedFunction<AsyncFunction<List<RowData>, Object>> generatedFetcher,
            DataStructureConverter<RowData, Object> fetcherConverter,
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedCalc,
            GeneratedResultFuture<TableFunctionResultFuture<List<RowData>>> generatedResultFuture,
            RowDataSerializer rightRowSerializer,
            boolean isLeftOuterJoin,
            int asyncBufferCapacity,
            int batchSize,
            long flushIntervalMillis) {
        super(
                generatedFetcher,
                fetcherConverter,
                generatedResultFuture,
                rightRowSerializer,
                isLeftOuterJoin,
                asyncBufferCapacity,
                batchSize,
                flushIntervalMillis);
        this.generatedCalc = generatedCalc;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        // try to compile the generated ResultFuture, fail fast if the code is corrupt.
        generatedCalc.compile(getRuntimeContext().getUserCodeClassLoader());
    }

    @Override
    public TableFunctionResultFuture<List<RowData>> createFetcherResultFuture(
            OpenContext openContext) throws Exception {
        TableFunctionResultFuture<List<RowData>> joinConditionCollector =
                super.createFetcherResultFuture(openContext);
        FlatMapFunction<RowData, RowData> calc =
                generatedCalc.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(calc, getRuntimeContext());
        FunctionUtils.openFunction(calc, openContext);
        return new TemporalTableCalcResultFuture(calc, joinConditionCollector);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    private class TemporalTableCalcResultFuture extends TableFunctionResultFuture<List<RowData>> {

        private static final long serialVersionUID = -6360673852888872924L;

        private final FlatMapFunction<RowData, RowData> calc;
        private final TableFunctionResultFuture<List<RowData>> joinConditionResultFuture;
        private final CalcCollectionCollector calcCollector = new CalcCollectionCollector();

        private TemporalTableCalcResultFuture(
                FlatMapFunction<RowData, RowData> calc,
                TableFunctionResultFuture<List<RowData>> joinConditionResultFuture) {
            this.calc = calc;
            this.joinConditionResultFuture = joinConditionResultFuture;
        }

        @Override
        public void setInput(Object input) {
            joinConditionResultFuture.setInput(input);
            calcCollector.reset();
        }

        @Override
        public void setResultFuture(ResultFuture<?> resultFuture) {
            joinConditionResultFuture.setResultFuture(resultFuture);
        }

        @Override
        public void complete(Collection<List<RowData>> result) {
            if (result == null || result.size() == 0) {
                joinConditionResultFuture.complete(result);
            } else {
                for (List<RowData> row : result) {
                    int prevSize = calcCollector.getSize();
                    try {
                        if (row == null || row.size() == 0) {
                            calcCollector.collect(null);
                        } else {
                            calc.flatMap(row.get(0), calcCollector);
                            if (calcCollector.getSize() <= prevSize) {
                                calcCollector.collect(null);
                            }
                        }
                    } catch (Exception e) {
                        joinConditionResultFuture.completeExceptionally(e);
                    }
                }
                joinConditionResultFuture.complete(calcCollector.collection);
            }
        }

        @Override
        public void complete(CollectionSupplier<List<RowData>> supplier) {
            try {
                Collection<List<RowData>> result = supplier.get();
                complete(result);
            } catch (Throwable t) {
                completeExceptionally(t);
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            joinConditionResultFuture.close();
            FunctionUtils.closeFunction(calc);
        }
    }

    private class CalcCollectionCollector implements Collector<RowData> {

        Collection<List<RowData>> collection;

        public void reset() {
            this.collection = new ArrayList<>();
        }

        @Override
        public void collect(RowData record) {
            if (record != null) {
                this.collection.add(Collections.singletonList(rightRowSerializer.copy(record)));
            } else {
                this.collection.add(Collections.emptyList());
            }
        }

        public int getSize() {
            return collection.size();
        }

        @Override
        public void close() {}
    }
}
