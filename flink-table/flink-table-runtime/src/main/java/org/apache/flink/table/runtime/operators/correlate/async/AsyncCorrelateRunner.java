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

package org.apache.flink.table.runtime.operators.correlate.async;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Async function runner for {@link org.apache.flink.table.functions.AsyncTableFunction}. It invokes
 * the UDF for each of the input rows, joining the responses with the input.
 */
public class AsyncCorrelateRunner extends RichAsyncFunction<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher;
    private final DataStructureConverter<RowData, Object> fetcherConverter;
    private transient AsyncFunction<RowData, Object> fetcher;

    public AsyncCorrelateRunner(
            GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher,
            DataStructureConverter<RowData, Object> fetcherConverter) {
        this.generatedFetcher = generatedFetcher;
        this.fetcherConverter = fetcherConverter;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        ClassLoader cl = getRuntimeContext().getUserCodeClassLoader();
        this.fetcher = generatedFetcher.newInstance(cl);

        FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
        FunctionUtils.openFunction(fetcher, openContext);

        fetcherConverter.open(cl);
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
        try {
            JoinedRowResultFuture outResultFuture =
                    new JoinedRowResultFuture(input, resultFuture, fetcherConverter);

            fetcher.asyncInvoke(input, outResultFuture);
        } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(fetcher);
    }

    private static final class JoinedRowResultFuture implements ResultFuture<Object> {
        private final DataStructureConverter<RowData, Object> resultConverter;

        private RowData leftRow;
        private ResultFuture<RowData> realOutput;

        private JoinedRowResultFuture(
                RowData row,
                ResultFuture<RowData> resultFuture,
                DataStructureConverter<RowData, Object> resultConverter) {
            this.leftRow = row;
            this.realOutput = resultFuture;
            this.resultConverter = resultConverter;
        }

        @Override
        public void complete(Collection<Object> result) {
            try {
                Collection<RowData> rightRows = wrapPrimitivesAndConvert(result);
                completeResultFuture(rightRows);
            } catch (Throwable t) {
                realOutput.completeExceptionally(t);
            }
        }

        private void completeResultFuture(Collection<RowData> rightRows) {
            realOutput.complete(
                    () -> {
                        if (rightRows == null || rightRows.isEmpty()) {
                            return Collections.emptyList();
                        } else {
                            List<RowData> outRows = new ArrayList<>();
                            for (RowData rightRow : rightRows) {
                                RowData outRow =
                                        new JoinedRowData(leftRow.getRowKind(), leftRow, rightRow);
                                outRows.add(outRow);
                            }
                            return outRows;
                        }
                    });
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private Collection<RowData> wrapPrimitivesAndConvert(Collection<Object> result) {
            Collection<RowData> rowDataCollection;
            if (resultConverter.isIdentityConversion()) {
                rowDataCollection = (Collection) result;
            } else {
                rowDataCollection = new ArrayList<>(result.size());
                for (Object element : result) {
                    if (element instanceof RowData) {
                        rowDataCollection.add((RowData) element);
                    } else {
                        rowDataCollection.add(resultConverter.toInternal(element));
                    }
                }
            }
            return rowDataCollection;
        }

        @Override
        public void completeExceptionally(Throwable error) {
            realOutput.completeExceptionally(error);
        }

        @Override
        public void complete(CollectionSupplier<Object> supplier) {
            throw new UnsupportedOperationException();
        }

        public void close() throws Exception {}
    }
}
