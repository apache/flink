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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.TableFunctionResultFuture;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.generated.GeneratedResultFuture;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/** The async join runner to lookup the dimension table. */
public class AsyncLookupJoinRunner extends RichAsyncFunction<RowData, RowData> {
    private static final long serialVersionUID = -6664660022391632480L;

    private final GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher;
    private final DataStructureConverter<RowData, Object> fetcherConverter;
    private final GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture;
    private final boolean isLeftOuterJoin;
    private final int asyncBufferCapacity;

    private transient AsyncFunction<RowData, Object> fetcher;

    protected final RowDataSerializer rightRowSerializer;

    /**
     * Buffers {@link ResultFuture} to avoid newInstance cost when processing elements every time.
     * We use {@link BlockingQueue} to make sure the head {@link ResultFuture}s are available.
     */
    private transient BlockingQueue<JoinedRowResultFuture> resultFutureBuffer;
    /**
     * A Collection contains all ResultFutures in the runner which is used to invoke {@code close()}
     * on every ResultFuture. {@link #resultFutureBuffer} may not contain all the ResultFutures
     * because ResultFutures will be polled from the buffer when processing.
     */
    private transient List<JoinedRowResultFuture> allResultFutures;

    public AsyncLookupJoinRunner(
            GeneratedFunction<AsyncFunction<RowData, Object>> generatedFetcher,
            DataStructureConverter<RowData, Object> fetcherConverter,
            GeneratedResultFuture<TableFunctionResultFuture<RowData>> generatedResultFuture,
            RowDataSerializer rightRowSerializer,
            boolean isLeftOuterJoin,
            int asyncBufferCapacity) {
        this.generatedFetcher = generatedFetcher;
        this.fetcherConverter = fetcherConverter;
        this.generatedResultFuture = generatedResultFuture;
        this.rightRowSerializer = rightRowSerializer;
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.asyncBufferCapacity = asyncBufferCapacity;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
        FunctionUtils.openFunction(fetcher, parameters);

        // try to compile the generated ResultFuture, fail fast if the code is corrupt.
        generatedResultFuture.compile(getRuntimeContext().getUserCodeClassLoader());

        fetcherConverter.open(getRuntimeContext().getUserCodeClassLoader());

        // asyncBufferCapacity + 1 as the queue size in order to avoid
        // blocking on the queue when taking a collector.
        this.resultFutureBuffer = new ArrayBlockingQueue<>(asyncBufferCapacity + 1);
        this.allResultFutures = new ArrayList<>();
        for (int i = 0; i < asyncBufferCapacity + 1; i++) {
            JoinedRowResultFuture rf =
                    new JoinedRowResultFuture(
                            resultFutureBuffer,
                            createFetcherResultFuture(parameters),
                            fetcherConverter,
                            isLeftOuterJoin,
                            rightRowSerializer.getArity());
            // add will throw exception immediately if the queue is full which should never happen
            resultFutureBuffer.add(rf);
            allResultFutures.add(rf);
        }
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
        JoinedRowResultFuture outResultFuture = resultFutureBuffer.take();
        // the input row is copied when object reuse in AsyncWaitOperator
        outResultFuture.reset(input, resultFuture);

        // fetcher has copied the input field when object reuse is enabled
        fetcher.asyncInvoke(input, outResultFuture);
    }

    public TableFunctionResultFuture<RowData> createFetcherResultFuture(Configuration parameters)
            throws Exception {
        TableFunctionResultFuture<RowData> resultFuture =
                generatedResultFuture.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(resultFuture, getRuntimeContext());
        FunctionUtils.openFunction(resultFuture, parameters);
        return resultFuture;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (fetcher != null) {
            FunctionUtils.closeFunction(fetcher);
        }
        if (allResultFutures != null) {
            for (JoinedRowResultFuture rf : allResultFutures) {
                rf.close();
            }
        }
    }

    @VisibleForTesting
    public List<JoinedRowResultFuture> getAllResultFutures() {
        return allResultFutures;
    }

    /**
     * The {@link JoinedRowResultFuture} is used to combine left {@link RowData} and right {@link
     * RowData} into {@link JoinedRowData}.
     *
     * <p>There are 3 phases in this collector.
     *
     * <ol>
     *   <li>accept lookup function return result and convert it into RowData, call it right result
     *   <li>project & filter the right result if there is a calc on the temporal table, see {@link
     *       AsyncLookupJoinWithCalcRunner#createFetcherResultFuture(Configuration)}
     *   <li>filter the result if a join condition exist, see {@link
     *       AsyncLookupJoinRunner#createFetcherResultFuture(Configuration)}
     *   <li>combine left input and the right result into a JoinedRowData, call it join result
     * </ol>
     *
     * <p>TODO: code generate a whole JoinedRowResultFuture in the future
     */
    private static final class JoinedRowResultFuture implements ResultFuture<Object> {

        private final BlockingQueue<JoinedRowResultFuture> resultFutureBuffer;
        private final TableFunctionResultFuture<RowData> joinConditionResultFuture;
        private final DataStructureConverter<RowData, Object> resultConverter;
        private final boolean isLeftOuterJoin;

        private final DelegateResultFuture delegate;
        private final GenericRowData nullRow;

        private RowData leftRow;
        private ResultFuture<RowData> realOutput;

        private JoinedRowResultFuture(
                BlockingQueue<JoinedRowResultFuture> resultFutureBuffer,
                TableFunctionResultFuture<RowData> joinConditionResultFuture,
                DataStructureConverter<RowData, Object> resultConverter,
                boolean isLeftOuterJoin,
                int rightArity) {
            this.resultFutureBuffer = resultFutureBuffer;
            this.joinConditionResultFuture = joinConditionResultFuture;
            this.resultConverter = resultConverter;
            this.isLeftOuterJoin = isLeftOuterJoin;
            this.delegate = new DelegateResultFuture();
            this.nullRow = new GenericRowData(rightArity);
        }

        public void reset(RowData row, ResultFuture<RowData> realOutput) {
            this.realOutput = realOutput;
            this.leftRow = row;
            joinConditionResultFuture.setInput(row);
            joinConditionResultFuture.setResultFuture(delegate);
            delegate.reset();
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public void complete(Collection<Object> result) {
            Collection<RowData> rowDataCollection;
            if (resultConverter.isIdentityConversion()) {
                rowDataCollection = (Collection) result;
            } else {
                rowDataCollection = new ArrayList<>(result.size());
                for (Object element : result) {
                    rowDataCollection.add(resultConverter.toInternal(element));
                }
            }

            // call condition collector first,
            // the filtered result will be routed to the delegateCollector
            try {
                joinConditionResultFuture.complete(rowDataCollection);
            } catch (Throwable t) {
                // we should catch the exception here to let the framework know
                completeExceptionally(t);
                return;
            }

            Collection<RowData> rightRows = delegate.collection;
            if (rightRows == null || rightRows.isEmpty()) {
                if (isLeftOuterJoin) {
                    RowData outRow = new JoinedRowData(leftRow, nullRow);
                    outRow.setRowKind(leftRow.getRowKind());
                    realOutput.complete(Collections.singleton(outRow));
                } else {
                    realOutput.complete(Collections.emptyList());
                }
            } else {
                List<RowData> outRows = new ArrayList<>();
                for (RowData rightRow : rightRows) {
                    RowData outRow = new JoinedRowData(leftRow, rightRow);
                    outRow.setRowKind(leftRow.getRowKind());
                    outRows.add(outRow);
                }
                realOutput.complete(outRows);
            }
            try {
                // put this collector to the queue to avoid this collector is used
                // again before outRows in the collector is not consumed.
                resultFutureBuffer.put(this);
            } catch (InterruptedException e) {
                completeExceptionally(e);
            }
        }

        @Override
        public void completeExceptionally(Throwable error) {
            realOutput.completeExceptionally(error);
        }

        public void close() throws Exception {
            joinConditionResultFuture.close();
        }

        private final class DelegateResultFuture implements ResultFuture<RowData> {

            private Collection<RowData> collection;

            public void reset() {
                this.collection = null;
            }

            @Override
            public void complete(Collection<RowData> result) {
                this.collection = result;
            }

            @Override
            public void completeExceptionally(Throwable error) {
                JoinedRowResultFuture.this.completeExceptionally(error);
            }
        }
    }
}
