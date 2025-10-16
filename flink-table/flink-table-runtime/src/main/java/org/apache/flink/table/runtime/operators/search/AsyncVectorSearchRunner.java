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

package org.apache.flink.table.runtime.operators.search;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.AsyncVectorSearchFunction;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.AbstractAsyncFunctionRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Async function runner for {@link AsyncVectorSearchFunction}, which takes the generated function,
 * instantiates it, and then calls its lifecycle methods.
 */
public class AsyncVectorSearchRunner extends AbstractAsyncFunctionRunner<RowData> {

    private static final long serialVersionUID = 1L;

    private final boolean isLeftOuterJoin;
    private final int asyncBufferCapacity;
    private final int searchTableFieldCount;

    /**
     * Buffers {@link ResultFuture} to avoid newInstance cost when processing elements every time.
     * We use {@link BlockingQueue} to make sure the head {@link ResultFuture}s are available.
     */
    private transient BlockingQueue<JoinedRowResultFuture> resultFutureBuffer;

    public AsyncVectorSearchRunner(
            GeneratedFunction<AsyncFunction<RowData, RowData>> generatedFetcher,
            boolean isLeftOuterJoin,
            int asyncBufferCapacity,
            int searchTableFieldCount) {
        super(generatedFetcher);
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.asyncBufferCapacity = asyncBufferCapacity;
        this.searchTableFieldCount = searchTableFieldCount;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.resultFutureBuffer = new ArrayBlockingQueue<>(asyncBufferCapacity + 1);
        // asyncBufferCapacity + 1 as the queue size in order to avoid
        // blocking on the queue when taking a collector.
        for (int i = 0; i < asyncBufferCapacity + 1; i++) {
            resultFutureBuffer.add(
                    new JoinedRowResultFuture(
                            resultFutureBuffer, isLeftOuterJoin, searchTableFieldCount));
        }
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
        JoinedRowResultFuture wrapper = resultFutureBuffer.take();
        wrapper.reset(input, resultFuture);
        fetcher.asyncInvoke(input, wrapper);
    }

    private static final class JoinedRowResultFuture implements ResultFuture<RowData> {

        private final BlockingQueue<JoinedRowResultFuture> resultFutureBuffer;
        private final boolean isLeftOuterJoin;
        private final GenericRowData nullRow;

        private RowData leftRow;
        private ResultFuture<RowData> realOutput;

        private JoinedRowResultFuture(
                BlockingQueue<JoinedRowResultFuture> resultFutureBuffer,
                boolean isLeftOuterJoin,
                int searchTableArity) {
            this.resultFutureBuffer = resultFutureBuffer;
            this.isLeftOuterJoin = isLeftOuterJoin;
            this.nullRow = new GenericRowData(searchTableArity);
        }

        public void reset(RowData leftRow, ResultFuture<RowData> realOutput) {
            this.leftRow = leftRow;
            this.realOutput = realOutput;
        }

        @Override
        public void complete(Collection<RowData> result) {
            if (result == null || result.isEmpty()) {
                if (isLeftOuterJoin) {
                    RowData outRow = new JoinedRowData(leftRow.getRowKind(), leftRow, nullRow);
                    realOutput.complete(Collections.singleton(outRow));
                } else {
                    realOutput.complete(Collections.emptyList());
                }
            } else {
                List<RowData> outRows = new ArrayList<>();
                for (RowData right : result) {
                    RowData outRow = new JoinedRowData(leftRow.getRowKind(), leftRow, right);
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

        @Override
        public void complete(CollectionSupplier<RowData> supplier) {
            throw new UnsupportedOperationException();
        }
    }
}
