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

package org.apache.flink.table.runtime.operators.ml;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.operators.AbstractAsyncFunctionRunner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Async function runner for {@link AsyncPredictFunction}, which takes the generated function,
 * instantiates it, and then calls its lifecycle methods.
 */
public class AsyncMLPredictRunner extends AbstractAsyncFunctionRunner<RowData> {

    private final int asyncBufferCapacity;

    /**
     * Buffers {@link ResultFuture} to avoid newInstance cost when processing elements every time.
     * We use {@link BlockingQueue} to make sure the head {@link ResultFuture}s are available.
     */
    private transient BlockingQueue<JoinedRowResultFuture> resultFutureBuffer;

    public AsyncMLPredictRunner(
            GeneratedFunction<AsyncFunction<RowData, RowData>> generatedFetcher,
            int asyncBufferCapacity) {
        super(generatedFetcher);
        this.asyncBufferCapacity = asyncBufferCapacity;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.resultFutureBuffer = new ArrayBlockingQueue<>(asyncBufferCapacity + 1);
        for (int i = 0; i < asyncBufferCapacity + 1; i++) {
            JoinedRowResultFuture rf = new JoinedRowResultFuture(resultFutureBuffer);
            // add will throw exception immediately if the queue is full which should never happen
            resultFutureBuffer.add(rf);
        }
        registerMetric(getRuntimeContext().getMetricGroup());
    }

    @Override
    public void asyncInvoke(RowData input, ResultFuture<RowData> resultFuture) throws Exception {
        try {
            JoinedRowResultFuture buffer = resultFutureBuffer.take();
            buffer.reset(input, resultFuture);
            fetcher.asyncInvoke(input, buffer);
        } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
        }
    }

    private void registerMetric(MetricGroup metricGroup) {
        metricGroup.gauge(
                "ai_queue_length", () -> asyncBufferCapacity + 1 - resultFutureBuffer.size());
        metricGroup.gauge("ai_queue_capacity", () -> asyncBufferCapacity);
        metricGroup.gauge(
                "ai_queue_usage_ratio",
                () ->
                        1.0
                                * (asyncBufferCapacity + 1 - resultFutureBuffer.size())
                                / asyncBufferCapacity);
    }

    private static final class JoinedRowResultFuture implements ResultFuture<RowData> {

        private final BlockingQueue<JoinedRowResultFuture> resultFutureBuffer;

        private ResultFuture<RowData> realOutput;
        private RowData leftRow;

        public JoinedRowResultFuture(BlockingQueue<JoinedRowResultFuture> resultFutureBuffer) {
            this.resultFutureBuffer = resultFutureBuffer;
        }

        public void reset(RowData row, ResultFuture<RowData> realOutput) {
            this.realOutput = realOutput;
            this.leftRow = row;
        }

        @Override
        public void complete(Collection<RowData> result) {
            List<RowData> outRows = new ArrayList<>();
            for (RowData rightRow : result) {
                RowData outRow = new JoinedRowData(leftRow.getRowKind(), leftRow, rightRow);
                outRows.add(outRow);
            }
            realOutput.complete(outRows);

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
