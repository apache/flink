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

package org.apache.flink.table.runtime.operators.aggregate.async;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.function.ThrowingConsumer;

import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A common base class for {@link KeyedAsyncFunction} that provides the basic functionality to
 * handle async operations. These include handling async invocations and timers. Namely, it provides
 * per-key sequencing of async operations and timers. The subclass should implement the calls
 * asyncInvokeProtected and onTimerProtected to handle sequenced invocations of asyncInvoke and
 * onTimer.
 */
public abstract class KeyedAsyncFunctionCommon<K, IN, OUT>
        implements KeyedAsyncFunction<K, IN, OUT> {

    /** Context for handling async invocations. */
    private transient OpenContext openContext;

    /** Sequencer for handling async invocations. */
    private transient PerKeyCallbackSequencer<Pair<IN, ResultFuture<OUT>>, OpenContext>
            asyncInvokeSequencer;

    /** Request id for sequencing asyncInvoke calls in order. */
    private transient long requestId = 0;

    public void open(OpenContext ctx) throws Exception {
        this.openContext = ctx;
        asyncInvokeSequencer =
                new PerKeyCallbackSequencer<>(
                        (reqId, p, context) -> asyncInvokeProtected(p.getKey(), p.getRight()));
    }

    @Override
    public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        asyncInvokeSequencer.callbackWhenNext(
                openContext, requestId++, Pair.of(input, resultFuture));
    }

    /**
     * This method is called for each asyncInvoke call. The subclass should implement this method to
     * handle an async operation after every asyncInvoke invocation. Implementers should call {@code
     * handleResponseForAsyncInvoke} to handle an async response.
     *
     * @param input The input row.
     * @param resultFuture The future to complete once the async operation is done.
     */
    protected void asyncInvokeProtected(IN input, ResultFuture<OUT> resultFuture)
            throws Exception {}

    /**
     * Handles the response for an async operation done in {@code asyncInvokeProtected}. This method
     * should be called after an async operation is started. It will notify the next waiter in the
     * sequence after completion.
     *
     * @param result The result to run.
     */
    public <T> void handleResponseForAsyncInvoke(Runnable result) throws Exception {
        try {
            result.run();
        } finally {
            asyncInvokeSequencer.notifyNextWaiter(openContext);
        }
    }

    /**
     * Handles the response for an async operation done in {@code asyncInvokeProtected}. This method
     * should be called after an async operation is started. It will notify the next waiter in the
     * sequence after completion.
     *
     * @param future The future to complete once the async operation is done.
     * @param handleError A consumer to handle an error.
     * @param handleSuccess A consumer to handle a successful response.
     */
    public <T> void handleResponseForAsyncInvoke(
            CompletableFuture<T> future,
            Consumer<Throwable> handleError,
            ThrowingConsumer<T, Exception> handleSuccess) {
        RowData currentKey = openContext.currentKey();
        future.whenComplete(
                (result, t) -> {
                    if (t != null) {
                        openContext.runOnMailboxThread(
                                () -> {
                                    openContext.setCurrentKey(currentKey);
                                    handleError.accept(t);
                                    asyncInvokeSequencer.notifyNextWaiter(openContext);
                                });
                        return;
                    }

                    openContext.runOnMailboxThread(
                            () -> {
                                openContext.setCurrentKey(currentKey);
                                try {
                                    handleSuccess.accept(result);
                                } catch (Exception e) {
                                    handleError.accept(e);
                                } finally {
                                    asyncInvokeSequencer.notifyNextWaiter(openContext);
                                }
                            });
                });
    }
}
