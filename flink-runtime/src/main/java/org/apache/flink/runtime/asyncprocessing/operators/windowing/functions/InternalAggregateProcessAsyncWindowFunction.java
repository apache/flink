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

package org.apache.flink.runtime.asyncprocessing.operators.windowing.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.WrappingFunction;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Internal window function for wrapping a {@link ProcessWindowFunction} that takes an {@code
 * Iterable} and an {@link AggregateFunction}. This is for async window operator.
 *
 * @param <K> The key type
 * @param <W> The window type
 * @param <T> The type of the input to the AggregateFunction
 * @param <ACC> The type of the AggregateFunction's accumulator
 * @param <V> The type of the AggregateFunction's result, and the input to the WindowFunction
 * @param <R> The result type of the WindowFunction
 */
public final class InternalAggregateProcessAsyncWindowFunction<T, ACC, V, R, K, W extends Window>
        extends WrappingFunction<ProcessWindowFunction<V, R, K, W>>
        implements InternalAsyncWindowFunction<StateIterator<T>, R, K, W> {

    private static final long serialVersionUID = 1L;

    private final AggregateFunction<T, ACC, V> aggFunction;

    public InternalAggregateProcessAsyncWindowFunction(
            AggregateFunction<T, ACC, V> aggFunction,
            ProcessWindowFunction<V, R, K, W> windowFunction) {
        super(windowFunction);
        this.aggFunction = aggFunction;
    }

    @Override
    public StateFuture<Void> process(
            K key,
            final W window,
            final InternalWindowContext context,
            StateIterator<T> input,
            Collector<R> out)
            throws Exception {
        InternalProcessWindowContext<V, R, K, W> ctx =
                new InternalProcessWindowContext<>(wrappedFunction);
        ctx.window = window;
        ctx.internalContext = context;
        AtomicReference<ACC> finalAcc = new AtomicReference<>(aggFunction.createAccumulator());

        return input.onNext(
                        val -> {
                            finalAcc.set(aggFunction.add(val, finalAcc.get()));
                        })
                .thenAccept(
                        ignore -> {
                            ProcessWindowFunction<V, R, K, W> wrappedFunction =
                                    this.wrappedFunction;
                            wrappedFunction.process(
                                    key,
                                    ctx,
                                    Collections.singletonList(
                                            aggFunction.getResult(finalAcc.get())),
                                    out);
                        });
    }

    @Override
    public StateFuture<Void> clear(final W window, final InternalWindowContext context)
            throws Exception {
        InternalProcessWindowContext<V, R, K, W> ctx =
                new InternalProcessWindowContext<>(wrappedFunction);
        ctx.window = window;
        ctx.internalContext = context;

        ProcessWindowFunction<V, R, K, W> wrappedFunction = this.wrappedFunction;
        wrappedFunction.clear(ctx);
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        throw new RuntimeException("This should never be called.");
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        throw new RuntimeException("This should never be called.");
    }
}
