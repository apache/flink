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

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.WrappingFunction;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Internal window function for wrapping a {@link ProcessWindowFunction} that takes an {@code
 * Iterable} when the window state also is an {@code Iterable}. This is for async window operator.
 */
public final class InternalIterableProcessAsyncWindowFunction<IN, OUT, KEY, W extends Window>
        extends WrappingFunction<ProcessWindowFunction<IN, OUT, KEY, W>>
        implements InternalAsyncWindowFunction<StateIterator<IN>, OUT, KEY, W> {

    private static final long serialVersionUID = 1L;

    public InternalIterableProcessAsyncWindowFunction(
            ProcessWindowFunction<IN, OUT, KEY, W> wrappedFunction) {
        super(wrappedFunction);
    }

    @Override
    public StateFuture<Void> process(
            KEY key,
            final W window,
            final InternalWindowContext context,
            StateIterator<IN> input,
            Collector<OUT> out)
            throws Exception {
        InternalProcessWindowContext<IN, OUT, KEY, W> ctx =
                new InternalProcessWindowContext<>(wrappedFunction);
        ctx.window = window;
        ctx.internalContext = context;

        List<IN> data = new ArrayList<>();

        return input.onNext(
                        (item) -> {
                            data.add(item);
                        })
                .thenAccept(
                        ignore -> {
                            ProcessWindowFunction<IN, OUT, KEY, W> wrappedFunction =
                                    this.wrappedFunction;
                            wrappedFunction.process(key, ctx, data, out);
                        });
    }

    @Override
    public StateFuture<Void> clear(final W window, final InternalWindowContext context)
            throws Exception {
        InternalProcessWindowContext<IN, OUT, KEY, W> ctx =
                new InternalProcessWindowContext<>(wrappedFunction);
        ctx.window = window;
        ctx.internalContext = context;
        ProcessWindowFunction<IN, OUT, KEY, W> wrappedFunction = this.wrappedFunction;
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
