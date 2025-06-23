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
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Internal window function for wrapping a {@link WindowFunction} that takes an {@code Iterable}
 * when the window state also is an {@code Iterable}. This is for async window operator.
 */
public final class InternalIterableAsyncWindowFunction<IN, OUT, KEY, W extends Window>
        extends WrappingFunction<WindowFunction<IN, OUT, KEY, W>>
        implements InternalAsyncWindowFunction<StateIterator<IN>, OUT, KEY, W> {

    private static final long serialVersionUID = 1L;

    public InternalIterableAsyncWindowFunction(WindowFunction<IN, OUT, KEY, W> wrappedFunction) {
        super(wrappedFunction);
    }

    @Override
    public StateFuture<Void> process(
            KEY key,
            W window,
            InternalWindowContext context,
            StateIterator<IN> input,
            Collector<OUT> out)
            throws Exception {
        List<IN> data = new ArrayList<>();

        if (input.isEmpty()) {
            return StateFutureUtils.completedVoidFuture();
        } else {
            return input.onNext(
                            (item) -> {
                                data.add(item);
                            })
                    .thenAccept(empty -> wrappedFunction.apply(key, window, data, out));
        }
    }

    @Override
    public StateFuture<Void> clear(W window, InternalWindowContext context) throws Exception {
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
