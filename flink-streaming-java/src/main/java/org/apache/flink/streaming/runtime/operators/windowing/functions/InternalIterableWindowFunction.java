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

package org.apache.flink.streaming.runtime.operators.windowing.functions;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * Internal window function for wrapping a {@link WindowFunction} that takes an {@code Iterable}
 * when the window state also is an {@code Iterable}.
 */
public final class InternalIterableWindowFunction<IN, OUT, KEY, W extends Window>
        extends WrappingFunction<WindowFunction<IN, OUT, KEY, W>>
        implements InternalWindowFunction<Iterable<IN>, OUT, KEY, W> {

    private static final long serialVersionUID = 1L;

    public InternalIterableWindowFunction(WindowFunction<IN, OUT, KEY, W> wrappedFunction) {
        super(wrappedFunction);
    }

    @Override
    public void process(
            KEY key,
            W window,
            InternalWindowContext context,
            Iterable<IN> input,
            Collector<OUT> out)
            throws Exception {
        wrappedFunction.apply(key, window, input, out);
    }

    @Override
    public void clear(W window, InternalWindowContext context) throws Exception {}

    @Override
    public RuntimeContext getRuntimeContext() {
        throw new RuntimeException("This should never be called.");
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        throw new RuntimeException("This should never be called.");
    }
}
