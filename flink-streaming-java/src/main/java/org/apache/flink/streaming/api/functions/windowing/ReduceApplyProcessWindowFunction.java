/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * Internal {@link ProcessWindowFunction} that is used for implementing a fold on a window
 * configuration that only allows {@link AllWindowFunction} and cannot directly execute a {@link
 * ReduceFunction}.
 */
@Internal
public class ReduceApplyProcessWindowFunction<K, W extends Window, T, R>
        extends ProcessWindowFunction<T, R, K, W> {

    private static final long serialVersionUID = 1L;

    private final ReduceFunction<T> reduceFunction;
    private final ProcessWindowFunction<T, R, K, W> windowFunction;
    private transient InternalProcessApplyWindowContext<T, R, K, W> ctx;

    public ReduceApplyProcessWindowFunction(
            ReduceFunction<T> reduceFunction, ProcessWindowFunction<T, R, K, W> windowFunction) {
        this.windowFunction = windowFunction;
        this.reduceFunction = reduceFunction;
    }

    @Override
    public void process(K k, final Context context, Iterable<T> input, Collector<R> out)
            throws Exception {

        T curr = null;
        for (T val : input) {
            if (curr == null) {
                curr = val;
            } else {
                curr = reduceFunction.reduce(curr, val);
            }
        }

        this.ctx.window = context.window();
        this.ctx.context = context;
        windowFunction.process(k, ctx, Collections.singletonList(curr), out);
    }

    @Override
    public void clear(final Context context) throws Exception {
        this.ctx.window = context.window();
        this.ctx.context = context;
        windowFunction.clear(ctx);
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        FunctionUtils.openFunction(this.windowFunction, configuration);
        ctx = new InternalProcessApplyWindowContext<>(windowFunction);
    }

    @Override
    public void close() throws Exception {
        FunctionUtils.closeFunction(this.windowFunction);
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);

        FunctionUtils.setFunctionRuntimeContext(this.windowFunction, t);
    }
}
