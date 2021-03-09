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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * A {@link WindowFunction} that composes an {@link AggregateFunction} and {@link WindowFunction}.
 * Upon invocation, this first applies {@code AggregateFunction} to the input, and then finally the
 * {@code WindowFunction} to the single result element.
 *
 * @param <K> The key type
 * @param <W> The window type
 * @param <T> The type of the input to the AggregateFunction
 * @param <ACC> The type of the AggregateFunction's accumulator
 * @param <V> The type of the AggregateFunction's result, and the input to the WindowFunction
 * @param <R> The result type of the WindowFunction
 */
@Internal
public class AggregateApplyWindowFunction<K, W extends Window, T, ACC, V, R>
        extends WrappingFunction<WindowFunction<V, R, K, W>> implements WindowFunction<T, R, K, W> {

    private static final long serialVersionUID = 1L;

    private final AggregateFunction<T, ACC, V> aggFunction;

    public AggregateApplyWindowFunction(
            AggregateFunction<T, ACC, V> aggFunction, WindowFunction<V, R, K, W> windowFunction) {
        super(windowFunction);
        this.aggFunction = aggFunction;
    }

    @Override
    public void apply(K key, W window, Iterable<T> values, Collector<R> out) throws Exception {
        ACC acc = aggFunction.createAccumulator();

        for (T val : values) {
            acc = aggFunction.add(val, acc);
        }

        wrappedFunction.apply(
                key, window, Collections.singletonList(aggFunction.getResult(acc)), out);
    }
}
