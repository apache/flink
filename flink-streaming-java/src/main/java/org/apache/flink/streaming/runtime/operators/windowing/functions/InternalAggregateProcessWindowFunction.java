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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * Internal window function for wrapping a {@link ProcessWindowFunction} that takes an
 * {@code Iterable} and an {@link AggregateFunction}.
 *
 * @param <K> The key type
 * @param <W> The window type
 * @param <T> The type of the input to the AggregateFunction
 * @param <ACC> The type of the AggregateFunction's accumulator
 * @param <V> The type of the AggregateFunction's result, and the input to the WindowFunction
 * @param <R> The result type of the WindowFunction
 */
public final class InternalAggregateProcessWindowFunction<T, ACC, V, R, K, W extends Window>
		extends WrappingFunction<ProcessWindowFunction<V, R, K, W>>
		implements InternalWindowFunction<Iterable<T>, R, K, W> {

	private static final long serialVersionUID = 1L;

	private final AggregateFunction<T, ACC, V> aggFunction;

	private final InternalProcessWindowContext<V, R, K, W> ctx;

	public InternalAggregateProcessWindowFunction(
			AggregateFunction<T, ACC, V> aggFunction,
			ProcessWindowFunction<V, R, K, W> windowFunction) {
		super(windowFunction);
		this.aggFunction = aggFunction;
		this.ctx = new InternalProcessWindowContext<>(windowFunction);
	}

	@Override
	public void process(K key, final W window, final InternalWindowContext context, Iterable<T> input, Collector<R> out) throws Exception {
		ACC acc = aggFunction.createAccumulator();

		for (T val : input) {
			acc = aggFunction.add(val, acc);
		}

		this.ctx.window = window;
		this.ctx.internalContext = context;
		ProcessWindowFunction<V, R, K, W> wrappedFunction = this.wrappedFunction;
		wrappedFunction.process(key, ctx, Collections.singletonList(aggFunction.getResult(acc)), out);
	}

	@Override
	public void clear(final W window, final InternalWindowContext context) throws Exception {
		this.ctx.window = window;
		this.ctx.internalContext = context;
		ProcessWindowFunction<V, R, K, W> wrappedFunction = this.wrappedFunction;
		wrappedFunction.clear(ctx);
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
