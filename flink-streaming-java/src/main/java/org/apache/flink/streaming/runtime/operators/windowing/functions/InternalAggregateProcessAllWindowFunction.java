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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * Internal window function for wrapping a {@link ProcessAllWindowFunction} that takes an
 * {@code Iterable} and an {@link AggregateFunction}.
 *
 * @param <W> The window type
 * @param <T> The type of the input to the AggregateFunction
 * @param <ACC> The type of the AggregateFunction's accumulator
 * @param <V> The type of the AggregateFunction's result, and the input to the WindowFunction
 * @param <R> The result type of the WindowFunction
 */
public final class InternalAggregateProcessAllWindowFunction<T, ACC, V, R, W extends Window>
		extends WrappingFunction<ProcessAllWindowFunction<V, R, W>>
		implements InternalWindowFunction<Iterable<T>, R, Byte, W> {

	private static final long serialVersionUID = 1L;

	private final AggregateFunction<T, ACC, V> aggFunction;

	public InternalAggregateProcessAllWindowFunction(
			AggregateFunction<T, ACC, V> aggFunction,
			ProcessAllWindowFunction<V, R, W> windowFunction) {
		super(windowFunction);
		this.aggFunction = aggFunction;
	}

	@Override
	public void apply(Byte key, final W window, Iterable<T> input, Collector<R> out) throws Exception {
		ProcessAllWindowFunction<V, R, W> wrappedFunction = this.wrappedFunction;
		ProcessAllWindowFunction<V, R, W>.Context context = wrappedFunction.new Context() {
			@Override
			public W window() {
				return window;
			}
		};

		final ACC acc = aggFunction.createAccumulator();

		for (T val : input) {
			aggFunction.add(val, acc);
		}

		wrappedFunction.process(context, Collections.singletonList(aggFunction.getResult(acc)), out);
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
