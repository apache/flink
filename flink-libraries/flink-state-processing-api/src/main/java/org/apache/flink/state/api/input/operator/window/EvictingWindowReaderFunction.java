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

package org.apache.flink.state.api.input.operator.window;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.WindowReaderFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * Wrapper for reading state from an evicting window operator.
 * @param <IN> The input type stored in state.
 * @param <R> The aggregated type.
 * @param <OUT> The output type of the reader function.
 * @param <KEY> The key type.
 * @param <W> The window type.
 */
@Internal
public abstract class EvictingWindowReaderFunction<IN, R, OUT, KEY, W extends Window> extends WindowReaderFunction<StreamRecord<IN>, OUT, KEY, W> {

	private final WindowReaderFunction<R, OUT, KEY, W> wrappedFunction;

	protected EvictingWindowReaderFunction(WindowReaderFunction<R, OUT, KEY, W> wrappedFunction) {
		this.wrappedFunction = Preconditions.checkNotNull(wrappedFunction, "Inner reader function cannot be null");
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		FunctionUtils.openFunction(wrappedFunction, parameters);
	}

	@Override
	public void close() throws Exception {
		FunctionUtils.closeFunction(wrappedFunction);
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		super.setRuntimeContext(t);
		FunctionUtils.setFunctionRuntimeContext(wrappedFunction, t);
	}

	@Override
	public void readWindow(KEY key, Context<W> context, Iterable<StreamRecord<IN>> elements, Collector<OUT> out) throws Exception {
		Iterable<R> result = transform(elements);
		wrappedFunction.readWindow(key, context, result, out);
	}

	public abstract Iterable<R> transform(Iterable<StreamRecord<IN>> elements) throws Exception;
}
