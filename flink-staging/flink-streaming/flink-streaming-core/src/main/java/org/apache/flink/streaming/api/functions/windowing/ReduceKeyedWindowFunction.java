/**
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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class ReduceKeyedWindowFunction<K, W extends Window, T> extends RichKeyedWindowFunction<T, T, K, W> {
	private static final long serialVersionUID = 1L;

	private final ReduceFunction<T> reduceFunction;

	public ReduceKeyedWindowFunction(ReduceFunction<T> reduceFunction) {
		this.reduceFunction = reduceFunction;
	}

	@Override
	public void setRuntimeContext(RuntimeContext ctx) {
		super.setRuntimeContext(ctx);
		FunctionUtils.setFunctionRuntimeContext(reduceFunction, ctx);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		FunctionUtils.openFunction(reduceFunction, parameters);
	}

	@Override
	public void close() throws Exception {
		super.close();
		FunctionUtils.closeFunction(reduceFunction);
	}

	@Override
	public void evaluate(K k, W window, Iterable<T> values, Collector<T> out) throws Exception {
		T result = null;

		for (T v: values) {
			if (result == null) {
				result = v;
			} else {
				result = reduceFunction.reduce(result, v);
			}
		}

		if (result != null) {
			out.collect(result);
		}
	}
}
