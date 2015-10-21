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
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class ReduceWindowFunction<K, W extends Window, T>
		extends WrappingFunction<ReduceFunction<T>>
		implements WindowFunction<T, T, K, W> {
	private static final long serialVersionUID = 1L;

	public ReduceWindowFunction(ReduceFunction<T> reduceFunction) {
		super(reduceFunction);
	}

	@Override
	public void apply(K k, W window, Iterable<T> values, Collector<T> out) throws Exception {
		T result = null;

		for (T v: values) {
			if (result == null) {
				result = v;
			} else {
				result = wrappedFunction.reduce(result, v);
			}
		}

		if (result != null) {
			out.collect(result);
		}
	}
}
