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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Internal interface for functions that are evaluated over keyed (grouped) windows.
 *
 * @param <IN> The type of the input value.
 * @param <OUT> The type of the output value.
 * @param <KEY> The type of the key.
 */
public interface InternalWindowFunction<IN, OUT, KEY, W extends Window> extends Function {
	/**
	 * Evaluates the window and outputs none or several elements.
	 *
	 * @param context The context in which the window is being evaluated.
	 * @param input The elements in the window being evaluated.
	 * @param out A collector for emitting elements.
	 *
	 * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
	 */
	void process(KEY key, W window, InternalWindowContext context, IN input, Collector<OUT> out) throws Exception;

	/**
	 * Deletes any state in the {@code Context} when the Window is purged.
	 *
	 * @param context The context to which the window is being evaluated
	 * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
	 */
	void clear(W window, InternalWindowContext context) throws Exception;

	/**
	 * A context for {@link InternalWindowFunction}, similar to
	 * {@link org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction.Context} but
	 * for internal use.
	 */
	interface InternalWindowContext extends java.io.Serializable {
		long currentProcessingTime();

		long currentWatermark();

		KeyedStateStore windowState();

		KeyedStateStore globalState();

		<X> void output(OutputTag<X> outputTag, X value);
	}
}
