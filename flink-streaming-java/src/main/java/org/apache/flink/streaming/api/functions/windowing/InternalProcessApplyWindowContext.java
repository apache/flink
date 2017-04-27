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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * Internal reusable context wrapper.
 *
 * @param <IN> The type of the input value.
 * @param <OUT> The type of the output value.
 * @param <KEY> The type of the window key.
 * @param <W> The type of the window.
 */
@Internal
public class InternalProcessApplyWindowContext<IN, OUT, KEY, W extends Window>
	extends ProcessWindowFunction<IN, OUT, KEY, W>.Context {

	W window;
	ProcessWindowFunction.Context context;

	InternalProcessApplyWindowContext(ProcessWindowFunction<IN, OUT, KEY, W> function) {
		function.super();
	}

	@Override
	public W window() {
		return window;
	}

	@Override
	public long currentProcessingTime() {
		return context.currentProcessingTime();
	}

	@Override
	public long currentWatermark() {
		return context.currentWatermark();
	}

	public KeyedStateStore windowState() {
		return context.windowState();
	}

	@Override
	public KeyedStateStore globalState() {
		return context.globalState();
	}
}
