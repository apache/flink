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

package org.apache.flink.state.api.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.Set;

/**
 * Base abstract class for functions that are evaluated over keyed (grouped) windows using a context
 * for retrieving extra information.
 *
 * @param <IN> The type of the input value.
 * @param <OUT> The type of the output value.
 * @param <KEY> The type of the key.
 * @param <W> The type of {@code Window} that this window function can be applied on.
 */
@PublicEvolving
public abstract class WindowReaderFunction<IN, OUT, KEY, W extends Window> extends AbstractRichFunction {

	private static final long serialVersionUID = 1L;

	/**
	 * Evaluates the window and outputs none or several elements.
	 *
	 * @param key The key for which this window is evaluated.
	 * @param context The context in which the window is being evaluated.
	 * @param elements The elements in the window being evaluated.
	 * @param out A collector for emitting elements.
	 *
	 * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
	 */
	public abstract void readWindow(KEY key, Context<W> context, Iterable<IN> elements, Collector<OUT> out) throws Exception;

	/**
	 * The context holding window metadata.
	 */
	public interface Context<W extends Window> extends java.io.Serializable {
		/**
		 * Returns the window that is being evaluated.
		 */
		W window();

		/**
		 * Retrieves a {@link State} object that can be used to interact with
		 * fault-tolerant state that is scoped to the trigger which corresponds
		 * to the current window.
		 *
		 * @param descriptor The StateDescriptor that contains the name and type of the
		 *                        state that is being accessed.
		 * @param <S>             The type of the state.
		 * @return The partitioned state object.
		 */
		<S extends State> S triggerState(StateDescriptor<S, ?> descriptor);

		/**
		 * State accessor for per-key and per-window state.
		 */
		KeyedStateStore windowState();

		/**
		 * State accessor for per-key global state.
		 */
		KeyedStateStore globalState();

		/**
		 * @return All event time timers registered by a trigger for the current window.
		 */
		Set<Long> registeredEventTimeTimers() throws Exception;

		/**
		 * @return All processing time timers registered by a trigger for the current window.
		 */
		Set<Long> registeredProcessingTimeTimers() throws Exception;

	}
}
