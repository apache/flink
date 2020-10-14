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

package org.apache.flink.table.runtime.operators.window.internal;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.Window;

/**
 * The {@link InternalWindowProcessFunction} for window aggregate should implement this interface.
 * It defines the methods to merge the state namespaces.
 *
 * @param <W> The type of {@code Window} that assigner assigns.
 */
public interface WindowAggregateProcessFunction<K, W extends Window>
		extends InternalWindowProcessFunction<K, W> {

	/**
	 * Prepares the accumulator of the given window before emit the final result. The accumulator
	 * is stored in the state or will be created if there is no corresponding accumulator in state.
	 *
	 * @param window the window
	 */
	void prepareAggregateAccumulatorForEmit(W window) throws Exception;

	/**
	 * Information available in an invocation of methods of {@link WindowAggregateProcessFunction}.
	 *
	 * @param <W>
	 */
	interface Context<K, W extends Window>
			extends InternalWindowProcessFunction.Context<K, W> {
		/**
		 * Gets the accumulators of the given window.
		 */
		RowData getWindowAccumulators(W window) throws Exception;

		/**
		 * Sets the accumulators of the given window.
		 */
		void setWindowAccumulators(W window, RowData acc) throws Exception;

		/**
		 * Clear previous agg state (used for retraction) of the given window.
		 */
		void clearPreviousState(W window) throws Exception;
	}
}
