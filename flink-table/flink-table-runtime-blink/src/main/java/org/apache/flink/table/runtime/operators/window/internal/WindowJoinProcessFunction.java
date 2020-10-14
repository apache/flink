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
 * The {@link InternalWindowProcessFunction} for window join should implement this interface.
 * It defines the methods to merge the state namespaces before joining the inputs.
 *
 * @param <W> The type of {@code Window} that assigner assigns.
 */
public interface WindowJoinProcessFunction<K, W extends Window>
		extends InternalWindowProcessFunction<K, W> {

	/**
	 * Prepares the records of the given window before join. The join inputs
	 * are stored in the state and in some scenario the records are scattered
	 * in multiple panes(e.g. the HOP window).
	 *
	 * @param window the window
	 * @return join input records as an Iterable
	 */
	Iterable<RowData> prepareInputsToJoin(W window) throws Exception;
}
