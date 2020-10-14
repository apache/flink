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

package org.apache.flink.table.runtime.operators.window.join.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.runtime.operators.window.Window;

/**
 * A {@link WindowedStateView} is a view to the state with window namespaces.
 * It provides some namespace management APIs. The state is used to store input records which are
 * partitioned by the belonging window (or pane if there is overlapping between windows).
 *
 * <p>Before operating on the records, a right window namespace must be set up first
 * using {@code setCurrentNamespace}.
 */
public interface WindowedStateView<W extends Window> {
	/**
	 * Set up the {@code window} as current namespace.
	 */
	void setCurrentNamespace(W window);

	/**
	 * Clear the state of current window records.
	 */
	void clear();

	/**
	 * Merges the namespaces of the underlying state.
	 *
	 * @param target Target window to merge into
	 * @param sources The merged windows
	 */
	void mergeNamespaces(W target, Iterable<W> sources) throws Exception;

	/** Container which takes the variables to create the keyed state. **/
	interface Context {

		/**
		 * Retrieves a {@link State} object that can be used to interact with
		 * fault-tolerant state that is scoped to the key of the current
		 * trigger invocation.
		 *
		 * @param windowSerializer The window serializer
		 * @param stateDescriptor  The StateDescriptor that contains the name and type of the
		 *                         state that is being accessed
		 * @param <S>              The type of the state
		 * @return The partitioned state object.
		 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for
		 * the function (function is not part of a KeyedStream)
		 */
		<S extends State, W extends Window> S getOrCreateKeyedState(
				TypeSerializer<W> windowSerializer,
				StateDescriptor<S, ?> stateDescriptor) throws Exception;
	}
}
