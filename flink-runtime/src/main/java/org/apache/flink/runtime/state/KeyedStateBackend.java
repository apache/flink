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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.heap.InternalKeyContext;

/**
 * A keyed state backend provides methods for managing keyed state.
 *
 * @param <K> The key by which state is keyed.
 */
public interface KeyedStateBackend<K> extends InternalKeyContext<K> {

	/**
	 * Sets the current key that is used for partitioned state.
	 * @param newKey The new current key.
	 */
	void setCurrentKey(K newKey);

	/**
	 * Creates or retrieves a keyed state backed by this state backend.
	 *
	 * @param namespaceSerializer The serializer used for the namespace type of the state
	 * @param stateDescriptor The identifier for the state. This contains name and can create a default state value.
	 *    
	 * @param <N> The type of the namespace.
	 * @param <S> The type of the state.
	 *
	 * @return A new key/value state backed by this backend.
	 *
	 * @throws Exception Exceptions may occur during initialization of the state and should be forwarded.
	 */
	<N, S extends State, T> S getOrCreateKeyedState(
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, T> stateDescriptor) throws Exception;

	/**
	 * Creates or retrieves a partitioned state backed by this state backend.
	 * 
	 * TODO: NOTE: This method does a lot of work caching / retrieving states just to update the namespace.
	 *       This method should be removed for the sake of namespaces being lazily fetched from the keyed
	 *       state backend, or being set on the state directly.
	 *
	 * @param stateDescriptor The identifier for the state. This contains name and can create a default state value.
	 *
	 * @param <N> The type of the namespace.
	 * @param <S> The type of the state.
	 *
	 * @return A new key/value state backed by this backend.
	 *
	 * @throws Exception Exceptions may occur during initialization of the state and should be forwarded.
	 */
	<N, S extends State> S getPartitionedState(
			N namespace,
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, ?> stateDescriptor) throws Exception;

	/**
	 * Closes the backend and releases all resources.
	 */
	void dispose();
}
