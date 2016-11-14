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

import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Collection;

/**
 * A keyed state backend provides methods for managing keyed state.
 *
 * @param <K> The key by which state is keyed.
 */
public interface KeyedStateBackend<K> {

	/**
	 * Sets the current key that is used for partitioned state.
	 * @param newKey The new current key.
	 */
	void setCurrentKey(K newKey);

	/**
	 * Used by states to access the current key.
	 */
	K getCurrentKey();

	/**
	 * Returns the key-group to which the current key belongs.
	 */
	int getCurrentKeyGroupIndex();

	/**
	 * Returns the number of key-groups aka max parallelism.
	 */
	int getNumberOfKeyGroups();

	/**
	 * Returns the key groups for this backend.
	 */
	KeyGroupsList getKeyGroupRange();

	/**
	 * {@link TypeSerializer} for the state backend key type.
	 */
	TypeSerializer<K> getKeySerializer();

	/**
	 * Creates or retrieves a partitioned state backed by this state backend.
	 *
	 * @param stateDescriptor The identifier for the state. This contains name and can create a default state value.

	 * @param <N> The type of the namespace.
	 * @param <S> The type of the state.
	 *
	 * @return A new key/value state backed by this backend.
	 *
	 * @throws Exception Exceptions may occur during initialization of the state and should be forwarded.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	<N, S extends State> S getPartitionedState(
			N namespace,
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, ?> stateDescriptor) throws Exception;


	@SuppressWarnings("unchecked,rawtypes")
	<N, S extends MergingState<?, ?>> void mergePartitionedStates(
			N target,
			Collection<N> sources,
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, ?> stateDescriptor) throws Exception;

	/**
	 * Closes the backend and releases all resources.
	 */
	void dispose();
}
