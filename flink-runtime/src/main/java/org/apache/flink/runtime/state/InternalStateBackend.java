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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.util.Disposable;

import java.util.Collection;
import java.util.Map;

/**
 * The class provides access and manage methods to {@link StateStorage}. Each
 * execution instance of an operator will deploy a backend to manage its
 * state storage.
 */
public interface InternalStateBackend extends Snapshotable<SnapshotResult<KeyedStateHandle>, Collection<KeyedStateHandle>>, Disposable {

	/**
	 * Dispose the backend. This method is called when the task completes its
	 * execution.
	 */
	@Override
	void dispose();

	/**
	 * Returns the total number of groups in all subtasks.
	 *
	 * @return The total number of groups in all subtasks.
	 */
	int getNumGroups();

	/**
	 * Returns the key groups for this backend.
	 */
	KeyGroupRange getKeyGroupRange();

	/**
	 * Returns the class loader for the user code in this operator.
	 *
	 * @return The class loader for the user code in this operator.
	 */
	ClassLoader getUserClassLoader();

	/**
	 * Returns all keyed states in this backend.
	 *
	 * @return All keyed states in this backend.
	 */
	Map<String, KeyedState> getKeyedStates();

	/**
	 * Returns all sub-keyed states in this backend.
	 *
	 * @return All sub-keyed states in this backend.
	 */
	Map<String, SubKeyedState> getSubKeyedStates();

	/**
	 * Returns all state storages in this backend.
	 *
	 * @return All state storages in this backend.
	 */
	Map<String, StateStorage> getStateStorages();

	/**
	 * Returns the keyed state with the given descriptor. The state will be
	 * created if it has not been created by the backend.
	 *
	 * @param stateDescriptor The descriptor of the state to be retrieved.
	 * @param <K> Type of the keys in the state.
	 * @param <V> Type of the values in the state.
	 * @param <S> Type of the state to be retrieved.
	 */
	<K, V, S extends KeyedState<K, V>> S getKeyedState(
		KeyedStateDescriptor<K, V, S> stateDescriptor
	) throws Exception ;

	/**
	 * Returns the subkeyed state with the given descriptor. The state will be
	 * created if it has not been created by the backend.
	 *
	 * @param stateDescriptor The descriptor of the state to be retrieved.
	 * @param <K> Type of the keys in the state.
	 * @param <N> Type of the namespaces in the state.
	 * @param <V> Type of the values in the state.
	 */
	<K, N, V, S extends SubKeyedState<K, N, V>> S getSubKeyedState(
		SubKeyedStateDescriptor<K, N, V, S> stateDescriptor
	) throws Exception ;

}
