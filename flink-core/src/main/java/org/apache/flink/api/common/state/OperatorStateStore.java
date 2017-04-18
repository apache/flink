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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;
import java.util.Set;

/**
 * This interface contains methods for registering operator state with a managed store.
 */
@PublicEvolving
public interface OperatorStateStore {

	/**
	 * Creates (or restores) a list state. Each state is registered under a unique name.
	 * The provided serializer is used to de/serialize the state in case of checkpointing (snapshot/restore).
	 *
	 * The items in the list are repartitionable by the system in case of changed operator parallelism.
	 *
	 * @param stateDescriptor The descriptor for this state, providing a name and serializer.
	 * @param <S> The generic type of the state
	 *
	 * @return A list for all state partitions.
	 * @throws Exception
	 */
	<S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception;

	/**
	 * Returns a set with the names of all currently registered states.
	 * @return set of names for all registered states.
	 */
	Set<String> getRegisteredStateNames();

	// -------------------------------------------------------------------------------------------
	//  Deprecated methods
	// -------------------------------------------------------------------------------------------

	/**
	 * Creates (or restores) a list state. Each state is registered under a unique name.
	 * The provided serializer is used to de/serialize the state in case of checkpointing (snapshot/restore).
	 *
	 * The items in the list are repartitionable by the system in case of changed operator parallelism.
	 *
	 * @param stateDescriptor The descriptor for this state, providing a name and serializer.
	 * @param <S> The generic type of the state
	 *
	 * @return A list for all state partitions.
	 * @throws Exception
	 *
	 * @deprecated since 1.3.0. This was deprecated as part of a refinement to the function names.
	 *             Please use {@link #getListState(ListStateDescriptor)} instead.
	 */
	@Deprecated
	<S> ListState<S> getOperatorState(ListStateDescriptor<S> stateDescriptor) throws Exception;

	/**
	 * Creates a state of the given name that uses Java serialization to persist the state. The items in the list
	 * are repartitionable by the system in case of changed operator parallelism.
	 * 
	 * <p>This is a simple convenience method. For more flexibility on how state serialization
	 * should happen, use the {@link #getListState(ListStateDescriptor)} method.
	 *
	 * @param stateName The name of state to create
	 * @return A list state using Java serialization to serialize state objects.
	 * @throws Exception
	 *
	 * @deprecated since 1.3.0. Using Java serialization for persisting state is not encouraged.
	 *             Please use {@link #getListState(ListStateDescriptor)} instead.
	 */
	@Deprecated
	<T extends Serializable> ListState<T> getSerializableListState(String stateName) throws Exception;
}
