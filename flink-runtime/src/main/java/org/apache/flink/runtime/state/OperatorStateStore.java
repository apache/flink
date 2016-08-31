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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;

import java.util.Set;

/**
 * Interface for a backend that manages partitionable operator state.
 */
public interface OperatorStateStore {

	/**
	 * Creates (or restores) the partitionable state in this backend. Each state is registered under a unique name.
	 * The provided serializer is used to de/serialize the state in case of checkpointing (snapshot/restore).
	 *
	 * @param stateDescriptor The descriptr for this state, providing a name and serializer
	 * @param <S> The generic type of the state
	 * @return A list for all state partitions.
	 * @throws Exception
	 */
	<S> ListState<S> getPartitionableState(ListStateDescriptor<S> stateDescriptor) throws Exception;

	/**
	 * Returns a set with the names of all currently registered states.
	 * @return set of names for all registered states.
	 */
	Set<String> getRegisteredStateNames();
}
