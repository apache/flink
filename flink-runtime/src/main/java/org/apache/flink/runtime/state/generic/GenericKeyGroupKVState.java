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

package org.apache.flink.runtime.state.generic;

import org.apache.flink.api.common.state.PartitionedState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.runtime.state.PartitionedStateBackend;
import org.apache.flink.util.Preconditions;

/**
 * Key-value state proxy which points to state objects of an underlying
 * {@link PartitionedStateBackend}. The partitioned state backend can be swapped to operate on
 * different state objects.
 *
 * @param <K> Type of the key
 * @param <T> Type of the value
 * @param <N> Type of the namespace
 * @param <S> Type of the partitioned state
 */
public class GenericKeyGroupKVState<K, T, N, S extends PartitionedState> {

	private final StateDescriptor<S, T> stateDescriptor;

	private final TypeSerializer<N> namespaceSerializer;

	// state of the current partitioned state backend and specified by the state descriptor
	protected S state;

	// current partitioned state backend; it's set by the GenericKeyGroupStateBackend based on the
	// current key value
	private PartitionedStateBackend<K> currentPartitionedStateBackend;

	private N currentNamespace;

	public GenericKeyGroupKVState(
		StateDescriptor<S, T> stateDescriptor,
		TypeSerializer<N> namespaceSerializer) {
		this.stateDescriptor = Preconditions.checkNotNull(stateDescriptor);
		this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
	}

	public void setPartitionedStateBackend(PartitionedStateBackend<K> partitionedStateBackend) throws Exception {
		this.currentPartitionedStateBackend = Preconditions.checkNotNull(partitionedStateBackend);
		state = currentPartitionedStateBackend.getPartitionedState(currentNamespace, namespaceSerializer, stateDescriptor);
	}

	public void setNamespace(N namespace) throws Exception {
		this.currentNamespace = namespace;

		if (state != null) {
			// set the namespace directly at the state object
			((KvState<K, N, ?>) state).setCurrentNamespace(currentNamespace);
		} else if (currentPartitionedStateBackend != null) {
			state = currentPartitionedStateBackend.getPartitionedState(currentNamespace, namespaceSerializer, stateDescriptor);
		}
	}
}
