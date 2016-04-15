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

public class GenericKeyGroupKVState<K, T, N, S extends PartitionedState> {

	private final StateDescriptor<S, T> stateDescriptor;

	private final TypeSerializer<N> namespaceSerializer;

	protected S state;

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

		if (currentNamespace != null) {
			state = currentPartitionedStateBackend.getPartitionedState(currentNamespace, namespaceSerializer, stateDescriptor);
		} else {
			// clear state so that we request a new state when calling setNamespace
			state = null;
		}
	}

	public void setNamespace(N namespace) throws Exception {
		this.currentNamespace = Preconditions.checkNotNull(namespace);

		if (state != null) {
			// set the namespace directly at the state object
			((KvState<K, N, S, ?, ?>) state).setCurrentNamespace(currentNamespace);
		} else if (currentPartitionedStateBackend != null) {
			state = currentPartitionedStateBackend.getPartitionedState(currentNamespace, namespaceSerializer, stateDescriptor);
		}
	}
}
