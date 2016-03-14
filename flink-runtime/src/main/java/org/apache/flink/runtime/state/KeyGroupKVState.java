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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.KeyGroupAssigner;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class KeyGroupKVState<K, N, S extends State, SD extends StateDescriptor<S, ?>> implements KvState<K, N, S, SD, KeyGroupStateBackend> {

	private final KeyGroupStateBackend backend;

	private final KeyGroupAssigner<K> keyGroupAssigner;

	private final TypeSerializer<N> namespaceSerializer;

	private final StateDescriptor<S, ?> stateDescriptor;

	private KvState<K, N, S, SD, ?> kvState;

	protected S state;

	private int previousKeyGroupID = -1;

	private N currentNamespace;

	private AbstractStateBackend currentBackend;

	public KeyGroupKVState(
		final KeyGroupStateBackend backend,
		final KeyGroupAssigner<K> keyGroupAssigner,
		final TypeSerializer<N> namespaceSerializer,
		final StateDescriptor<S, ?> stateDescriptor) {
		this.backend = backend;
		this.keyGroupAssigner = keyGroupAssigner;
		this.namespaceSerializer = namespaceSerializer;
		this.stateDescriptor = stateDescriptor;
	}

	@Override
	public void setCurrentKey(K key) {
		int keyGroupID = keyGroupAssigner.getKeyGroupID(key);

		if (keyGroupID != previousKeyGroupID) {
			previousKeyGroupID = keyGroupID;

			currentBackend = backend.getBackend(keyGroupID);

			try {
				state = currentBackend.getPartitionedState(currentNamespace, namespaceSerializer, stateDescriptor);
			} catch (Exception e) {
				throw new RuntimeException("Could not get partitioned state.", e);
			}

			kvState = (KvState<K, N, S, SD, ?>) state;
		}

		kvState.setCurrentKey(key);
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		currentNamespace = namespace;

		if (kvState != null) {
			kvState.setCurrentNamespace(namespace);
		}
	}

	@Override
	public KvStateSnapshot<K, N, S, SD, KeyGroupStateBackend> snapshot(long checkpointId, long timestamp) throws Exception {
		return null;
	}

	@Override
	public void dispose() {
		backend.dispose(stateDescriptor);
	}
}
