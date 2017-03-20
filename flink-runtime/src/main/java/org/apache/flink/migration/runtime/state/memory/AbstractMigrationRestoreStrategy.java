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

package org.apache.flink.migration.runtime.state.memory;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * This class outlines the general strategy to restore from migration states.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 *
 * @deprecated Internal class for savepoint backwards compatibility. Don't use for other purposes.
 */
@Deprecated
public abstract class AbstractMigrationRestoreStrategy<K, N, S> implements MigrationRestoreSnapshot<K, N, S> {

	/**
	 * Key Serializer
	 */
	protected final TypeSerializer<K> keySerializer;

	/**
	 * Namespace Serializer
	 */
	protected final TypeSerializer<N> namespaceSerializer;

	/**
	 * Serializer for the state value
	 */
	protected final TypeSerializer<S> stateSerializer;

	public AbstractMigrationRestoreStrategy(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializer<S> stateSerializer) {

		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
		this.stateSerializer = Preconditions.checkNotNull(stateSerializer);
	}

	@Override
	public StateTable<K, N, S> deserialize(String stateName, HeapKeyedStateBackend<K> stateBackend) throws IOException {

		Preconditions.checkNotNull(stateName, "State name is null. Cannot deserialize snapshot.");
		Preconditions.checkNotNull(stateBackend, "State backend is null. Cannot deserialize snapshot.");

		final KeyGroupRange keyGroupRange = stateBackend.getKeyGroupRange();
		Preconditions.checkState(1 == keyGroupRange.getNumberOfKeyGroups(),
				"Unexpected number of key-groups for restoring from Flink 1.1");

		TypeSerializer<N> patchedNamespaceSerializer = this.namespaceSerializer;

		if (patchedNamespaceSerializer instanceof VoidSerializer) {
			patchedNamespaceSerializer = (TypeSerializer<N>) VoidNamespaceSerializer.INSTANCE;
		}

		RegisteredBackendStateMetaInfo<N, S> registeredBackendStateMetaInfo =
				new RegisteredBackendStateMetaInfo<>(
						StateDescriptor.Type.UNKNOWN,
						stateName,
						patchedNamespaceSerializer,
						stateSerializer);

		final StateTable<K, N, S> stateTable = stateBackend.newStateTable(registeredBackendStateMetaInfo);
		final DataInputView inView = openDataInputView();
		final int keyGroup = keyGroupRange.getStartKeyGroup();
		final int numNamespaces = inView.readInt();

		for (int i = 0; i < numNamespaces; i++) {
			N namespace = namespaceSerializer.deserialize(inView);
			if (null == namespace) {
				namespace = (N) VoidNamespace.INSTANCE;
			}
			final int numKV = inView.readInt();
			for (int j = 0; j < numKV; j++) {
				K key = keySerializer.deserialize(inView);
				S value = stateSerializer.deserialize(inView);
				stateTable.put(key, keyGroup, namespace, value);
			}
		}
		return stateTable;
	}

	/**
	 * Different state handles require different code to end up with a {@link DataInputView}.
	 */
	protected abstract DataInputView openDataInputView() throws IOException;
}
