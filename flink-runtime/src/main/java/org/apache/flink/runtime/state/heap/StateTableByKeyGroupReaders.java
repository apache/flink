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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;

/**
 * This class provides a static factory method to create different implementations of {@link StateTableByKeyGroupReader}
 * depending on the provided serialization format version.
 * <p>
 * The implementations are also located here as inner classes.
 */
class StateTableByKeyGroupReaders {

	/**
	 * Creates a new StateTableByKeyGroupReader that inserts de-serialized mappings into the given table, using the
	 * de-serialization algorithm that matches the given version.
	 *
	 * @param table the {@link StateTable} into which de-serialized mappings are inserted.
	 * @param version version for the de-serialization algorithm.
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state.
	 * @return the appropriate reader.
	 */
	static <K, N, S> StateTableByKeyGroupReader readerForVersion(StateTable<K, N, S> table, int version) {
		switch (version) {
			case 1:
				return new StateTableByKeyGroupReaderV1<>(table);
			case 2:
			case 3:
			case 4:
				return new StateTableByKeyGroupReaderV2V3<>(table);
			default:
				throw new IllegalArgumentException("Unknown version: " + version);
		}
	}

	static abstract class AbstractStateTableByKeyGroupReader<K, N, S>
			implements StateTableByKeyGroupReader {

		protected final StateTable<K, N, S> stateTable;

		AbstractStateTableByKeyGroupReader(StateTable<K, N, S> stateTable) {
			this.stateTable = stateTable;
		}

		@Override
		public abstract void readMappingsInKeyGroup(DataInputView div, int keyGroupId) throws IOException;

		protected TypeSerializer<K> getKeySerializer() {
			return stateTable.keyContext.getKeySerializer();
		}

		protected TypeSerializer<N> getNamespaceSerializer() {
			return stateTable.getNamespaceSerializer();
		}

		protected TypeSerializer<S> getStateSerializer() {
			return stateTable.getStateSerializer();
		}
	}

	static final class StateTableByKeyGroupReaderV1<K, N, S>
			extends AbstractStateTableByKeyGroupReader<K, N, S> {

		StateTableByKeyGroupReaderV1(StateTable<K, N, S> stateTable) {
			super(stateTable);
		}

		@Override
		public void readMappingsInKeyGroup(DataInputView inView, int keyGroupId) throws IOException {

			if (inView.readByte() == 0) {
				return;
			}

			final TypeSerializer<K> keySerializer = getKeySerializer();
			final TypeSerializer<N> namespaceSerializer = getNamespaceSerializer();
			final TypeSerializer<S> stateSerializer = getStateSerializer();

			// V1 uses kind of namespace compressing format
			int numNamespaces = inView.readInt();
			for (int k = 0; k < numNamespaces; k++) {
				N namespace = namespaceSerializer.deserialize(inView);
				int numEntries = inView.readInt();
				for (int l = 0; l < numEntries; l++) {
					K key = keySerializer.deserialize(inView);
					S state = stateSerializer.deserialize(inView);
					stateTable.put(key, keyGroupId, namespace, state);
				}
			}
		}
	}

	private static final class StateTableByKeyGroupReaderV2V3<K, N, S>
			extends AbstractStateTableByKeyGroupReader<K, N, S> {

		StateTableByKeyGroupReaderV2V3(StateTable<K, N, S> stateTable) {
			super(stateTable);
		}

		@Override
		public void readMappingsInKeyGroup(DataInputView inView, int keyGroupId) throws IOException {

			final TypeSerializer<K> keySerializer = getKeySerializer();
			final TypeSerializer<N> namespaceSerializer = getNamespaceSerializer();
			final TypeSerializer<S> stateSerializer = getStateSerializer();

			int numKeys = inView.readInt();
			for (int i = 0; i < numKeys; ++i) {
				N namespace = namespaceSerializer.deserialize(inView);
				K key = keySerializer.deserialize(inView);
				S state = stateSerializer.deserialize(inView);
				stateTable.put(key, keyGroupId, namespace, state);
			}
		}
	}
}
