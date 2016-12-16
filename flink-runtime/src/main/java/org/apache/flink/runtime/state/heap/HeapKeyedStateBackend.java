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

package org.apache.flink.runtime.state.heap;

import org.apache.commons.io.IOUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.migration.MigrationUtil;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;
import org.apache.flink.migration.runtime.state.filesystem.AbstractFsStateSnapshot;
import org.apache.flink.migration.runtime.state.memory.AbstractMemStateSnapshot;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;

/**
 * A {@link AbstractKeyedStateBackend} that keeps state on the Java Heap and will serialize state to
 * streams provided by a {@link org.apache.flink.runtime.state.CheckpointStreamFactory} upon
 * checkpointing.
 *
 * @param <K> The key by which state is keyed.
 */
public class HeapKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

	private static final Logger LOG = LoggerFactory.getLogger(HeapKeyedStateBackend.class);

	/**
	 * Map of state tables that stores all state of key/value states. We store it centrally so
	 * that we can easily checkpoint/restore it.
	 *
	 * <p>The actual parameters of StateTable are {@code StateTable<NamespaceT, Map<KeyT, StateT>>}
	 * but we can't put them here because different key/value states with different types and
	 * namespace types share this central list of tables.
	 */
	private final Map<String, StateTable<K, ?, ?>> stateTables = new HashMap<>();

	public HeapKeyedStateBackend(
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			ClassLoader userCodeClassLoader,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange) {

		super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange);

		LOG.info("Initializing heap keyed state backend with stream factory.");
	}

	public HeapKeyedStateBackend(
			TaskKvStateRegistry kvStateRegistry,
			TypeSerializer<K> keySerializer,
			ClassLoader userCodeClassLoader,
			int numberOfKeyGroups,
			KeyGroupRange keyGroupRange,
			Collection<KeyGroupsStateHandle> restoredState) throws Exception {
		super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange);

		LOG.info("Initializing heap keyed state backend from snapshot.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoredState);
		}

		if (MigrationUtil.isOldSavepointKeyedState(restoredState)) {
			restoreOldSavepointKeyedState(restoredState);
		} else {
			restorePartitionedState(restoredState);
		}
	}

	// ------------------------------------------------------------------------
	//  state backend operations
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private <N, V> StateTable<K, N, V> tryRegisterStateTable(
			TypeSerializer<N> namespaceSerializer, StateDescriptor<?, V> stateDesc) {

		String name = stateDesc.getName();
		StateTable<K, N, V> stateTable = (StateTable<K, N, V>) stateTables.get(name);

		RegisteredBackendStateMetaInfo<N, V> newMetaInfo =
				new RegisteredBackendStateMetaInfo<>(stateDesc.getType(), name, namespaceSerializer, stateDesc.getSerializer());

		return tryRegisterStateTable(stateTable, newMetaInfo);
	}

	private <N, V> StateTable<K, N, V> tryRegisterStateTable(
			StateTable<K, N, V> stateTable, RegisteredBackendStateMetaInfo<N, V> newMetaInfo) {

		if (stateTable == null) {
			stateTable = new StateTable<>(newMetaInfo, keyGroupRange);
			stateTables.put(newMetaInfo.getName(), stateTable);
		} else {
			if (!newMetaInfo.isCompatibleWith(stateTable.getMetaInfo())) {
				throw new RuntimeException("Trying to access state using incompatible meta info, was " +
						stateTable.getMetaInfo() + " trying access with " + newMetaInfo);
			}
			stateTable.setMetaInfo(newMetaInfo);
		}
		return stateTable;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <N, V> ValueState<V> createValueState(TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<V> stateDesc) throws Exception {
		StateTable<K, N, V> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapValueState<>(this, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception {
		String name = stateDesc.getName();
		StateTable<K, N, ArrayList<T>> stateTable = (StateTable<K, N, ArrayList<T>>) stateTables.get(name);

		RegisteredBackendStateMetaInfo<N, ArrayList<T>> newMetaInfo =
				new RegisteredBackendStateMetaInfo<>(stateDesc.getType(), name, namespaceSerializer, new ArrayListSerializer<>(stateDesc.getSerializer()));

		stateTable = tryRegisterStateTable(stateTable, newMetaInfo);
		return new HeapListState<>(this, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer, ReducingStateDescriptor<T> stateDesc) throws Exception {
		StateTable<K, N, T> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapReducingState<>(this, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}
	@SuppressWarnings("unchecked")
	@Override
	protected <N, T, ACC> FoldingState<T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer, FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
		StateTable<K, N, ACC> stateTable = tryRegisterStateTable(namespaceSerializer, stateDesc);
		return new HeapFoldingState<>(this, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	@SuppressWarnings("unchecked")
	public RunnableFuture<KeyGroupsStateHandle> snapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory streamFactory) throws Exception {

		if (stateTables.isEmpty()) {
			return new DoneFuture<>(null);
		}

		try (CheckpointStreamFactory.CheckpointStateOutputStream stream = streamFactory.
				createCheckpointStateOutputStream(checkpointId, timestamp)) {

			DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(stream);

			Preconditions.checkState(stateTables.size() <= Short.MAX_VALUE,
					"Too many KV-States: " + stateTables.size() +
							". Currently at most " + Short.MAX_VALUE + " states are supported");

			List<KeyedBackendSerializationProxy.StateMetaInfo<?, ?>> metaInfoProxyList = new ArrayList<>(stateTables.size());

			Map<String, Integer> kVStateToId = new HashMap<>(stateTables.size());

			for (Map.Entry<String, StateTable<K, ?, ?>> kvState : stateTables.entrySet()) {

				RegisteredBackendStateMetaInfo<?, ?> metaInfo = kvState.getValue().getMetaInfo();
				KeyedBackendSerializationProxy.StateMetaInfo<?, ?> metaInfoProxy = new KeyedBackendSerializationProxy.StateMetaInfo(
						metaInfo.getStateType(),
						metaInfo.getName(),
						metaInfo.getNamespaceSerializer(),
						metaInfo.getStateSerializer());

				metaInfoProxyList.add(metaInfoProxy);
				kVStateToId.put(kvState.getKey(), kVStateToId.size());
			}

			KeyedBackendSerializationProxy serializationProxy =
					new KeyedBackendSerializationProxy(keySerializer, metaInfoProxyList);

			serializationProxy.write(outView);

			int offsetCounter = 0;
			long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];

			for (int keyGroupIndex = keyGroupRange.getStartKeyGroup(); keyGroupIndex <= keyGroupRange.getEndKeyGroup(); keyGroupIndex++) {
				keyGroupRangeOffsets[offsetCounter++] = stream.getPos();
				outView.writeInt(keyGroupIndex);
				for (Map.Entry<String, StateTable<K, ?, ?>> kvState : stateTables.entrySet()) {
					outView.writeShort(kVStateToId.get(kvState.getKey()));
					writeStateTableForKeyGroup(outView, kvState.getValue(), keyGroupIndex);
				}
			}

			StreamStateHandle streamStateHandle = stream.closeAndGetHandle();

			KeyGroupRangeOffsets offsets = new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);
			final KeyGroupsStateHandle keyGroupsStateHandle = new KeyGroupsStateHandle(offsets, streamStateHandle);
			return new DoneFuture<>(keyGroupsStateHandle);
		}
	}

	private <N, S> void writeStateTableForKeyGroup(
			DataOutputView outView,
			StateTable<K, N, S> stateTable,
			int keyGroupIndex) throws IOException {

		TypeSerializer<N> namespaceSerializer = stateTable.getNamespaceSerializer();
		TypeSerializer<S> stateSerializer = stateTable.getStateSerializer();

		Map<N, Map<K, S>> namespaceMap = stateTable.get(keyGroupIndex);
		if (namespaceMap == null) {
			outView.writeByte(0);
		} else {
			outView.writeByte(1);

			// number of namespaces
			outView.writeInt(namespaceMap.size());
			for (Map.Entry<N, Map<K, S>> namespace : namespaceMap.entrySet()) {
				namespaceSerializer.serialize(namespace.getKey(), outView);

				Map<K, S> entryMap = namespace.getValue();

				// number of entries
				outView.writeInt(entryMap.size());
				for (Map.Entry<K, S> entry : entryMap.entrySet()) {
					keySerializer.serialize(entry.getKey(), outView);
					stateSerializer.serialize(entry.getValue(), outView);
				}
			}
		}
	}

	@SuppressWarnings({"unchecked"})
	private void restorePartitionedState(Collection<KeyGroupsStateHandle> state) throws Exception {

		int numRegisteredKvStates = 0;
		Map<Integer, String> kvStatesById = new HashMap<>();

		for (KeyGroupsStateHandle keyGroupsHandle : state) {

			if (keyGroupsHandle == null) {
				continue;
			}

			FSDataInputStream fsDataInputStream = keyGroupsHandle.openInputStream();
			cancelStreamRegistry.registerClosable(fsDataInputStream);

			try {
				DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(fsDataInputStream);

				KeyedBackendSerializationProxy serializationProxy =
						new KeyedBackendSerializationProxy(userCodeClassLoader);

				serializationProxy.read(inView);

				List<KeyedBackendSerializationProxy.StateMetaInfo<?, ?>> metaInfoList =
						serializationProxy.getNamedStateSerializationProxies();

				for (KeyedBackendSerializationProxy.StateMetaInfo<?, ?> metaInfoSerializationProxy : metaInfoList) {

					StateTable<K, ?, ?> stateTable = stateTables.get(metaInfoSerializationProxy.getStateName());

					//important: only create a new table we did not already create it previously
					if (null == stateTable) {

						RegisteredBackendStateMetaInfo<?, ?> registeredBackendStateMetaInfo =
								new RegisteredBackendStateMetaInfo<>(metaInfoSerializationProxy);

						stateTable = new StateTable<>(registeredBackendStateMetaInfo, keyGroupRange);
						stateTables.put(metaInfoSerializationProxy.getStateName(), stateTable);
						kvStatesById.put(numRegisteredKvStates, metaInfoSerializationProxy.getStateName());
						++numRegisteredKvStates;
					}
				}

				for (Tuple2<Integer, Long> groupOffset : keyGroupsHandle.getGroupRangeOffsets()) {
					int keyGroupIndex = groupOffset.f0;
					long offset = groupOffset.f1;
					fsDataInputStream.seek(offset);

					int writtenKeyGroupIndex = inView.readInt();
					assert writtenKeyGroupIndex == keyGroupIndex;

					for (int i = 0; i < metaInfoList.size(); i++) {
						int kvStateId = inView.readShort();

						byte isPresent = inView.readByte();
						if (isPresent == 0) {
							continue;
						}

						StateTable<K, ?, ?> stateTable = stateTables.get(kvStatesById.get(kvStateId));
						Preconditions.checkNotNull(stateTable);

						readStateTableForKeyGroup(inView, stateTable, keyGroupIndex);
					}
				}
			} finally {
				cancelStreamRegistry.unregisterClosable(fsDataInputStream);
				IOUtils.closeQuietly(fsDataInputStream);
			}
		}
	}

	private <N, S> void readStateTableForKeyGroup(
			DataInputView inView,
			StateTable<K, N, S> stateTable,
			int keyGroupIndex) throws IOException {

		TypeSerializer<N> namespaceSerializer = stateTable.getNamespaceSerializer();
		TypeSerializer<S> stateSerializer = stateTable.getStateSerializer();

		Map<N, Map<K, S>> namespaceMap = new HashMap<>();
		stateTable.set(keyGroupIndex, namespaceMap);

		int numNamespaces = inView.readInt();
		for (int k = 0; k < numNamespaces; k++) {
			N namespace = namespaceSerializer.deserialize(inView);
			Map<K, S> entryMap = new HashMap<>();
			namespaceMap.put(namespace, entryMap);

			int numEntries = inView.readInt();
			for (int l = 0; l < numEntries; l++) {
				K key = keySerializer.deserialize(inView);
				S state = stateSerializer.deserialize(inView);
				entryMap.put(key, state);
			}
		}
	}

	@Override
	public String toString() {
		return "HeapKeyedStateBackend";
	}

	/**
	 * REMOVE
	 */
	@Internal
	@Deprecated
	public Map<String, StateTable<K, ?, ?>> getStateTables() {
		return stateTables;
	}

	@Deprecated
	private void restoreOldSavepointKeyedState(
			Collection<KeyGroupsStateHandle> stateHandles) throws IOException, ClassNotFoundException {

		if (stateHandles.isEmpty()) {
			return;
		}

		Preconditions.checkState(1 == stateHandles.size(), "Only one element expected here.");

		HashMap<String, KvStateSnapshot<K, ?, ?, ?>> namedStates;
		try (FSDataInputStream inputStream = stateHandles.iterator().next().openInputStream()) {
			namedStates = InstantiationUtil.deserializeObject(inputStream, userCodeClassLoader);
		}

		for (Map.Entry<String, KvStateSnapshot<K, ?, ?, ?>> nameToState : namedStates.entrySet()) {

			KvStateSnapshot<K, ?, ?, ?> genericSnapshot = nameToState.getValue();

			final RestoredState restoredState;

			if (genericSnapshot instanceof AbstractMemStateSnapshot) {

				AbstractMemStateSnapshot<K, ?, ?, ?, ?> stateSnapshot =
						(AbstractMemStateSnapshot<K, ?, ?, ?, ?>) nameToState.getValue();

				restoredState = restoreHeapState(stateSnapshot);

			} else if (genericSnapshot instanceof AbstractFsStateSnapshot) {

				AbstractFsStateSnapshot<K, ?, ?, ?, ?> stateSnapshot =
						(AbstractFsStateSnapshot<K, ?, ?, ?, ?>) nameToState.getValue();
				restoredState = restoreFsState(stateSnapshot);
			} else {
				throw new IllegalStateException("Unknown state: " + genericSnapshot);
			}

			Map rawResultMap = restoredState.getRawResultMap();
			TypeSerializer<?> namespaceSerializer = restoredState.getNamespaceSerializer();
			TypeSerializer<?> stateSerializer = restoredState.getStateSerializer();

			if (namespaceSerializer instanceof VoidSerializer) {
				namespaceSerializer = VoidNamespaceSerializer.INSTANCE;
			}

			Map nullNameSpaceFix = (Map) rawResultMap.remove(null);

			if (null != nullNameSpaceFix) {
				rawResultMap.put(VoidNamespace.INSTANCE, nullNameSpaceFix);
			}

			RegisteredBackendStateMetaInfo<?, ?> registeredBackendStateMetaInfo =
					new RegisteredBackendStateMetaInfo<>(
							StateDescriptor.Type.UNKNOWN,
							nameToState.getKey(),
							namespaceSerializer,
							stateSerializer);

			StateTable<K, ?, ?> stateTable = new StateTable<>(registeredBackendStateMetaInfo, keyGroupRange);
			stateTable.getState().set(0, rawResultMap);

			// add named state to the backend
			getStateTables().put(registeredBackendStateMetaInfo.getName(), stateTable);
		}
	}

	private RestoredState restoreHeapState(AbstractMemStateSnapshot<K, ?, ?, ?, ?> stateSnapshot) throws IOException {
		return new RestoredState(
				stateSnapshot.deserialize(),
				stateSnapshot.getNamespaceSerializer(),
				stateSnapshot.getStateSerializer());
	}

	private RestoredState restoreFsState(AbstractFsStateSnapshot<K, ?, ?, ?, ?> stateSnapshot) throws IOException {
		FileSystem fs = stateSnapshot.getFilePath().getFileSystem();
		//TODO register closeable to support fast cancelation?
		try (FSDataInputStream inStream = fs.open(stateSnapshot.getFilePath())) {

			DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(inStream);

			final int numNamespaces = inView.readInt();
			HashMap rawResultMap = new HashMap<>(numNamespaces);

			TypeSerializer<K> keySerializer = stateSnapshot.getKeySerializer();
			TypeSerializer<?> namespaceSerializer = stateSnapshot.getNamespaceSerializer();
			TypeSerializer<?> stateSerializer = stateSnapshot.getStateSerializer();

			for (int i = 0; i < numNamespaces; i++) {
				Object namespace = namespaceSerializer.deserialize(inView);
				final int numKV = inView.readInt();
				Map<K, Object> namespaceMap = new HashMap<>(numKV);
				rawResultMap.put(namespace, namespaceMap);
				for (int j = 0; j < numKV; j++) {
					K key = keySerializer.deserialize(inView);
					Object value = stateSerializer.deserialize(inView);
					namespaceMap.put(key, value);
				}
			}
			return new RestoredState(rawResultMap, namespaceSerializer, stateSerializer);
		} catch (Exception e) {
			throw new IOException("Failed to restore state from file system", e);
		}
	}

	static final class RestoredState {

		private final Map rawResultMap;
		private final TypeSerializer<?> namespaceSerializer;
		private final TypeSerializer<?> stateSerializer ;

		public RestoredState(Map rawResultMap, TypeSerializer<?> namespaceSerializer, TypeSerializer<?> stateSerializer) {
			this.rawResultMap = rawResultMap;
			this.namespaceSerializer = namespaceSerializer;
			this.stateSerializer = stateSerializer;
		}

		public Map getRawResultMap() {
			return rawResultMap;
		}

		public TypeSerializer<?> getNamespaceSerializer() {
			return namespaceSerializer;
		}

		public TypeSerializer<?> getStateSerializer() {
			return stateSerializer;
		}
	}
}
