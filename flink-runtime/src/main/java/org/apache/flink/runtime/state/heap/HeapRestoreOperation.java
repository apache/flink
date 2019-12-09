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

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of heap restore operation.
 *
 * @param <K> The data type that the serializer serializes.
 */
public class HeapRestoreOperation<K> implements RestoreOperation<Void> {
	private final Collection<KeyedStateHandle> restoreStateHandles;
	private final StateSerializerProvider<K> keySerializerProvider;
	private final ClassLoader userCodeClassLoader;
	private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;
	private final CloseableRegistry cancelStreamRegistry;
	private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
	@Nonnull
	private final KeyGroupRange keyGroupRange;
	@Nonnegative
	private final int numberOfKeyGroups;
	private final HeapSnapshotStrategy<K> snapshotStrategy;
	private final InternalKeyContext<K> keyContext;

	HeapRestoreOperation(
		@Nonnull Collection<KeyedStateHandle> restoreStateHandles,
		StateSerializerProvider<K> keySerializerProvider,
		ClassLoader userCodeClassLoader,
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		CloseableRegistry cancelStreamRegistry,
		HeapPriorityQueueSetFactory priorityQueueSetFactory,
		@Nonnull KeyGroupRange keyGroupRange,
		int numberOfKeyGroups,
		HeapSnapshotStrategy<K> snapshotStrategy,
		InternalKeyContext<K> keyContext) {
		this.restoreStateHandles = restoreStateHandles;
		this.keySerializerProvider = keySerializerProvider;
		this.userCodeClassLoader = userCodeClassLoader;
		this.registeredKVStates = registeredKVStates;
		this.registeredPQStates = registeredPQStates;
		this.cancelStreamRegistry = cancelStreamRegistry;
		this.priorityQueueSetFactory = priorityQueueSetFactory;
		this.keyGroupRange = keyGroupRange;
		this.numberOfKeyGroups = numberOfKeyGroups;
		this.snapshotStrategy = snapshotStrategy;
		this.keyContext = keyContext;
	}

	@Override
	public Void restore() throws Exception {

		final Map<Integer, StateMetaInfoSnapshot> kvStatesById = new HashMap<>();
		registeredKVStates.clear();
		registeredPQStates.clear();

		boolean keySerializerRestored = false;

		for (KeyedStateHandle keyedStateHandle : restoreStateHandles) {

			if (keyedStateHandle == null) {
				continue;
			}

			if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected: " + KeyGroupsStateHandle.class +
					", but found: " + keyedStateHandle.getClass());
			}

			KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
			FSDataInputStream fsDataInputStream = keyGroupsStateHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(fsDataInputStream);

			try {
				DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(fsDataInputStream);

				KeyedBackendSerializationProxy<K> serializationProxy =
					new KeyedBackendSerializationProxy<>(userCodeClassLoader);

				serializationProxy.read(inView);

				if (!keySerializerRestored) {
					// check for key serializer compatibility; this also reconfigures the
					// key serializer to be compatible, if it is required and is possible
					TypeSerializerSchemaCompatibility<K> keySerializerSchemaCompat =
						keySerializerProvider.setPreviousSerializerSnapshotForRestoredState(serializationProxy.getKeySerializerSnapshot());
					if (keySerializerSchemaCompat.isCompatibleAfterMigration() || keySerializerSchemaCompat.isIncompatible()) {
						throw new StateMigrationException("The new key serializer must be compatible.");
					}

					keySerializerRestored = true;
				}

				List<StateMetaInfoSnapshot> restoredMetaInfos =
					serializationProxy.getStateMetaInfoSnapshots();

				createOrCheckStateForMetaInfo(restoredMetaInfos, kvStatesById);

				readStateHandleStateData(
					fsDataInputStream,
					inView,
					keyGroupsStateHandle.getGroupRangeOffsets(),
					kvStatesById, restoredMetaInfos.size(),
					serializationProxy.getReadVersion(),
					serializationProxy.isUsingKeyGroupCompression());
			} finally {
				if (cancelStreamRegistry.unregisterCloseable(fsDataInputStream)) {
					IOUtils.closeQuietly(fsDataInputStream);
				}
			}
		}
		return null;
	}

	private void createOrCheckStateForMetaInfo(
		List<StateMetaInfoSnapshot> restoredMetaInfo,
		Map<Integer, StateMetaInfoSnapshot> kvStatesById) {

		for (StateMetaInfoSnapshot metaInfoSnapshot : restoredMetaInfo) {
			final StateSnapshotRestore registeredState;

			switch (metaInfoSnapshot.getBackendStateType()) {
				case KEY_VALUE:
					registeredState = registeredKVStates.get(metaInfoSnapshot.getName());
					if (registeredState == null) {
						RegisteredKeyValueStateBackendMetaInfo<?, ?> registeredKeyedBackendStateMetaInfo =
							new RegisteredKeyValueStateBackendMetaInfo<>(metaInfoSnapshot);
						registeredKVStates.put(
							metaInfoSnapshot.getName(),
							snapshotStrategy.newStateTable(
								keyContext,
								registeredKeyedBackendStateMetaInfo,
								keySerializerProvider.currentSchemaSerializer()));
					}
					break;
				case PRIORITY_QUEUE:
					registeredState = registeredPQStates.get(metaInfoSnapshot.getName());
					if (registeredState == null) {
						createInternal(new RegisteredPriorityQueueStateBackendMetaInfo<>(metaInfoSnapshot));
					}
					break;
				default:
					throw new IllegalStateException("Unexpected state type: " +
						metaInfoSnapshot.getBackendStateType() + ".");
			}

			if (registeredState == null) {
				kvStatesById.put(kvStatesById.size(), metaInfoSnapshot);
			}
		}
	}

	private <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> void createInternal(
		RegisteredPriorityQueueStateBackendMetaInfo<T> metaInfo) {

		final String stateName = metaInfo.getName();
		final HeapPriorityQueueSet<T> priorityQueue = priorityQueueSetFactory.create(
			stateName,
			metaInfo.getElementSerializer());

		HeapPriorityQueueSnapshotRestoreWrapper<T> wrapper =
			new HeapPriorityQueueSnapshotRestoreWrapper<>(
				priorityQueue,
				metaInfo,
				KeyExtractorFunction.forKeyedObjects(),
				keyGroupRange,
				numberOfKeyGroups);

		registeredPQStates.put(stateName, wrapper);
	}

	private void readStateHandleStateData(
		FSDataInputStream fsDataInputStream,
		DataInputViewStreamWrapper inView,
		KeyGroupRangeOffsets keyGroupOffsets,
		Map<Integer, StateMetaInfoSnapshot> kvStatesById,
		int numStates,
		int readVersion,
		boolean isCompressed) throws IOException {

		final StreamCompressionDecorator streamCompressionDecorator = isCompressed ?
			SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

		for (Tuple2<Integer, Long> groupOffset : keyGroupOffsets) {
			int keyGroupIndex = groupOffset.f0;
			long offset = groupOffset.f1;

			// Check that restored key groups all belong to the backend.
			Preconditions.checkState(keyGroupRange.contains(keyGroupIndex), "The key group must belong to the backend.");

			fsDataInputStream.seek(offset);

			int writtenKeyGroupIndex = inView.readInt();
			Preconditions.checkState(writtenKeyGroupIndex == keyGroupIndex,
				"Unexpected key-group in restore.");

			try (InputStream kgCompressionInStream =
					 streamCompressionDecorator.decorateWithCompression(fsDataInputStream)) {

				readKeyGroupStateData(
					kgCompressionInStream,
					kvStatesById,
					keyGroupIndex,
					numStates,
					readVersion);
			}
		}
	}

	private void readKeyGroupStateData(
		InputStream inputStream,
		Map<Integer, StateMetaInfoSnapshot> kvStatesById,
		int keyGroupIndex,
		int numStates,
		int readVersion) throws IOException {

		DataInputViewStreamWrapper inView =
			new DataInputViewStreamWrapper(inputStream);

		for (int i = 0; i < numStates; i++) {

			final int kvStateId = inView.readShort();
			final StateMetaInfoSnapshot stateMetaInfoSnapshot = kvStatesById.get(kvStateId);
			final StateSnapshotRestore registeredState;

			switch (stateMetaInfoSnapshot.getBackendStateType()) {
				case KEY_VALUE:
					registeredState = registeredKVStates.get(stateMetaInfoSnapshot.getName());
					break;
				case PRIORITY_QUEUE:
					registeredState = registeredPQStates.get(stateMetaInfoSnapshot.getName());
					break;
				default:
					throw new IllegalStateException("Unexpected state type: " +
						stateMetaInfoSnapshot.getBackendStateType() + ".");
			}

			StateSnapshotKeyGroupReader keyGroupReader = registeredState.keyGroupReader(readVersion);
			keyGroupReader.readMappingsInKeyGroup(inView, keyGroupIndex);
		}
	}
}
