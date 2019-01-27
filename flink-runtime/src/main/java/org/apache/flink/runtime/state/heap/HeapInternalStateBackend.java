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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.async.AbstractAsyncCallableWithResources;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.InternalBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyGroupsStateSnapshot;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredStateMetaInfo;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.StateStorage;
import org.apache.flink.runtime.state.heap.internal.StateTable;
import org.apache.flink.runtime.state.heap.internal.StateTableSnapshot;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedStateDescriptor;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedStateDescriptor;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;

/**
 * Implementation of {@link AbstractInternalStateBackend} which stores the key-value
 * pairs of states on the Java Heap.
 */
public class HeapInternalStateBackend extends AbstractInternalStateBackend {

	private static final Logger LOG = LoggerFactory.getLogger(HeapInternalStateBackend.class);

	/**
	 * The configuration for local recovery.
	 */
	private final LocalRecoveryConfig localRecoveryConfig;

	/**
	 * Whether this backend supports async snapshot.
	 */
	private final boolean asynchronousSnapshot;

	public HeapInternalStateBackend(
		int numberOfGroups,
		KeyGroupRange keyGroupRange,
		ClassLoader userClassLoader,
		LocalRecoveryConfig localRecoveryConfig,
		TaskKvStateRegistry kvStateRegistry,
		boolean asynchronousSnapshot,
		ExecutionConfig executionConfig
	) {
		super(numberOfGroups, keyGroupRange, userClassLoader, kvStateRegistry, executionConfig);

		this.localRecoveryConfig = Preconditions.checkNotNull(localRecoveryConfig);
		this.asynchronousSnapshot = asynchronousSnapshot;

		LOG.info("HeapInternalStateBackend is created with {} mode.", (asynchronousSnapshot ? "async" : "sync"));
	}

	@Override
	public void closeImpl() {

	}

	@Override
	@SuppressWarnings("unchecked")
	protected StateStorage getOrCreateStateStorageForKeyedState(RegisteredStateMetaInfo stateMetaInfo) {
		HeapStateStorage stateStorage = (HeapStateStorage) stateStorages.get(stateMetaInfo.getName());

		if (stateStorage == null) {
			stateStorage = new HeapStateStorage<>(
				this,
				stateMetaInfo,
				VoidNamespace.INSTANCE,
				false,
				asynchronousSnapshot
			);
			stateStorages.put(stateMetaInfo.getName(), stateStorage);
		}
		stateStorage.setStateMetaInfo(stateMetaInfo);

		return stateStorage;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected StateStorage getOrCreateStateStorageForSubKeyedState(RegisteredStateMetaInfo stateMetaInfo) {
		HeapStateStorage stateStorage = (HeapStateStorage) stateStorages.get(stateMetaInfo.getName());

		if (stateStorage == null) {
			stateStorage = new HeapStateStorage<>(
				this,
				stateMetaInfo,
				null,
				true,
				asynchronousSnapshot
			);
			stateStorages.put(stateMetaInfo.getName(), stateStorage);
		}
		stateStorage.setStateMetaInfo(stateMetaInfo);

		return stateStorage;
	}

	@Override
	public int numStateEntries() {
		int count = 0;
		List<Object> stateStorages = getKeyedStates().values().stream().map(KeyedState::getStateStorage).collect(Collectors.toList());
		stateStorages.addAll(getSubKeyedStates().values().stream().map(SubKeyedState::getStateStorage).collect(Collectors.toList()));
		for (Object stateStorage : stateStorages) {
			count += ((HeapStateStorage) stateStorage).getStateTable().size();
		}
		return count;
	}

	/**
	 * Returns the total number of state entries across all keys for the given namespace.
	 */
	@VisibleForTesting
	public int numStateEntries(Object namespace) {
		int count = 0;
		for (SubKeyedState subKeyedState : getSubKeyedStates().values()) {
			count += ((HeapStateStorage) subKeyedState.getStateStorage()).getStateTable().sizeOfNamespace(namespace);
		}
		return count;
	}

	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory primaryStreamFactory,
		CheckpointOptions checkpointOptions
	) {

		if (registeredStateMetaInfos.isEmpty()) {
			return DoneFuture.of(SnapshotResult.empty());
		}

		long syncStartTime = System.currentTimeMillis();

		List<StateMetaInfoSnapshot> keyedStateMetaSnapshots = new ArrayList<>();

		List<StateMetaInfoSnapshot> subKeyedStateMetaSnapshots = new ArrayList<>();

		final Map<String, Integer> keyedStateToId = new HashMap<>();
		final Map<String, Integer> subKeyedStateToId = new HashMap<>();

		final Map<String, org.apache.flink.runtime.state.heap.internal.StateTableSnapshot> keyedStateStableSnapshots = new HashMap<>(this.keyedStates.size());
		final Map<String, org.apache.flink.runtime.state.heap.internal.StateTableSnapshot> subKeyedStateStableSnapshots = new HashMap<>(this.subKeyedStates.size());

		for (Map.Entry<String, RegisteredStateMetaInfo> registeredStateMetaInfoEntry : registeredStateMetaInfos.entrySet()) {
			String stateName = registeredStateMetaInfoEntry.getKey();
			StateTable stateTable = ((HeapStateStorage) stateStorages.get(stateName)).getStateTable();
			RegisteredStateMetaInfo stateMetaInfo = registeredStateMetaInfoEntry.getValue();
			if (stateMetaInfo.getStateType().isKeyedState()) {
				keyedStateMetaSnapshots.add(stateMetaInfo.snapshot());
				keyedStateToId.put(stateName, keyedStateToId.size());
				keyedStateStableSnapshots.put(stateName, stateTable.createSnapshot());
			} else {
				subKeyedStateMetaSnapshots.add(stateMetaInfo.snapshot());
				subKeyedStateToId.put(stateName, subKeyedStateToId.size());
				subKeyedStateStableSnapshots.put(stateName, stateTable.createSnapshot());
			}

		}

		final SupplierWithException<CheckpointStreamWithResultProvider, Exception> checkpointStreamSupplier =
			localRecoveryConfig.isLocalRecoveryEnabled() ?

				() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory,
					localRecoveryConfig.getLocalStateDirectoryProvider()) :

				() -> CheckpointStreamWithResultProvider.createSimpleStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					primaryStreamFactory);

		// implementation of the async IO operation, based on FutureTask
		final AbstractAsyncCallableWithResources<SnapshotResult<KeyedStateHandle>> ioCallable =
			new AbstractAsyncCallableWithResources<SnapshotResult<KeyedStateHandle>>() {

				CheckpointStreamWithResultProvider streamAndResultExtractor = null;

				@Override
				protected void acquireResources() throws Exception {
					streamAndResultExtractor = checkpointStreamSupplier.get();
					cancelStreamRegistry.registerCloseable(streamAndResultExtractor);
				}

				@Override
				protected void releaseResources() {

					unregisterAndCloseStreamAndResultExtractor();

					for (org.apache.flink.runtime.state.heap.internal.StateTableSnapshot tableSnapshot : keyedStateStableSnapshots.values()) {
						tableSnapshot.release();
					}

					for (org.apache.flink.runtime.state.heap.internal.StateTableSnapshot tableSnapshot : subKeyedStateStableSnapshots.values()) {
						tableSnapshot.release();
					}
				}

				@Override
				protected void stopOperation() {
					unregisterAndCloseStreamAndResultExtractor();
				}

				private void unregisterAndCloseStreamAndResultExtractor() {
					if (cancelStreamRegistry.unregisterCloseable(streamAndResultExtractor)) {
						IOUtils.closeQuietly(streamAndResultExtractor);
						streamAndResultExtractor = null;
					}
				}

				@Nonnull
				@Override
				protected SnapshotResult<KeyedStateHandle> performOperation() throws Exception {

					long asyncStartTime = System.currentTimeMillis();

					CheckpointStreamFactory.CheckpointStateOutputStream localStream =
						this.streamAndResultExtractor.getCheckpointOutputStream();

					DataOutputViewStreamWrapper outView = new DataOutputViewStreamWrapper(localStream);

					final InternalBackendSerializationProxy serializationProxy =
						new InternalBackendSerializationProxy(
							keyedStateMetaSnapshots,
							subKeyedStateMetaSnapshots,
							!Objects.equals(UncompressedStreamCompressionDecorator.INSTANCE, keyGroupCompressionDecorator));
					serializationProxy.write(outView);

					Map<Integer, Tuple2<Long, Integer>> metaInfos = new HashMap<>();

					KeyGroupRange groups = getKeyGroupRange();

					for (int group : groups) {

						long offset = localStream.getPos();
						int numEntries = 0;

						outView.writeInt(group);

						// write keyed state
						for (Map.Entry<String, StateTableSnapshot> entry : keyedStateStableSnapshots.entrySet()) {
							numEntries += writeGroupStates(localStream, entry.getValue(), keyedStateToId.get(entry.getKey()), group);
						}

						// write sub-keyed state
						for (Map.Entry<String, StateTableSnapshot> entry : subKeyedStateStableSnapshots.entrySet()) {
							numEntries += writeGroupStates(localStream, entry.getValue(), subKeyedStateToId.get(entry.getKey()), group);
						}

						if (numEntries != 0) {
							metaInfos.put(group, new Tuple2<>(offset, numEntries));
						}
					}

					if (cancelStreamRegistry.unregisterCloseable(streamAndResultExtractor)) {
						SnapshotResult<StreamStateHandle> streamSnapshotResult =
							streamAndResultExtractor.closeAndFinalizeCheckpointStreamResult();
						streamAndResultExtractor = null;

						StreamStateHandle streamStateHandle = streamSnapshotResult.getJobManagerOwnedSnapshot();
						KeyedStateHandle snapshot =
							new KeyGroupsStateSnapshot(
								groups, metaInfos, streamStateHandle);

						LOG.info("Heap backend snapshot (" + primaryStreamFactory + ", asynchronous part) in thread " +
							Thread.currentThread() + " took " + (System.currentTimeMillis() - asyncStartTime) + " ms.");

						StreamStateHandle localStreamStateHandle = streamSnapshotResult.getTaskLocalSnapshot();
						if (localStreamStateHandle != null) {
							KeyedStateHandle localSnapshot =
								new KeyGroupsStateSnapshot(
									groups, metaInfos, localStreamStateHandle);

							return SnapshotResult.withLocalState(snapshot, localSnapshot);
						} else {
							return SnapshotResult.of(snapshot);
						}
					} else {
						throw new IOException("Stream already closed and cannot return a handle.");
					}
				}
			};

		AsyncStoppableTaskWithCallback<SnapshotResult<KeyedStateHandle>> task =
			AsyncStoppableTaskWithCallback.from(ioCallable);

		if (!asynchronousSnapshot) {
			task.run();
		}

		LOG.info("Heap backend snapshot (" + primaryStreamFactory + ", synchronous part) in thread " +
			Thread.currentThread() + " took " + (System.currentTimeMillis() - syncStartTime) + " ms.");

		return task;
	}

	private int writeGroupStates(
		CheckpointStreamFactory.CheckpointStateOutputStream localStream,
		StateTableSnapshot stateTableSnapshot,
		int stateId,
		int group) throws IOException {

		int numEntries = 0;
		try (OutputStream kgCompressionOut = keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
			DataOutputViewStreamWrapper kgCompressionView = new DataOutputViewStreamWrapper(kgCompressionOut);
			kgCompressionView.writeInt(stateId);
			numEntries += stateTableSnapshot.writeMappingsInKeyGroup(kgCompressionView, group);
		}
		return numEntries;
	}

	@Override
	public void restore(
		Collection<KeyedStateHandle> restoredSnapshots
	) throws Exception {
		if (restoredSnapshots == null || restoredSnapshots.isEmpty()) {
			return;
		}

		LOG.info("Initializing heap internal state backend from snapshot.");

		for (KeyedStateHandle rawSnapshot : restoredSnapshots) {
			if (rawSnapshot == null) {
				continue;
			}

			Preconditions.checkState(rawSnapshot instanceof KeyGroupsStateSnapshot);
			KeyGroupsStateSnapshot snapshot =
				(KeyGroupsStateSnapshot) rawSnapshot;

			StreamStateHandle snapshotHandle = snapshot.getSnapshotHandle();
			if (snapshotHandle == null) {
				continue;
			}

			FSDataInputStream inputStream = snapshotHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(inputStream);

			try {
				DataInputViewStreamWrapper inputView =
					new DataInputViewStreamWrapper(inputStream);

				// isSerializerPresenceRequired flag is set to true, since for the heap state backend,
				// deserialization of state happens eagerly at restore time
				InternalBackendSerializationProxy serializationProxy =
					new InternalBackendSerializationProxy(getUserClassLoader(), true);
				serializationProxy.read(inputView);

				Map<Integer, KeyedStateDescriptor> keyedStatesById = new HashMap<>();
				List<StateMetaInfoSnapshot> keyedStateMetaInfos = serializationProxy.getKeyedStateMetaSnapshots();
				for (int i = 0; i < keyedStateMetaInfos.size(); i++) {
					StateMetaInfoSnapshot keyedStateMetaSnapshot = keyedStateMetaInfos.get(i);
					String stateName = keyedStateMetaSnapshot.getName();

					restoredKvStateMetaInfos.put(stateName, keyedStateMetaSnapshot);

					RegisteredStateMetaInfo keyedStateMetaInfo = RegisteredStateMetaInfo.createKeyedStateMetaInfo(keyedStateMetaSnapshot);
					registeredStateMetaInfos.put(stateName, keyedStateMetaInfo);
					KeyedStateDescriptor keyedStateDescriptor = keyedStateMetaSnapshot.createKeyedStateDescriptor();
					StateStorage stateStorage = getOrCreateStateStorageForKeyedState(keyedStateMetaInfo);
					stateStorages.put(stateName, stateStorage);
					keyedStatesById.put(i, keyedStateDescriptor);
				}

				Map<Integer, SubKeyedStateDescriptor> subKeyedStatesById = new HashMap<>();
				List<StateMetaInfoSnapshot> subKeyedStateMetaSnapshots = serializationProxy.getSubKeyedStateMetaSnapshots();
				for (int i = 0; i < subKeyedStateMetaSnapshots.size(); i++) {
					StateMetaInfoSnapshot subKeyedStateMetaSnapshot = subKeyedStateMetaSnapshots.get(i);
					String stateName = subKeyedStateMetaSnapshot.getName();

					RegisteredStateMetaInfo subKeyedStateMetaInfo = RegisteredStateMetaInfo.createSubKeyedStateMetaInfo(subKeyedStateMetaSnapshot);
					registeredStateMetaInfos.put(stateName, subKeyedStateMetaInfo);
					restoredKvStateMetaInfos.put(stateName, subKeyedStateMetaSnapshot);
					SubKeyedStateDescriptor subKeyedStateDescriptor = subKeyedStateMetaSnapshot.createSubKeyedStateDescriptor();
					StateStorage stateStorage = getOrCreateStateStorageForSubKeyedState(subKeyedStateMetaInfo);
					stateStorages.put(stateName, stateStorage);
					subKeyedStatesById.put(i, subKeyedStateDescriptor);
				}

				Map<Integer, Tuple2<Long, Integer>> metaInfos = snapshot.getMetaInfos();

				final StreamCompressionDecorator streamCompressionDecorator = serializationProxy.isUsingKeyGroupCompression() ?
					SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

				for (int group : getKeyGroupRange()) {
					Tuple2<Long, Integer> tuple = metaInfos.get(group);

					if (tuple == null) {
						continue;
					}

					long offset = tuple.f0;
					int totalEntries = tuple.f1;

					inputStream.seek(offset);

					int writtenKeyGroupIndex = inputView.readInt();
					Preconditions.checkState(writtenKeyGroupIndex == group, "Unexpected key-group in restore.");

					int numEntries = 0;

					try (InputStream kgCompressionInStream =
							 streamCompressionDecorator.decorateWithCompression(inputStream)) {
						DataInputViewStreamWrapper kgCompressionInView =
							new DataInputViewStreamWrapper(kgCompressionInStream);

						// restore keyed states
						for (int i = 0; i < keyedStateMetaInfos.size(); i++) {
							int stateId = kgCompressionInView.readInt();
							KeyedStateDescriptor descriptor = keyedStatesById.get(stateId);
							HeapStateStorage stateStorage = (HeapStateStorage) stateStorages.get(descriptor.getName());
							numEntries += readMappingsInKeyGroupForKeyedState(kgCompressionInView, descriptor, stateStorage);
						}

						// restore sub-keyed states
						for (int i = 0; i < subKeyedStateMetaSnapshots.size(); i++) {
							int stateId = kgCompressionInView.readInt();
							SubKeyedStateDescriptor descriptor = subKeyedStatesById.get(stateId);
							HeapStateStorage stateStorage = (HeapStateStorage) stateStorages.get(descriptor.getName());
							numEntries += readMappingsInKeyGroupForSubKeyedState(kgCompressionInView, descriptor, stateStorage);
						}

						Preconditions.checkState(totalEntries == numEntries, "Unexpected number of entries");
					}

				}
			} finally {
				if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
					IOUtils.closeQuietly(inputStream);
				}
			}
		}
	}

	//------------------------------------------------------------------------------------------------------------------

	private int readMappingsInKeyGroupForKeyedState(
		DataInputView inView,
		KeyedStateDescriptor descriptor,
		HeapStateStorage stateStorage
	) throws Exception {

		final TypeSerializer keySerializer = descriptor.getKeySerializer();
		final TypeSerializer stateSerializer = descriptor.getValueSerializer();

		int numKeys = inView.readInt();
		for (int i = 0; i < numKeys; ++i) {
			Object key = keySerializer.deserialize(inView);
			Object state = stateSerializer.deserialize(inView);
			stateStorage.put(key, state);
		}

		return numKeys;
	}

	private int readMappingsInKeyGroupForSubKeyedState(
		DataInputView inView,
		SubKeyedStateDescriptor descriptor,
		HeapStateStorage stateStorage
	) throws Exception {

		final TypeSerializer keySerializer = descriptor.getKeySerializer();
		final TypeSerializer namespaceSerializer = descriptor.getNamespaceSerializer();
		final TypeSerializer stateSerializer = descriptor.getValueSerializer();

		int numKeys = inView.readInt();
		for (int i = 0; i < numKeys; ++i) {
			Object key = keySerializer.deserialize(inView);
			Object namespace = namespaceSerializer.deserialize(inView);
			Object state = stateSerializer.deserialize(inView);
			stateStorage.setCurrentNamespace(namespace);
			stateStorage.put(key, state);
		}

		return numKeys;
	}

}
