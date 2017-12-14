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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.async.AbstractAsyncCallableWithResources;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RunnableFuture;

/**
 * Default implementation of OperatorStateStore that provides the ability to make snapshots.
 */
@Internal
public class DefaultOperatorStateBackend implements OperatorStateBackend {

	private static final Logger LOG = LoggerFactory.getLogger(DefaultOperatorStateBackend.class);

	/**
	 * The default namespace for state in cases where no state name is provided
	 */
	public static final String DEFAULT_OPERATOR_STATE_NAME = "_default_";

	/**
	 * Map for all registered operator states. Maps state name -> state
	 */
	private final Map<String, PartitionableListState<?>> registeredStates;

	/**
	 * CloseableRegistry to participate in the tasks lifecycle.
	 */
	private final CloseableRegistry closeStreamOnCancelRegistry;

	/**
	 * Default serializer. Only used for the default operator state.
	 */
	private final JavaSerializer<Serializable> javaSerializer;

	/**
	 * The user code classloader.
	 */
	private final ClassLoader userClassloader;

	/**
	 * The execution configuration.
	 */
	private final ExecutionConfig executionConfig;

	/**
	 * Flag to de/activate asynchronous snapshots.
	 */
	private final boolean asynchronousSnapshots;

	/**
	 * Map of state names to their corresponding restored state meta info.
	 *
	 * <p>TODO this map can be removed when eager-state registration is in place.
	 * TODO we currently need this cached to check state migration strategies when new serializers are registered.
	 */
	private final Map<String, RegisteredOperatorBackendStateMetaInfo.Snapshot<?>> restoredStateMetaInfos;

	/**
	 * Cache of already accessed states.
	 *
	 * <p>In contrast to {@link #registeredStates} and {@link #restoredStateMetaInfos} which may be repopulated
	 * with restored state, this map is always empty at the beginning.
	 *
	 * <p>TODO this map should be moved to a base class once we have proper hierarchy for the operator state backends.
	 *
	 * @see <a href="https://issues.apache.org/jira/browse/FLINK-6849">FLINK-6849</a>
	 */
	private final HashMap<String, PartitionableListState<?>> accessedStatesByName;

	public DefaultOperatorStateBackend(
		ClassLoader userClassLoader,
		ExecutionConfig executionConfig,
		boolean asynchronousSnapshots) throws IOException {

		this.closeStreamOnCancelRegistry = new CloseableRegistry();
		this.userClassloader = Preconditions.checkNotNull(userClassLoader);
		this.executionConfig = executionConfig;
		this.javaSerializer = new JavaSerializer<>();
		this.registeredStates = new HashMap<>();
		this.asynchronousSnapshots = asynchronousSnapshots;
		this.accessedStatesByName = new HashMap<>();
		this.restoredStateMetaInfos = new HashMap<>();
	}

	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	@Override
	public Set<String> getRegisteredStateNames() {
		return registeredStates.keySet();
	}

	@Override
	public void close() throws IOException {
		closeStreamOnCancelRegistry.close();
	}

	@Override
	public void dispose() {
		IOUtils.closeQuietly(this);
		registeredStates.clear();
	}

	// -------------------------------------------------------------------------------------------
	//  State access methods
	// -------------------------------------------------------------------------------------------

	@Override
	public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
		return getListState(stateDescriptor, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);
	}

	@Override
	public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
		return getListState(stateDescriptor, OperatorStateHandle.Mode.BROADCAST);
	}

	// -------------------------------------------------------------------------------------------
	//  Deprecated state access methods
	// -------------------------------------------------------------------------------------------

	/**
	 * @deprecated This was deprecated as part of a refinement to the function names.
	 *             Please use {@link #getListState(ListStateDescriptor)} instead.
	 */
	@Deprecated
	@Override
	public <S> ListState<S> getOperatorState(ListStateDescriptor<S> stateDescriptor) throws Exception {
		return getListState(stateDescriptor);
	}

	/**
	 * @deprecated Using Java serialization for persisting state is not encouraged.
	 *             Please use {@link #getListState(ListStateDescriptor)} instead.
	 */
	@SuppressWarnings("unchecked")
	@Deprecated
	@Override
	public <T extends Serializable> ListState<T> getSerializableListState(String stateName) throws Exception {
		return (ListState<T>) getListState(new ListStateDescriptor<>(stateName, javaSerializer));
	}

	// -------------------------------------------------------------------------------------------
	//  Snapshot and restore
	// -------------------------------------------------------------------------------------------

	@Override
	public RunnableFuture<OperatorStateHandle> snapshot(
			final long checkpointId,
			final long timestamp,
			final CheckpointStreamFactory streamFactory,
			final CheckpointOptions checkpointOptions) throws Exception {

		final long syncStartTime = System.currentTimeMillis();

		if (registeredStates.isEmpty()) {
			return DoneFuture.nullValue();
		}

		final Map<String, PartitionableListState<?>> registeredStatesDeepCopies =
				new HashMap<>(registeredStates.size());

		// eagerly create deep copies of the list states in the sync phase, so that we can use them in the async writing
		ClassLoader snapshotClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(userClassloader);
		try {
			for (Map.Entry<String, PartitionableListState<?>> entry : this.registeredStates.entrySet()) {
				PartitionableListState<?> listState = entry.getValue();
				if (null != listState) {
					listState = listState.deepCopy();
				}
				registeredStatesDeepCopies.put(entry.getKey(), listState);
			}
		} finally {
			Thread.currentThread().setContextClassLoader(snapshotClassLoader);
		}

		// implementation of the async IO operation, based on FutureTask
		final AbstractAsyncCallableWithResources<OperatorStateHandle> ioCallable =
			new AbstractAsyncCallableWithResources<OperatorStateHandle>() {

				CheckpointStreamFactory.CheckpointStateOutputStream out = null;

				@Override
				protected void acquireResources() throws Exception {
					openOutStream();
				}

				@Override
				protected void releaseResources() throws Exception {
					closeOutStream();
				}

				@Override
				protected void stopOperation() throws Exception {
					closeOutStream();
				}

				private void openOutStream() throws Exception {
					out = streamFactory.createCheckpointStateOutputStream(checkpointId, timestamp);
					closeStreamOnCancelRegistry.registerCloseable(out);
				}

				private void closeOutStream() {
					if (closeStreamOnCancelRegistry.unregisterCloseable(out)) {
						IOUtils.closeQuietly(out);
					}
				}

				@Override
				public OperatorStateHandle performOperation() throws Exception {
					long asyncStartTime = System.currentTimeMillis();

					CheckpointStreamFactory.CheckpointStateOutputStream localOut = this.out;

					final Map<String, OperatorStateHandle.StateMetaInfo> writtenStatesMetaData =
						new HashMap<>(registeredStatesDeepCopies.size());

					List<RegisteredOperatorBackendStateMetaInfo.Snapshot<?>> metaInfoSnapshots =
						new ArrayList<>(registeredStatesDeepCopies.size());

					for (Map.Entry<String, PartitionableListState<?>> entry : registeredStatesDeepCopies.entrySet()) {
						metaInfoSnapshots.add(entry.getValue().getStateMetaInfo().snapshot());
					}

					DataOutputView dov = new DataOutputViewStreamWrapper(localOut);

					OperatorBackendSerializationProxy backendSerializationProxy =
						new OperatorBackendSerializationProxy(metaInfoSnapshots);

					backendSerializationProxy.write(dov);

					dov.writeInt(registeredStatesDeepCopies.size());

					for (Map.Entry<String, PartitionableListState<?>> entry :
						registeredStatesDeepCopies.entrySet()) {

						PartitionableListState<?> value = entry.getValue();
						long[] partitionOffsets = value.write(localOut);
						OperatorStateHandle.Mode mode = value.getStateMetaInfo().getAssignmentMode();
						writtenStatesMetaData.put(
							entry.getKey(),
							new OperatorStateHandle.StateMetaInfo(partitionOffsets, mode));
					}

					OperatorStateHandle retValue = null;

					if (closeStreamOnCancelRegistry.unregisterCloseable(out)) {

						StreamStateHandle stateHandle = out.closeAndGetHandle();

						if (stateHandle != null) {
							retValue = new OperatorStateHandle(writtenStatesMetaData, stateHandle);
						}
					}

					if (asynchronousSnapshots) {
						LOG.info("DefaultOperatorStateBackend snapshot ({}, asynchronous part) in thread {} took {} ms.",
							streamFactory, Thread.currentThread(), (System.currentTimeMillis() - asyncStartTime));
					}

					return retValue;
				}
			};

		AsyncStoppableTaskWithCallback<OperatorStateHandle> task = AsyncStoppableTaskWithCallback.from(ioCallable);

		if (!asynchronousSnapshots) {
			task.run();
		}

		LOG.info("DefaultOperatorStateBackend snapshot ({}, synchronous part) in thread {} took {} ms.",
				streamFactory, Thread.currentThread(), (System.currentTimeMillis() - syncStartTime));

		return task;
	}

	@Override
	public void restore(Collection<OperatorStateHandle> restoreSnapshots) throws Exception {

		if (null == restoreSnapshots) {
			return;
		}

		for (OperatorStateHandle stateHandle : restoreSnapshots) {

			if (stateHandle == null) {
				continue;
			}

			FSDataInputStream in = stateHandle.openInputStream();
			closeStreamOnCancelRegistry.registerCloseable(in);

			ClassLoader restoreClassLoader = Thread.currentThread().getContextClassLoader();

			try {
				Thread.currentThread().setContextClassLoader(userClassloader);
				OperatorBackendSerializationProxy backendSerializationProxy =
						new OperatorBackendSerializationProxy(userClassloader);

				backendSerializationProxy.read(new DataInputViewStreamWrapper(in));

				List<RegisteredOperatorBackendStateMetaInfo.Snapshot<?>> restoredMetaInfoSnapshots =
						backendSerializationProxy.getStateMetaInfoSnapshots();

				// Recreate all PartitionableListStates from the meta info
				for (RegisteredOperatorBackendStateMetaInfo.Snapshot<?> restoredMetaInfo : restoredMetaInfoSnapshots) {

					if (restoredMetaInfo.getPartitionStateSerializer() == null ||
							restoredMetaInfo.getPartitionStateSerializer() instanceof UnloadableDummyTypeSerializer) {

						// must fail now if the previous serializer cannot be restored because there is no serializer
						// capable of reading previous state
						// TODO when eager state registration is in place, we can try to get a convert deserializer
						// TODO from the newly registered serializer instead of simply failing here

						throw new IOException("Unable to restore operator state [" + restoredMetaInfo.getName() + "]." +
							" The previous serializer of the operator state must be present; the serializer could" +
							" have been removed from the classpath, or its implementation have changed and could" +
							" not be loaded. This is a temporary restriction that will be fixed in future versions.");
					}

					restoredStateMetaInfos.put(restoredMetaInfo.getName(), restoredMetaInfo);

					PartitionableListState<?> listState = registeredStates.get(restoredMetaInfo.getName());

					if (null == listState) {
						listState = new PartitionableListState<>(
								new RegisteredOperatorBackendStateMetaInfo<>(
										restoredMetaInfo.getName(),
										restoredMetaInfo.getPartitionStateSerializer(),
										restoredMetaInfo.getAssignmentMode()));

						registeredStates.put(listState.getStateMetaInfo().getName(), listState);
					} else {
						// TODO with eager state registration in place, check here for serializer migration strategies
					}
				}

				// Restore all the state in PartitionableListStates
				for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> nameToOffsets :
						stateHandle.getStateNameToPartitionOffsets().entrySet()) {

					PartitionableListState<?> stateListForName = registeredStates.get(nameToOffsets.getKey());

					Preconditions.checkState(null != stateListForName, "Found state without " +
							"corresponding meta info: " + nameToOffsets.getKey());

					deserializeStateValues(stateListForName, in, nameToOffsets.getValue());
				}

			} finally {
				Thread.currentThread().setContextClassLoader(restoreClassLoader);
				if (closeStreamOnCancelRegistry.unregisterCloseable(in)) {
					IOUtils.closeQuietly(in);
				}
			}
		}
	}

	/**
	 * Implementation of operator list state.
	 *
	 * @param <S> the type of an operator state partition.
	 */
	static final class PartitionableListState<S> implements ListState<S> {

		/**
		 * Meta information of the state, including state name, assignment mode, and serializer
		 */
		private RegisteredOperatorBackendStateMetaInfo<S> stateMetaInfo;

		/**
		 * The internal list the holds the elements of the state
		 */
		private final ArrayList<S> internalList;

		/**
		 * A serializer that allows to perfom deep copies of internalList
		 */
		private final ArrayListSerializer<S> internalListCopySerializer;

		public PartitionableListState(RegisteredOperatorBackendStateMetaInfo<S> stateMetaInfo) {
			this(stateMetaInfo, new ArrayList<S>());
		}

		private PartitionableListState(
				RegisteredOperatorBackendStateMetaInfo<S> stateMetaInfo,
				ArrayList<S> internalList) {

			this.stateMetaInfo = Preconditions.checkNotNull(stateMetaInfo);
			this.internalList = Preconditions.checkNotNull(internalList);
			this.internalListCopySerializer = new ArrayListSerializer<>(stateMetaInfo.getPartitionStateSerializer());
		}

		private PartitionableListState(PartitionableListState<S> toCopy) {

			this(toCopy.stateMetaInfo, toCopy.internalListCopySerializer.copy(toCopy.internalList));
		}

		public void setStateMetaInfo(RegisteredOperatorBackendStateMetaInfo<S> stateMetaInfo) {
			this.stateMetaInfo = stateMetaInfo;
		}

		public RegisteredOperatorBackendStateMetaInfo<S> getStateMetaInfo() {
			return stateMetaInfo;
		}

		public PartitionableListState<S> deepCopy() {
			return new PartitionableListState<>(this);
		}

		@Override
		public void clear() {
			internalList.clear();
		}

		@Override
		public Iterable<S> get() {
			return internalList;
		}

		@Override
		public void add(S value) {
			internalList.add(value);
		}

		@Override
		public String toString() {
			return "PartitionableListState{" +
					"stateMetaInfo=" + stateMetaInfo +
					", internalList=" + internalList +
					'}';
		}

		public long[] write(FSDataOutputStream out) throws IOException {

			long[] partitionOffsets = new long[internalList.size()];

			DataOutputView dov = new DataOutputViewStreamWrapper(out);

			for (int i = 0; i < internalList.size(); ++i) {
				S element = internalList.get(i);
				partitionOffsets[i] = out.getPos();
				getStateMetaInfo().getPartitionStateSerializer().serialize(element, dov);
			}

			return partitionOffsets;
		}
	}

	private <S> ListState<S> getListState(
			ListStateDescriptor<S> stateDescriptor,
			OperatorStateHandle.Mode mode) throws IOException, StateMigrationException {

		Preconditions.checkNotNull(stateDescriptor);
		String name = Preconditions.checkNotNull(stateDescriptor.getName());

		@SuppressWarnings("unchecked")
		PartitionableListState<S> previous = (PartitionableListState<S>) accessedStatesByName.get(name);
		if (previous != null) {
			checkStateNameAndMode(previous.getStateMetaInfo(), name, mode);
			return previous;
		}

		// end up here if its the first time access after execution for the
		// provided state name; check compatibility of restored state, if any
		// TODO with eager registration in place, these checks should be moved to restore()

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());
		TypeSerializer<S> partitionStateSerializer = Preconditions.checkNotNull(stateDescriptor.getElementSerializer());

		@SuppressWarnings("unchecked")
		PartitionableListState<S> partitionableListState = (PartitionableListState<S>) registeredStates.get(name);

		if (null == partitionableListState) {
			// no restored state for the state name; simply create new state holder

			partitionableListState = new PartitionableListState<>(
				new RegisteredOperatorBackendStateMetaInfo<>(
					name,
					partitionStateSerializer,
					mode));

			registeredStates.put(name, partitionableListState);
		} else {
			// has restored state; check compatibility of new state access

			checkStateNameAndMode(partitionableListState.getStateMetaInfo(), name, mode);

			@SuppressWarnings("unchecked")
			RegisteredOperatorBackendStateMetaInfo.Snapshot<S> restoredMetaInfo =
				(RegisteredOperatorBackendStateMetaInfo.Snapshot<S>) restoredStateMetaInfos.get(name);

			// check compatibility to determine if state migration is required
			CompatibilityResult<S> stateCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					restoredMetaInfo.getPartitionStateSerializer(),
					UnloadableDummyTypeSerializer.class,
					restoredMetaInfo.getPartitionStateSerializerConfigSnapshot(),
					partitionStateSerializer);

			if (!stateCompatibility.isRequiresMigration()) {
				// new serializer is compatible; use it to replace the old serializer
				partitionableListState.setStateMetaInfo(
					new RegisteredOperatorBackendStateMetaInfo<>(name, partitionStateSerializer, mode));
			} else {
				// TODO state migration currently isn't possible.

				// NOTE: for heap backends, it is actually fine to proceed here without failing the restore,
				// since the state has already been deserialized to objects and we can just continue with
				// the new serializer; we're deliberately failing here for now to have equal functionality with
				// the RocksDB backend to avoid confusion for users.

				throw new StateMigrationException("State migration isn't supported, yet.");
			}
		}

		accessedStatesByName.put(name, partitionableListState);
		return partitionableListState;
	}

	private static <S> void deserializeStateValues(
		PartitionableListState<S> stateListForName,
		FSDataInputStream in,
		OperatorStateHandle.StateMetaInfo metaInfo) throws IOException {

		if (null != metaInfo) {
			long[] offsets = metaInfo.getOffsets();
			if (null != offsets) {
				DataInputView div = new DataInputViewStreamWrapper(in);
				TypeSerializer<S> serializer = stateListForName.getStateMetaInfo().getPartitionStateSerializer();
				for (long offset : offsets) {
					in.seek(offset);
					stateListForName.add(serializer.deserialize(div));
				}
			}
		}
	}

	private static void checkStateNameAndMode(
			RegisteredOperatorBackendStateMetaInfo previousMetaInfo,
			String expectedName,
			OperatorStateHandle.Mode expectedMode) {

		Preconditions.checkState(
			previousMetaInfo.getName().equals(expectedName),
			"Incompatible state names. " +
				"Was [" + previousMetaInfo.getName() + "], " +
				"registered with [" + expectedName + "].");

		Preconditions.checkState(
			previousMetaInfo.getAssignmentMode().equals(expectedMode),
			"Incompatible state assignment modes. " +
				"Was [" + previousMetaInfo.getAssignmentMode() + "], " +
				"registered with [" + expectedMode + "].");
	}
}
