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

import org.apache.commons.io.IOUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.AbstractAsyncSnapshotIOCallable;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.util.Preconditions;
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
		final AbstractAsyncSnapshotIOCallable<OperatorStateHandle> ioCallable =
			new AbstractAsyncSnapshotIOCallable<OperatorStateHandle>(
				checkpointId,
				timestamp,
				streamFactory,
				closeStreamOnCancelRegistry) {

				@Override
				public OperatorStateHandle performOperation() throws Exception {
					long asyncStartTime = System.currentTimeMillis();

					final Map<String, OperatorStateHandle.StateMetaInfo> writtenStatesMetaData =
						new HashMap<>(registeredStatesDeepCopies.size());

					List<RegisteredOperatorBackendStateMetaInfo.Snapshot<?>> metaInfoSnapshots =
						new ArrayList<>(registeredStatesDeepCopies.size());

					for (Map.Entry<String, PartitionableListState<?>> entry : registeredStatesDeepCopies.entrySet()) {
						metaInfoSnapshots.add(entry.getValue().getStateMetaInfo().snapshot());
					}

					CheckpointStreamFactory.CheckpointStateOutputStream out = getIoHandle();
					DataOutputView dov = new DataOutputViewStreamWrapper(out);

					OperatorBackendSerializationProxy backendSerializationProxy =
						new OperatorBackendSerializationProxy(metaInfoSnapshots);

					backendSerializationProxy.write(dov);

					dov.writeInt(registeredStatesDeepCopies.size());

					for (Map.Entry<String, PartitionableListState<?>> entry :
						registeredStatesDeepCopies.entrySet()) {

						PartitionableListState<?> value = entry.getValue();
						long[] partitionOffsets = value.write(out);
						OperatorStateHandle.Mode mode = value.getStateMetaInfo().getAssignmentMode();
						writtenStatesMetaData.put(
							entry.getKey(),
							new OperatorStateHandle.StateMetaInfo(partitionOffsets, mode));
					}

					StreamStateHandle stateHandle = closeStreamAndGetStateHandle();

					if (asynchronousSnapshots) {
						LOG.info("DefaultOperatorStateBackend snapshot ({}, asynchronous part) in thread {} took {} ms.",
							streamFactory, Thread.currentThread(), (System.currentTimeMillis() - asyncStartTime));
					}

					if (stateHandle == null) {
						return null;
					}

					return new OperatorStateHandle(writtenStatesMetaData, stateHandle);
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
			closeStreamOnCancelRegistry.registerClosable(in);

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
				closeStreamOnCancelRegistry.unregisterClosable(in);
				IOUtils.closeQuietly(in);
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
		private final RegisteredOperatorBackendStateMetaInfo<S> stateMetaInfo;

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

		public RegisteredOperatorBackendStateMetaInfo<S> getStateMetaInfo() {
			return stateMetaInfo;
		}

		public List<S> getInternalList() {
			return internalList;
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
		OperatorStateHandle.Mode mode) throws IOException {
		Preconditions.checkNotNull(stateDescriptor);

		stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());

		String name = Preconditions.checkNotNull(stateDescriptor.getName());
		TypeSerializer<S> partitionStateSerializer = Preconditions.checkNotNull(stateDescriptor.getElementSerializer());

		@SuppressWarnings("unchecked")
		PartitionableListState<S> partitionableListState = (PartitionableListState<S>) registeredStates.get(name);

		if (null == partitionableListState) {
			partitionableListState = new PartitionableListState<>(
				new RegisteredOperatorBackendStateMetaInfo<>(
					name,
					partitionStateSerializer,
					mode));

			registeredStates.put(name, partitionableListState);
		} else {
			// TODO with eager registration in place, these checks should be moved to restore()

			Preconditions.checkState(
				partitionableListState.getStateMetaInfo().getName().equals(name),
				"Incompatible state names. " +
					"Was [" + partitionableListState.getStateMetaInfo().getName() + "], " +
					"registered with [" + name + "].");

			Preconditions.checkState(
				partitionableListState.getStateMetaInfo().getAssignmentMode().equals(mode),
				"Incompatible state assignment modes. " +
					"Was [" + partitionableListState.getStateMetaInfo().getAssignmentMode() + "], " +
					"registered with [" + mode + "].");
		}

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
}
