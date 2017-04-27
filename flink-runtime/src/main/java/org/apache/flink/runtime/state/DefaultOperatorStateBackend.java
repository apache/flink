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
		for (Map.Entry<String, PartitionableListState<?>> entry : this.registeredStates.entrySet()) {

			PartitionableListState<?> listState = entry.getValue();
			if (null != listState) {
				listState = listState.deepCopy();
			}
			registeredStatesDeepCopies.put(entry.getKey(), listState);
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

					List<OperatorBackendSerializationProxy.StateMetaInfo<?>> metaInfoList =
						new ArrayList<>(registeredStatesDeepCopies.size());

					for (Map.Entry<String, PartitionableListState<?>> entry :
						registeredStatesDeepCopies.entrySet()) {

						PartitionableListState<?> state = entry.getValue();
						OperatorBackendSerializationProxy.StateMetaInfo<?> metaInfo =
							new OperatorBackendSerializationProxy.StateMetaInfo<>(
								state.getName(),
								state.getPartitionStateSerializer(),
								state.getAssignmentMode());
						metaInfoList.add(metaInfo);
					}

					CheckpointStreamFactory.CheckpointStateOutputStream out = getIoHandle();
					DataOutputView dov = new DataOutputViewStreamWrapper(out);

					OperatorBackendSerializationProxy backendSerializationProxy =
						new OperatorBackendSerializationProxy(metaInfoList);

					backendSerializationProxy.write(dov);

					dov.writeInt(registeredStatesDeepCopies.size());

					for (Map.Entry<String, PartitionableListState<?>> entry :
						registeredStatesDeepCopies.entrySet()) {

						PartitionableListState<?> value = entry.getValue();
						long[] partitionOffsets = value.write(out);
						OperatorStateHandle.Mode mode = value.getAssignmentMode();
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

					OperatorStateHandle operatorStateHandle =
						new OperatorStateHandle(writtenStatesMetaData, stateHandle);

					return operatorStateHandle;
				}
			};

		AsyncStoppableTaskWithCallback<OperatorStateHandle> task = AsyncStoppableTaskWithCallback.from(ioCallable);

		if (!asynchronousSnapshots) {
			task.run();
		}

		LOG.info("DefaultOperatorStateBackend snapshot (" + streamFactory + ", synchronous part) in thread " +
				Thread.currentThread() + " took " + (System.currentTimeMillis() - syncStartTime) + " ms.");

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

				List<OperatorBackendSerializationProxy.StateMetaInfo<?>> metaInfoList =
						backendSerializationProxy.getNamedStateSerializationProxies();

				// Recreate all PartitionableListStates from the meta info
				for (OperatorBackendSerializationProxy.StateMetaInfo<?> stateMetaInfo : metaInfoList) {
					PartitionableListState<?> listState = registeredStates.get(stateMetaInfo.getName());

					if (null == listState) {
						listState = new PartitionableListState<>(
								stateMetaInfo.getName(),
								stateMetaInfo.getStateSerializer(),
								stateMetaInfo.getMode());

						registeredStates.put(listState.getName(), listState);
					} else {
						Preconditions.checkState(listState.getPartitionStateSerializer().canRestoreFrom(
								stateMetaInfo.getStateSerializer()), "Incompatible state serializers found: " +
								listState.getPartitionStateSerializer() + " is not compatible with " +
								stateMetaInfo.getStateSerializer());
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
	 *
	 * Implementation of operator list state.
	 *
	 * @param <S> the type of an operator state partition.
	 */
	static final class PartitionableListState<S> implements ListState<S> {

		/**
		 * The name of the state, as registered by the user
		 */
		private final String name;

		/**
		 * The type serializer for the elements in the state list
		 */
		private final TypeSerializer<S> partitionStateSerializer;

		/**
		 * The mode how elements in this state are assigned to tasks during restore
		 */
		private final OperatorStateHandle.Mode assignmentMode;

		/**
		 * The internal list the holds the elements of the state
		 */
		private final ArrayList<S> internalList;

		/**
		 * A serializer that allows to perfom deep copies of internalList
		 */
		private final ArrayListSerializer<S> internalListCopySerializer;

		public PartitionableListState(
				String name,
				TypeSerializer<S> partitionStateSerializer,
				OperatorStateHandle.Mode assignmentMode) {

			this(name, partitionStateSerializer, assignmentMode, new ArrayList<S>());
		}

		private PartitionableListState(
				String name,
				TypeSerializer<S> partitionStateSerializer,
				OperatorStateHandle.Mode assignmentMode,
				ArrayList<S> internalList) {

			this.name = Preconditions.checkNotNull(name);
			this.partitionStateSerializer = Preconditions.checkNotNull(partitionStateSerializer);
			this.assignmentMode = Preconditions.checkNotNull(assignmentMode);
			this.internalList = Preconditions.checkNotNull(internalList);
			this.internalListCopySerializer = new ArrayListSerializer<>(partitionStateSerializer);
		}

		private PartitionableListState(PartitionableListState<S> toCopy) {

			this(
					toCopy.name,
					toCopy.partitionStateSerializer.duplicate(),
					toCopy.assignmentMode,
					toCopy.internalListCopySerializer.copy(toCopy.internalList));
		}

		public String getName() {
			return name;
		}

		public OperatorStateHandle.Mode getAssignmentMode() {
			return assignmentMode;
		}

		public TypeSerializer<S> getPartitionStateSerializer() {
			return partitionStateSerializer;
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
					"name='" + name + '\'' +
					", assignmentMode=" + assignmentMode +
					", internalList=" + internalList +
					'}';
		}

		public long[] write(FSDataOutputStream out) throws IOException {

			long[] partitionOffsets = new long[internalList.size()];

			DataOutputView dov = new DataOutputViewStreamWrapper(out);

			for (int i = 0; i < internalList.size(); ++i) {
				S element = internalList.get(i);
				partitionOffsets[i] = out.getPos();
				partitionStateSerializer.serialize(element, dov);
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
				name,
				partitionStateSerializer,
				mode);

			registeredStates.put(name, partitionableListState);
		} else {
			Preconditions.checkState(
				partitionableListState.getAssignmentMode().equals(mode),
				"Incompatible assignment mode. Provided: " + mode + ", expected: " +
					partitionableListState.getAssignmentMode());
			Preconditions.checkState(
				stateDescriptor.getElementSerializer().
					canRestoreFrom(partitionableListState.getPartitionStateSerializer()),
				"Incompatible type serializers. Provided: " + stateDescriptor.getElementSerializer() +
					", found: " + partitionableListState.getPartitionStateSerializer());
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
				TypeSerializer<S> serializer = stateListForName.getPartitionStateSerializer();
				for (long offset : offsets) {
					in.seek(offset);
					stateListForName.add(serializer.deserialize(div));
				}
			}
		}
	}
}
