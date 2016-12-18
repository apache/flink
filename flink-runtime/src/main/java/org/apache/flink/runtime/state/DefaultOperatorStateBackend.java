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
import org.apache.flink.util.Preconditions;

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
public class DefaultOperatorStateBackend implements OperatorStateBackend {

	/** The default namespace for state in cases where no state name is provided */
	public static final String DEFAULT_OPERATOR_STATE_NAME = "_default_";
	
	private final Map<String, PartitionableListState<?>> registeredStates;
	private final Collection<OperatorStateHandle> restoreSnapshots;
	private final CloseableRegistry closeStreamOnCancelRegistry;
	private final JavaSerializer<Serializable> javaSerializer;
	private final ClassLoader userClassloader;

	/**
	 * Restores a OperatorStateStore (lazily) using the provided snapshots.
	 *
	 * @param restoreSnapshots snapshots that are available to restore partitionable states on request.
	 */
	public DefaultOperatorStateBackend(
			ClassLoader userClassLoader,
			Collection<OperatorStateHandle> restoreSnapshots) throws IOException {

		this.userClassloader = Preconditions.checkNotNull(userClassLoader);
		this.javaSerializer = new JavaSerializer<>();
		this.registeredStates = new HashMap<>();
		this.closeStreamOnCancelRegistry = new CloseableRegistry();
		this.restoreSnapshots = restoreSnapshots;
		restoreState();
	}

	/**
	 * Creates an empty OperatorStateStore.
	 */
	public DefaultOperatorStateBackend(ClassLoader userClassLoader) throws IOException {
		this(userClassLoader, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Serializable> ListState<T> getSerializableListState(String stateName) throws Exception {
		return (ListState<T>) getOperatorState(new ListStateDescriptor<>(stateName, javaSerializer));
	}
	
	@Override
	public <S> ListState<S> getOperatorState(ListStateDescriptor<S> stateDescriptor) throws IOException {

		Preconditions.checkNotNull(stateDescriptor);

		String name = Preconditions.checkNotNull(stateDescriptor.getName());
		TypeSerializer<S> partitionStateSerializer = Preconditions.checkNotNull(stateDescriptor.getSerializer());

		@SuppressWarnings("unchecked")
		PartitionableListState<S> partitionableListState = (PartitionableListState<S>) registeredStates.get(name);

		if (null == partitionableListState) {

			partitionableListState = new PartitionableListState<>(name, partitionStateSerializer);
			registeredStates.put(name, partitionableListState);
		} else {
			Preconditions.checkState(
					partitionableListState.getPartitionStateSerializer().
							isCompatibleWith(stateDescriptor.getSerializer()),
					"Incompatible type serializers. Provided: " + stateDescriptor.getSerializer() +
							", found: " + partitionableListState.getPartitionStateSerializer());
		}

		return partitionableListState;
	}

	private void restoreState() throws IOException {

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
								stateMetaInfo.getStateSerializer());

						registeredStates.put(listState.getName(), listState);
					} else {
						Preconditions.checkState(listState.getPartitionStateSerializer().isCompatibleWith(
								stateMetaInfo.getStateSerializer()), "Incompatible state serializers found: " +
								listState.getPartitionStateSerializer() + " is not compatible with " +
								stateMetaInfo.getStateSerializer());
					}
				}

				// Restore all the state in PartitionableListStates
				for (Map.Entry<String, long[]> nameToOffsets : stateHandle.getStateNameToPartitionOffsets().entrySet()) {
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

	private static <S> void deserializeStateValues(
			PartitionableListState<S> stateListForName,
			FSDataInputStream in,
			long[] offsets) throws IOException {

		DataInputView div = new DataInputViewStreamWrapper(in);
		TypeSerializer<S> serializer = stateListForName.getPartitionStateSerializer();
		for (long offset : offsets) {
			in.seek(offset);
			stateListForName.add(serializer.deserialize(div));
		}
	}
	
	@Override
	public RunnableFuture<OperatorStateHandle> snapshot(
			long checkpointId, long timestamp, CheckpointStreamFactory streamFactory) throws Exception {

		if (registeredStates.isEmpty()) {
			return new DoneFuture<>(null);
		}

		List<OperatorBackendSerializationProxy.StateMetaInfo<?>> metaInfoList =
				new ArrayList<>(registeredStates.size());

		for (Map.Entry<String, PartitionableListState<?>> entry : registeredStates.entrySet()) {
			PartitionableListState<?> state = entry.getValue();
			OperatorBackendSerializationProxy.StateMetaInfo<?> metaInfo =
					new OperatorBackendSerializationProxy.StateMetaInfo<>(
							state.getName(),
							state.getPartitionStateSerializer());
			metaInfoList.add(metaInfo);
		}

		Map<String, long[]> writtenStatesMetaData = new HashMap<>(registeredStates.size());

		CheckpointStreamFactory.CheckpointStateOutputStream out = streamFactory.
				createCheckpointStateOutputStream(checkpointId, timestamp);

		try {
			closeStreamOnCancelRegistry.registerClosable(out);

			DataOutputView dov = new DataOutputViewStreamWrapper(out);

			OperatorBackendSerializationProxy backendSerializationProxy =
					new OperatorBackendSerializationProxy(metaInfoList);

			backendSerializationProxy.write(dov);

			dov.writeInt(registeredStates.size());
			for (Map.Entry<String, PartitionableListState<?>> entry : registeredStates.entrySet()) {

				long[] partitionOffsets = entry.getValue().write(out);
				writtenStatesMetaData.put(entry.getKey(), partitionOffsets);
			}

			OperatorStateHandle handle = new OperatorStateHandle(writtenStatesMetaData, out.closeAndGetHandle());

			return new DoneFuture<>(handle);
		} finally {
			closeStreamOnCancelRegistry.unregisterClosable(out);
			out.close();
		}
	}

	@Override
	public void dispose() {
		registeredStates.clear();
	}

	@Override
	public Set<String> getRegisteredStateNames() {
		return registeredStates.keySet();
	}

	@Override
	public void close() throws IOException {
		closeStreamOnCancelRegistry.close();
	}

	static final class PartitionableListState<S> implements ListState<S> {

		private final List<S> internalList;
		private final String name;
		private final TypeSerializer<S> partitionStateSerializer;

		public PartitionableListState(String name, TypeSerializer<S> partitionStateSerializer) {
			this.internalList = new ArrayList<>();
			this.partitionStateSerializer = Preconditions.checkNotNull(partitionStateSerializer);
			this.name = Preconditions.checkNotNull(name);
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

		public List<S> getInternalList() {
			return internalList;
		}

		public String getName() {
			return name;
		}

		public TypeSerializer<S> getPartitionStateSerializer() {
			return partitionStateSerializer;
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
					"listState=" + internalList +
					'}';
		}
	}
}

