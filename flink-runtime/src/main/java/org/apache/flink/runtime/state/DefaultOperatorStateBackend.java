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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.TypeSerializer;
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

	private final Map<String, PartitionableListState<?>> registeredStates;
	private final Collection<OperatorStateHandle> restoreSnapshots;
	private final ClosableRegistry closeStreamOnCancelRegistry;
	private final JavaSerializer<Serializable> javaSerializer;

	/**
	 * Restores a OperatorStateStore (lazily) using the provided snapshots.
	 *
	 * @param restoreSnapshots snapshots that are available to restore partitionable states on request.
	 */
	public DefaultOperatorStateBackend(
			ClassLoader userClassLoader,
			Collection<OperatorStateHandle> restoreSnapshots) {

		Preconditions.checkNotNull(userClassLoader);
		this.javaSerializer = new JavaSerializer<>(userClassLoader);
		this.restoreSnapshots = restoreSnapshots;
		this.registeredStates = new HashMap<>();
		this.closeStreamOnCancelRegistry = new ClosableRegistry();
	}

	/**
	 * Creates an empty OperatorStateStore.
	 */
	public DefaultOperatorStateBackend(ClassLoader userClassLoader) {
		this(userClassLoader, null);
	}

	@Override
	public ListState<Serializable> getSerializableListState(String stateName) throws Exception {
		return getOperatorState(new ListStateDescriptor<>(stateName, javaSerializer));
	}

	/**
	 * @see OperatorStateStore
	 */
	@Override
	public <S> ListState<S> getOperatorState(
			ListStateDescriptor<S> stateDescriptor) throws IOException {

		Preconditions.checkNotNull(stateDescriptor);

		String name = Preconditions.checkNotNull(stateDescriptor.getName());
		TypeSerializer<S> partitionStateSerializer = Preconditions.checkNotNull(stateDescriptor.getSerializer());

		@SuppressWarnings("unchecked")
		PartitionableListState<S> partitionableListState = (PartitionableListState<S>) registeredStates.get(name);

		if (null == partitionableListState) {

			partitionableListState = new PartitionableListState<>(partitionStateSerializer);

			registeredStates.put(name, partitionableListState);

			// Try to restore previous state if state handles to snapshots are provided
			if (restoreSnapshots != null) {
				for (OperatorStateHandle stateHandle : restoreSnapshots) {

					long[] offsets = stateHandle.getStateNameToPartitionOffsets().get(name);

					if (offsets != null) {

						FSDataInputStream in = stateHandle.openInputStream();
						try {
							closeStreamOnCancelRegistry.registerClosable(in);

							DataInputView div = new DataInputViewStreamWrapper(in);

							for (int i = 0; i < offsets.length; ++i) {

								in.seek(offsets[i]);
								S partitionState = partitionStateSerializer.deserialize(div);
								partitionableListState.add(partitionState);
							}
						} finally {
							closeStreamOnCancelRegistry.unregisterClosable(in);
							in.close();
						}
					}
				}
			}
		}

		return partitionableListState;
	}

	/**
	 * @see SnapshotProvider
	 */
	@Override
	public RunnableFuture<OperatorStateHandle> snapshot(
			long checkpointId, long timestamp, CheckpointStreamFactory streamFactory) throws Exception {

		if (registeredStates.isEmpty()) {
			return new DoneFuture<>(null);
		}

		Map<String, long[]> writtenStatesMetaData = new HashMap<>(registeredStates.size());

		CheckpointStreamFactory.CheckpointStateOutputStream out = streamFactory.
				createCheckpointStateOutputStream(checkpointId, timestamp);

		try {
			closeStreamOnCancelRegistry.registerClosable(out);

			DataOutputView dov = new DataOutputViewStreamWrapper(out);

			dov.writeInt(registeredStates.size());
			for (Map.Entry<String, PartitionableListState<?>> entry : registeredStates.entrySet()) {

				long[] partitionOffsets = entry.getValue().write(out);
				writtenStatesMetaData.put(entry.getKey(), partitionOffsets);
			}

			OperatorStateHandle handle = new OperatorStateHandle(out.closeAndGetHandle(), writtenStatesMetaData);

			return new DoneFuture<>(handle);
		} finally {
			closeStreamOnCancelRegistry.unregisterClosable(out);
			out.close();
		}
	}

	@Override
	public void dispose() {

	}

	static final class PartitionableListState<S> implements ListState<S> {

		private final List<S> listState;
		private final TypeSerializer<S> partitionStateSerializer;

		public PartitionableListState(TypeSerializer<S> partitionStateSerializer) {
			this.listState = new ArrayList<>();
			this.partitionStateSerializer = Preconditions.checkNotNull(partitionStateSerializer);
		}

		@Override
		public void clear() {
			listState.clear();
		}

		@Override
		public Iterable<S> get() {
			return listState;
		}

		@Override
		public void add(S value) {
			listState.add(value);
		}

		public long[] write(FSDataOutputStream out) throws IOException {

			long[] partitionOffsets = new long[listState.size()];

			DataOutputView dov = new DataOutputViewStreamWrapper(out);

			for (int i = 0; i < listState.size(); ++i) {
				S element = listState.get(i);
				partitionOffsets[i] = out.getPos();
				partitionStateSerializer.serialize(element, dov);
			}

			return partitionOffsets;
		}
	}

	@Override
	public Set<String> getRegisteredStateNames() {
		return registeredStates.keySet();
	}

	@Override
	public void close() throws IOException {
		closeStreamOnCancelRegistry.close();
	}
}

