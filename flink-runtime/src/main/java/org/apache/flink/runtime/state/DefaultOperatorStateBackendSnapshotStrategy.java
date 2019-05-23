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

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

/**
 * Snapshot strategy for this backend.
 */
class DefaultOperatorStateBackendSnapshotStrategy extends AbstractSnapshotStrategy<OperatorStateHandle> {
	private final ClassLoader userClassLoader;
	private final boolean asynchronousSnapshots;
	private final Map<String, PartitionableListState<?>> registeredOperatorStates;
	private final Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStates;
	private final CloseableRegistry closeStreamOnCancelRegistry;

	protected DefaultOperatorStateBackendSnapshotStrategy(
		ClassLoader userClassLoader,
		boolean asynchronousSnapshots,
		Map<String, PartitionableListState<?>> registeredOperatorStates,
		Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStates,
		CloseableRegistry closeStreamOnCancelRegistry) {
		super("DefaultOperatorStateBackend snapshot");
		this.userClassLoader = userClassLoader;
		this.asynchronousSnapshots = asynchronousSnapshots;
		this.registeredOperatorStates = registeredOperatorStates;
		this.registeredBroadcastStates = registeredBroadcastStates;
		this.closeStreamOnCancelRegistry = closeStreamOnCancelRegistry;
	}

	@Nonnull
	@Override
	public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
		final long checkpointId,
		final long timestamp,
		@Nonnull final CheckpointStreamFactory streamFactory,
		@Nonnull final CheckpointOptions checkpointOptions) throws IOException {

		if (registeredOperatorStates.isEmpty() && registeredBroadcastStates.isEmpty()) {
			return DoneFuture.of(SnapshotResult.empty());
		}

		final Map<String, PartitionableListState<?>> registeredOperatorStatesDeepCopies =
			new HashMap<>(registeredOperatorStates.size());
		final Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStatesDeepCopies =
			new HashMap<>(registeredBroadcastStates.size());

		ClassLoader snapshotClassLoader = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(userClassLoader);
		try {
			// eagerly create deep copies of the list and the broadcast states (if any)
			// in the synchronous phase, so that we can use them in the async writing.

			if (!registeredOperatorStates.isEmpty()) {
				for (Map.Entry<String, PartitionableListState<?>> entry : registeredOperatorStates.entrySet()) {
					PartitionableListState<?> listState = entry.getValue();
					if (null != listState) {
						listState = listState.deepCopy();
					}
					registeredOperatorStatesDeepCopies.put(entry.getKey(), listState);
				}
			}

			if (!registeredBroadcastStates.isEmpty()) {
				for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry : registeredBroadcastStates.entrySet()) {
					BackendWritableBroadcastState<?, ?> broadcastState = entry.getValue();
					if (null != broadcastState) {
						broadcastState = broadcastState.deepCopy();
					}
					registeredBroadcastStatesDeepCopies.put(entry.getKey(), broadcastState);
				}
			}
		} finally {
			Thread.currentThread().setContextClassLoader(snapshotClassLoader);
		}

		AsyncSnapshotCallable<SnapshotResult<OperatorStateHandle>> snapshotCallable =
			new AsyncSnapshotCallable<SnapshotResult<OperatorStateHandle>>() {

				@Override
				protected SnapshotResult<OperatorStateHandle> callInternal() throws Exception {

					CheckpointStreamFactory.CheckpointStateOutputStream localOut =
						streamFactory.createCheckpointStateOutputStream(CheckpointedStateScope.EXCLUSIVE);
					snapshotCloseableRegistry.registerCloseable(localOut);

					// get the registered operator state infos ...
					List<StateMetaInfoSnapshot> operatorMetaInfoSnapshots =
						new ArrayList<>(registeredOperatorStatesDeepCopies.size());

					for (Map.Entry<String, PartitionableListState<?>> entry :
						registeredOperatorStatesDeepCopies.entrySet()) {
						operatorMetaInfoSnapshots.add(entry.getValue().getStateMetaInfo().snapshot());
					}

					// ... get the registered broadcast operator state infos ...
					List<StateMetaInfoSnapshot> broadcastMetaInfoSnapshots =
						new ArrayList<>(registeredBroadcastStatesDeepCopies.size());

					for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry :
						registeredBroadcastStatesDeepCopies.entrySet()) {
						broadcastMetaInfoSnapshots.add(entry.getValue().getStateMetaInfo().snapshot());
					}

					// ... write them all in the checkpoint stream ...
					DataOutputView dov = new DataOutputViewStreamWrapper(localOut);

					OperatorBackendSerializationProxy backendSerializationProxy =
						new OperatorBackendSerializationProxy(operatorMetaInfoSnapshots, broadcastMetaInfoSnapshots);

					backendSerializationProxy.write(dov);

					// ... and then go for the states ...

					// we put BOTH normal and broadcast state metadata here
					int initialMapCapacity =
						registeredOperatorStatesDeepCopies.size() + registeredBroadcastStatesDeepCopies.size();
					final Map<String, OperatorStateHandle.StateMetaInfo> writtenStatesMetaData =
						new HashMap<>(initialMapCapacity);

					for (Map.Entry<String, PartitionableListState<?>> entry :
						registeredOperatorStatesDeepCopies.entrySet()) {

						PartitionableListState<?> value = entry.getValue();
						long[] partitionOffsets = value.write(localOut);
						OperatorStateHandle.Mode mode = value.getStateMetaInfo().getAssignmentMode();
						writtenStatesMetaData.put(
							entry.getKey(),
							new OperatorStateHandle.StateMetaInfo(partitionOffsets, mode));
					}

					// ... and the broadcast states themselves ...
					for (Map.Entry<String, BackendWritableBroadcastState<?, ?>> entry :
						registeredBroadcastStatesDeepCopies.entrySet()) {

						BackendWritableBroadcastState<?, ?> value = entry.getValue();
						long[] partitionOffsets = {value.write(localOut)};
						OperatorStateHandle.Mode mode = value.getStateMetaInfo().getAssignmentMode();
						writtenStatesMetaData.put(
							entry.getKey(),
							new OperatorStateHandle.StateMetaInfo(partitionOffsets, mode));
					}

					// ... and, finally, create the state handle.
					OperatorStateHandle retValue = null;

					if (snapshotCloseableRegistry.unregisterCloseable(localOut)) {

						StreamStateHandle stateHandle = localOut.closeAndGetHandle();

						if (stateHandle != null) {
							retValue = new OperatorStreamStateHandle(writtenStatesMetaData, stateHandle);
						}

						return SnapshotResult.of(retValue);
					} else {
						throw new IOException("Stream was already unregistered.");
					}
				}

				@Override
				protected void cleanupProvidedResources() {
					// nothing to do
				}

				@Override
				protected void logAsyncSnapshotComplete(long startTime) {
					if (asynchronousSnapshots) {
						logAsyncCompleted(streamFactory, startTime);
					}
				}
			};

		final FutureTask<SnapshotResult<OperatorStateHandle>> task =
			snapshotCallable.toAsyncSnapshotFutureTask(closeStreamOnCancelRegistry);

		if (!asynchronousSnapshots) {
			task.run();
		}

		return task;
	}
}
