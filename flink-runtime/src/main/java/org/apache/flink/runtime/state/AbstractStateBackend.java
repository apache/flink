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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateBackend;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.execution.Environment;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A state backend defines how state is stored and snapshotted during checkpoints.
 */
public abstract class AbstractStateBackend implements java.io.Serializable {
	
	private static final long serialVersionUID = 4620413814639220247L;

	protected transient TypeSerializer<?> keySerializer;

	protected transient ClassLoader userCodeClassLoader;

	protected transient Object currentKey;

	/** For efficient access in setCurrentKey() */
	private transient KvState<?, ?, ?, ?, ?>[] keyValueStates;

	/** So that we can give out state when the user uses the same key. */
	private transient HashMap<String, KvState<?, ?, ?, ?, ?>> keyValueStatesByName;

	/** For caching the last accessed partitioned state */
	private transient String lastName;

	@SuppressWarnings("rawtypes")
	private transient KvState lastState;

	// ------------------------------------------------------------------------
	//  initialization and cleanup
	// ------------------------------------------------------------------------

	/**
	 * This method is called by the task upon deployment to initialize the state backend for
	 * data for a specific job.
	 *
	 * @param env The {@link Environment} of the task that instantiated the state backend
	 * @param operatorIdentifier Unique identifier for naming states created by this backend
	 * @throws Exception Overwritten versions of this method may throw exceptions, in which
	 *                   case the job that uses the state backend is considered failed during
	 *                   deployment.
	 */
	public void initializeForJob(Environment env,
		String operatorIdentifier,
		TypeSerializer<?> keySerializer) throws Exception {
		this.userCodeClassLoader = env.getUserClassLoader();
		this.keySerializer = keySerializer;
	}

	/**
	 * Disposes all state associated with the current job.
	 *
	 * @throws Exception Exceptions may occur during disposal of the state and should be forwarded.
	 */
	public abstract void disposeAllStateForCurrentJob() throws Exception;

	/**
	 * Closes the state backend, releasing all internal resources, but does not delete any persistent
	 * checkpoint data.
	 *
	 * @throws Exception Exceptions can be forwarded and will be logged by the system
	 */
	public abstract void close() throws Exception;

	public void dispose() {
		if (keyValueStates != null) {
			for (KvState<?, ?, ?, ?, ?> state : keyValueStates) {
				state.dispose();
			}
		}
	}
	
	// ------------------------------------------------------------------------
	//  key/value state
	// ------------------------------------------------------------------------

	/**
	 * Creates and returns a new {@link ValueState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> The type of the value that the {@code ValueState} can store.
	 */
	protected abstract <N, T> ValueState<T> createValueState(TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link ListState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> The type of the values that the {@code ListState} can store.
	 */
	protected abstract <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link ReducingState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> The type of the values that the {@code ListState} can store.
	 */
	protected abstract <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer, ReducingStateDescriptor<T> stateDesc) throws Exception;

	/**
	 * Creates and returns a new {@link FoldingState}.
	 *
	 * @param namespaceSerializer TypeSerializer for the state namespace.
	 * @param stateDesc The {@code StateDescriptor} that contains the name of the state.
	 *
	 * @param <N> The type of the namespace.
	 * @param <T> Type of the values folded into the state
	 * @param <ACC> Type of the value in the state	 *
	 */
	abstract protected <N, T, ACC> FoldingState<T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer, FoldingStateDescriptor<T, ACC> stateDesc) throws Exception;

	/**
	 * Sets the current key that is used for partitioned state.
	 * @param currentKey The current key.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setCurrentKey(Object currentKey) {
		this.currentKey = currentKey;
		if (keyValueStates != null) {
			for (KvState kv : keyValueStates) {
				kv.setCurrentKey(currentKey);
			}
		}
	}

	public Object getCurrentKey() {
		return currentKey;
	}

	/**
	 * Creates or retrieves a partitioned state backed by this state backend.
	 *
	 * @param stateDescriptor The state identifier for the state. This contains name
	 *                           and can create a default state value.

	 * @param <N> The type of the namespace.
	 * @param <S> The type of the state.
	 *
	 * @return A new key/value state backed by this backend.
	 *
	 * @throws Exception Exceptions may occur during initialization of the state and should be forwarded.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public <N, S extends State> S getPartitionedState(final N namespace, final TypeSerializer<N> namespaceSerializer, final StateDescriptor<S, ?> stateDescriptor) throws Exception {

		if (keySerializer == null) {
			throw new Exception("State key serializer has not been configured in the config. " +
					"This operation cannot use partitioned state.");
		}
		
		if (!stateDescriptor.isSerializerInitialized()) {
			stateDescriptor.initializeSerializerUnlessSet(new ExecutionConfig());
		}

		if (keyValueStatesByName == null) {
			keyValueStatesByName = new HashMap<>();
		}

		if (lastName != null && lastName.equals(stateDescriptor.getName())) {
			lastState.setCurrentNamespace(namespace);
			return (S) lastState;
		}

		KvState<?, ?, ?, ?, ?> previous = keyValueStatesByName.get(stateDescriptor.getName());
		if (previous != null) {
			lastState = previous;
			lastState.setCurrentNamespace(namespace);
			lastName = stateDescriptor.getName();
			return (S) previous;
		}

		// create a new blank key/value state
		S kvstate = stateDescriptor.bind(new StateBackend() {
			@Override
			public <T> ValueState<T> createValueState(ValueStateDescriptor<T> stateDesc) throws Exception {
				return AbstractStateBackend.this.createValueState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T> ListState<T> createListState(ListStateDescriptor<T> stateDesc) throws Exception {
				return AbstractStateBackend.this.createListState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T> ReducingState<T> createReducingState(ReducingStateDescriptor<T> stateDesc) throws Exception {
				return AbstractStateBackend.this.createReducingState(namespaceSerializer, stateDesc);
			}

			@Override
			public <T, ACC> FoldingState<T, ACC> createFoldingState(FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
				return AbstractStateBackend.this.createFoldingState(namespaceSerializer, stateDesc);
			}

		});

		keyValueStatesByName.put(stateDescriptor.getName(), (KvState) kvstate);
		keyValueStates = keyValueStatesByName.values().toArray(new KvState[keyValueStatesByName.size()]);

		lastName = stateDescriptor.getName();
		lastState = (KvState<?, ?, ?, ?, ?>) kvstate;

		((KvState) kvstate).setCurrentKey(currentKey);
		((KvState) kvstate).setCurrentNamespace(namespace);

		return kvstate;
	}

	public HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> snapshotPartitionedState(long checkpointId, long timestamp) throws Exception {
		if (keyValueStates != null) {
			HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> snapshots = new HashMap<>(keyValueStatesByName.size());

			for (Map.Entry<String, KvState<?, ?, ?, ?, ?>> entry : keyValueStatesByName.entrySet()) {
				KvStateSnapshot<?, ?, ?, ?, ?> snapshot = entry.getValue().snapshot(checkpointId, timestamp);
				snapshots.put(entry.getKey(), snapshot);
			}
			return snapshots;
		}

		return null;
	}

	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		// We check whether the KvStates require notifications
		if (keyValueStates != null) {
			for (KvState<?, ?, ?, ?, ?> kvstate : keyValueStates) {
				if (kvstate instanceof CheckpointListener) {
					((CheckpointListener) kvstate).notifyCheckpointComplete(checkpointId);
				}
			}
		}
	}

	/**
	 * Injects K/V state snapshots for lazy restore.
	 * @param keyValueStateSnapshots The Map of snapshots
	 */
	@SuppressWarnings("unchecked,rawtypes")
	public final void injectKeyValueStateSnapshots(HashMap<String, KvStateSnapshot> keyValueStateSnapshots, long recoveryTimestamp) throws Exception {
		if (keyValueStateSnapshots != null) {
			if (keyValueStatesByName == null) {
				keyValueStatesByName = new HashMap<>();
			}

			for (Map.Entry<String, KvStateSnapshot> state : keyValueStateSnapshots.entrySet()) {
				KvState kvState = state.getValue().restoreState(this,
					keySerializer,
					userCodeClassLoader,
					recoveryTimestamp);
				keyValueStatesByName.put(state.getKey(), kvState);
			}
			keyValueStates = keyValueStatesByName.values().toArray(new KvState[keyValueStatesByName.size()]);
		}
	}

	// ------------------------------------------------------------------------
	//  storing state for a checkpoint
	// ------------------------------------------------------------------------

	/**
	 * Creates an output stream that writes into the state of the given checkpoint. When the stream
	 * is closes, it returns a state handle that can retrieve the state back.
	 *
	 * @param checkpointID The ID of the checkpoint.
	 * @param timestamp The timestamp of the checkpoint.
	 * @return An output stream that writes state for the given checkpoint.
	 *
	 * @throws Exception Exceptions may occur while creating the stream and should be forwarded.
	 */
	public abstract CheckpointStateOutputStream createCheckpointStateOutputStream(
			long checkpointID, long timestamp) throws Exception;

	/**
	 * Creates a {@link DataOutputView} stream that writes into the state of the given checkpoint.
	 * When the stream is closes, it returns a state handle that can retrieve the state back.
	 *
	 * @param checkpointID The ID of the checkpoint.
	 * @param timestamp The timestamp of the checkpoint.
	 * @return An DataOutputView stream that writes state for the given checkpoint.
	 *
	 * @throws Exception Exceptions may occur while creating the stream and should be forwarded.
	 */
	public CheckpointStateOutputView createCheckpointStateOutputView(
			long checkpointID, long timestamp) throws Exception {
		return new CheckpointStateOutputView(createCheckpointStateOutputStream(checkpointID, timestamp));
	}

	/**
	 * Writes the given state into the checkpoint, and returns a handle that can retrieve the state back.
	 *
	 * @param state The state to be checkpointed.
	 * @param checkpointID The ID of the checkpoint.
	 * @param timestamp The timestamp of the checkpoint.
	 * @param <S> The type of the state.
	 *
	 * @return A state handle that can retrieve the checkpoined state.
	 *
	 * @throws Exception Exceptions may occur during serialization / storing the state and should be forwarded.
	 */
	public abstract <S extends Serializable> StateHandle<S> checkpointStateSerializable(
			S state, long checkpointID, long timestamp) throws Exception;


	// ------------------------------------------------------------------------
	//  Checkpoint state output stream
	// ------------------------------------------------------------------------

	/**
	 * A dedicated output stream that produces a {@link StreamStateHandle} when closed.
	 */
	public static abstract class CheckpointStateOutputStream extends OutputStream {

		/**
		 * Closes the stream and gets a state handle that can create an input stream
		 * producing the data written to this stream.
		 *
		 * @return A state handle that can create an input stream producing the data written to this stream.
		 * @throws IOException Thrown, if the stream cannot be closed.
		 */
		public abstract StreamStateHandle closeAndGetHandle() throws IOException;
	}

	/**
	 * A dedicated DataOutputView stream that produces a {@code StateHandle<DataInputView>} when closed.
	 */
	public static final class CheckpointStateOutputView extends DataOutputViewStreamWrapper {

		private final CheckpointStateOutputStream out;

		public CheckpointStateOutputView(CheckpointStateOutputStream out) {
			super(out);
			this.out = out;
		}

		/**
		 * Closes the stream and gets a state handle that can create a DataInputView.
		 * producing the data written to this stream.
		 *
		 * @return A state handle that can create an input stream producing the data written to this stream.
		 * @throws IOException Thrown, if the stream cannot be closed.
		 */
		public StateHandle<DataInputView> closeAndGetHandle() throws IOException {
			return new DataInputViewHandle(out.closeAndGetHandle());
		}

		@Override
		public void close() throws IOException {
			out.close();
		}
	}

	/**
	 * Simple state handle that resolved a {@link DataInputView} from a StreamStateHandle.
	 */
	private static final class DataInputViewHandle implements StateHandle<DataInputView> {

		private static final long serialVersionUID = 2891559813513532079L;

		private final StreamStateHandle stream;

		private DataInputViewHandle(StreamStateHandle stream) {
			this.stream = stream;
		}

		@Override
		public DataInputView getState(ClassLoader userCodeClassLoader) throws Exception {
			return new DataInputViewStreamWrapper(stream.getState(userCodeClassLoader));
		}

		@Override
		public void discardState() throws Exception {
			stream.discardState();
		}

		@Override
		public long getStateSize() throws Exception {
			return stream.getStateSize();
		}
	}
}
