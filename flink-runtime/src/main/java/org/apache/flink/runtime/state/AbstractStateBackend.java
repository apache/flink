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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateBackend;
import org.apache.flink.api.common.state.StateIdentifier;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A state backend defines how state is stored and snapshotted during checkpoints.
 */
public abstract class AbstractStateBackend implements java.io.Serializable, StateBackend {
	
	private static final long serialVersionUID = 4620413814639220247L;

	protected transient TypeSerializer<?> keySerializer;

	protected transient ClassLoader userCodeClassLoader;

	protected transient Object currentKey;

	/** For efficient access in setCurrentKey() */
	private transient KvState<?, ?, ?, ?>[] keyValueStates;

	/** So that we can give out state when the user uses the same key. */
	private transient HashMap<String, KvState<?, ?, ?, ?>> keyValueStatesByName;

	/** State handles for lazy state restore. */
	private transient HashMap<String, KvStateSnapshot<?, ?, ?, ?>> keyValueStateSnapshots;

	// ------------------------------------------------------------------------
	//  initialization and cleanup
	// ------------------------------------------------------------------------
	
	/**
	 * This method is called by the task upon deployment to initialize the state backend for
	 * data for a specific job.
	 * 
	 * @param job The ID of the job for which the state backend instance checkpoints data.
	 * @throws Exception Overwritten versions of this method may throw exceptions, in which
	 *                   case the job that uses the state backend is considered failed during
	 *                   deployment.
	 */
	public void initializeForJob(JobID job, TypeSerializer<?> keySerializer, ClassLoader userCodeClassLoader) throws Exception {
		this.userCodeClassLoader = userCodeClassLoader;
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
			for (KvState<?, ?, ?, ?> state : keyValueStates) {
				state.dispose();
			}
		}
	}
	
	// ------------------------------------------------------------------------
	//  key/value state
	// ------------------------------------------------------------------------

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

	/**
	 * Creates or retrieves a partitioned state backed by this state backend.
	 *
	 * @param stateIdentifier The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param <K> The type of the key.
	 * @param <S> The type of the state.
	 *
	 * @return A new key/value state backed by this backend.
	 *
	 * @throws Exception Exceptions may occur during initialization of the state and should be forwarded.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public <K, S extends State> S getPartitionedState(StateIdentifier<S> stateIdentifier) throws Exception {

		if (keySerializer == null) {
			throw new Exception("State key serializer has not been configured in the config. " +
					"This operation cannot use partitioned state.");
		}

		if (keyValueStatesByName == null) {
			keyValueStatesByName = new HashMap<>();
		}

		S previous = (S) keyValueStatesByName.get(stateIdentifier.getName());
		if (previous != null) {
			return previous;
		}

		S kvstate = null;

		// check whether we restore the key/value state from a snapshot, or create a new blank one
		if (keyValueStateSnapshots != null) {
			@SuppressWarnings("unchecked")
			KvStateSnapshot<K, S, StateIdentifier<S>, AbstractStateBackend> snapshot = (KvStateSnapshot) keyValueStateSnapshots.remove(stateIdentifier.getName());

			if (snapshot != null) {
				kvstate = (S) snapshot.restoreState(this, (TypeSerializer) keySerializer, stateIdentifier, userCodeClassLoader);
			}
		}

		if (kvstate == null) {
			// create a new blank key/value state
			kvstate = stateIdentifier.bind(this);
		}

		((KvState) kvstate).setCurrentKey(currentKey);

		keyValueStatesByName.put(stateIdentifier.getName(), (KvState) kvstate);
		keyValueStates = keyValueStatesByName.values().toArray(new KvState[keyValueStatesByName.size()]);
		return kvstate;
	}

	public HashMap<String, KvStateSnapshot<?, ?, ?, ?>> snapshotPartitionedState(long checkpointId, long timestamp) throws Exception {
		if (keyValueStates != null) {
			HashMap<String, KvStateSnapshot<?, ?, ?, ?>> snapshots = new HashMap<>(keyValueStatesByName.size());

			for (Map.Entry<String, KvState<?, ?, ?, ?>> entry : keyValueStatesByName.entrySet()) {
				KvStateSnapshot<?, ?, ?, ?> snapshot = entry.getValue().snapshot(checkpointId, timestamp);
				snapshots.put(entry.getKey(), snapshot);
			}
			return snapshots;
		}

		return null;
	}

	/**
	 * Injects K/V state snapshots for lazy restore.
	 * @param keyValueStateSnapshots The Map of snapshots
	 */
	public void injectKeyValueStateSnapshots(HashMap<String, KvStateSnapshot<?, ?, ?, ?>> keyValueStateSnapshots) {
		this.keyValueStateSnapshots = keyValueStateSnapshots;
	}

	public void clear(StateIdentifier<?> stateIdentifier) {
		keyValueStatesByName.remove(stateIdentifier.getName());
		keyValueStates = keyValueStatesByName.values().toArray(new KvState[keyValueStatesByName.size()]);
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
	}
}
