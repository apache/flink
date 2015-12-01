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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.execution.Environment;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * A state backend defines how state is stored and snapshotted during checkpoints.
 *
 * @param <Backend> The type of backend itself. This generic parameter is used to refer to the
 *                  type of backend when creating state backed by this backend.
 */
public abstract class StateBackend<Backend extends StateBackend<Backend>> implements java.io.Serializable {

	private static final long serialVersionUID = 4620413814639220247L;

	// ------------------------------------------------------------------------
	//  initialization and cleanup
	// ------------------------------------------------------------------------

	/**
	 * This method is called by the task upon deployment to initialize the state backend for
	 * data for a specific job.
	 *
	 * @param env The {@link Environment} of the task that instantiated the state backend
	 * @throws Exception Overwritten versions of this method may throw exceptions, in which
	 *                   case the job that uses the state backend is considered failed during
	 *                   deployment.
	 */
	public abstract void initializeForJob(Environment env) throws Exception;

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

	// ------------------------------------------------------------------------
	//  key/value state
	// ------------------------------------------------------------------------

	/**
	 * Creates a key/value state backed by this state backend.
	 *
	 * @param stateId Unique id that identifies the kv state in the streaming program. 
	 * @param stateName Name of the created state
	 * @param keySerializer The serializer for the key.
	 * @param valueSerializer The serializer for the value.
	 * @param defaultValue The value that is returned when no other value has been associated with a key, yet.
	 * @param <K> The type of the key.
	 * @param <V> The type of the value.
	 *
	 * @return A new key/value state backed by this backend.
	 *
	 * @throws Exception Exceptions may occur during initialization of the state and should be forwarded.
	 */
	public abstract <K, V> KvState<K, V, Backend> createKvState(String stateId, String stateName,
			TypeSerializer<K> keySerializer, TypeSerializer<V> valueSerializer,
			V defaultValue) throws Exception;


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
