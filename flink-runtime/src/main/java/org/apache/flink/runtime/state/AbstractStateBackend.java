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

import org.apache.flink.api.common.state.KeyGroupAssigner;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.generic.GenericKeyGroupStateBackend;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * State backend for non-partitioned state (e.g. operator or function state).
 *
 * The state backend only offers an output stream/output view to store state persistently. The
 * stored state is bound to the stream operator instance which uses the state backend. Consequently,
 * this state cannot be scaled up or down.
 *
 * Additionally, the AbstractStateBackend acts as a factory for {@link PartitionedStateBackend} and
 * the corresponding {@link GenericKeyGroupStateBackend}.
 */
public abstract class AbstractStateBackend implements AutoCloseable, Serializable {
	private static final long serialVersionUID = 274476799538194350L;

	protected transient ClassLoader classLoader;

	/**
	 * Initializes the state backend before it is used by an operator. The method sets fields
	 * which are either transient and have been lost due to serialization or because they weren't
	 * available when the object was created.
	 *
	 * @param environment Environment in whose context the state backend runs
	 * @param operatorIdentifier Identifier of the operator
	 * @throws Exception
	 */
	public void initializeForJob(Environment environment, String operatorIdentifier) throws Exception {
		classLoader = environment.getUserClassLoader();
	}

	/**
	 * Creates a partitioned version of this state backend.
	 *
	 * This method is only used by the GenericKeyGroupStateBackend to for each key group a
	 * partitioned state backend which backs this key group.
	 *
	 * @param keySerializer Key serializer for the partitioned states
	 * @param <K> Type of the key
	 * @return PartitionedStateBackend
	 * @throws Exception
	 */
	public abstract <K> PartitionedStateBackend<K> createPartitionedStateBackend(TypeSerializer<K> keySerializer) throws Exception;

	/**
	 * Creates a key group state backend for this abstract state backend.
	 *
	 * @param keySerializer Key serializer for the partitioned states
	 * @param keyGroupAssigner Key group assigner to map keys to key group indices
	 * @param <K> Type of the key
	 * @return KeyGroupStateBackend
	 */
	public <K> KeyGroupStateBackend<K> createKeyGroupStateBackend(TypeSerializer<K> keySerializer, KeyGroupAssigner<K> keyGroupAssigner) {
		return new GenericKeyGroupStateBackend<K>(this, keySerializer, keyGroupAssigner);
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
