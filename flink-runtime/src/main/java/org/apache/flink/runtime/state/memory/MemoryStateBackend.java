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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * A {@link AbstractStateBackend} that stores all its data and checkpoints in memory and has no
 * capabilities to spill to disk. Checkpoints are serialized and the serialized data is
 * transferred
 */
public class MemoryStateBackend extends AbstractStateBackend {

	private static final long serialVersionUID = 4109305377809414635L;

	/** The default maximal size that the snapshotted memory state may have (5 MiBytes) */
	private static final int DEFAULT_MAX_STATE_SIZE = 5 * 1024 * 1024;

	/** The maximal size that the snapshotted memory state may have */
	private final int maxStateSize;

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the default state size (5 MB).
	 */
	public MemoryStateBackend() {
		this(DEFAULT_MAX_STATE_SIZE);
	}

	/**
	 * Creates a new memory state backend that accepts states whose serialized forms are
	 * up to the given number of bytes.
	 *
	 * @param maxStateSize The maximal size of the serialized state
	 */
	public MemoryStateBackend(int maxStateSize) {
		this.maxStateSize = maxStateSize;
	}

	// ------------------------------------------------------------------------
	//  initialization and cleanup
	// ------------------------------------------------------------------------

	@Override
	public void disposeAllStateForCurrentJob() {
		// nothing to do here, GC will do it
	}

	@Override
	public void close() throws Exception {}

	// ------------------------------------------------------------------------
	//  State backend operations
	// ------------------------------------------------------------------------

	@Override
	public <N, V> ValueState<V> createValueState(TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<V> stateDesc) throws Exception {
		return new MemValueState<>(keySerializer, namespaceSerializer, stateDesc);
	}

	@Override
	public <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception {
		return new MemListState<>(keySerializer, namespaceSerializer, stateDesc);
	}

	@Override
	public <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer, ReducingStateDescriptor<T> stateDesc) throws Exception {
		return new MemReducingState<>(keySerializer, namespaceSerializer, stateDesc);
	}

	/**
	 * Serialized the given state into bytes using Java serialization and creates a state handle that
	 * can re-create that state.
	 *
	 * @param state The state to checkpoint.
	 * @param checkpointID The ID of the checkpoint.
	 * @param timestamp The timestamp of the checkpoint.
	 * @param <S> The type of the state.
	 *
	 * @return A state handle that contains the given state serialized as bytes.
	 * @throws Exception Thrown, if the serialization fails.
	 */
	@Override
	public <S extends Serializable> StateHandle<S> checkpointStateSerializable(
			S state, long checkpointID, long timestamp) throws Exception
	{
		SerializedStateHandle<S> handle = new SerializedStateHandle<>(state);
		checkSize(handle.getSizeOfSerializedState(), maxStateSize);
		return new SerializedStateHandle<S>(state);
	}

	@Override
	public CheckpointStateOutputStream createCheckpointStateOutputStream(
			long checkpointID, long timestamp) throws Exception
	{
		return new MemoryCheckpointOutputStream(maxStateSize);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "MemoryStateBackend (data in heap memory / checkpoints to JobManager)";
	}

	static void checkSize(int size, int maxSize) throws IOException {
		if (size > maxSize) {
			throw new IOException(
					"Size of the state is larger than the maximum permitted memory-backed state. Size="
							+ size + " , maxSize=" + maxSize
							+ " . Consider using a different state backend, like the File System State backend.");
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A CheckpointStateOutputStream that writes into a byte array.
	 */
	public static final class MemoryCheckpointOutputStream extends CheckpointStateOutputStream {

		private final ByteArrayOutputStream os = new ByteArrayOutputStream();

		private final int maxSize;

		private boolean closed;

		public MemoryCheckpointOutputStream(int maxSize) {
			this.maxSize = maxSize;
		}

		@Override
		public void write(int b) {
			os.write(b);
		}

		@Override
		public void write(byte[] b, int off, int len) {
			os.write(b, off, len);
		}

		// --------------------------------------------------------------------

		@Override
		public void close() {
			closed = true;
			os.reset();
		}

		@Override
		public StreamStateHandle closeAndGetHandle() throws IOException {
			return new ByteStreamStateHandle(closeAndGetBytes());
		}

		/**
		 * Closes the stream and returns the byte array containing the stream's data.
		 * @return The byte array containing the stream's data.
		 * @throws IOException Thrown if the size of the data exceeds the maximal
		 */
		public byte[] closeAndGetBytes() throws IOException {
			if (!closed) {
				checkSize(os.size(), maxSize);
				byte[] bytes = os.toByteArray();
				close();
				return bytes;
			}
			else {
				throw new IllegalStateException("stream has already been closed");
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Static default instance
	// ------------------------------------------------------------------------

	/**
	 * Gets the default instance of this state backend, using the default maximal state size.
	 * @return The default instance of this state backend.
	 */
	public static MemoryStateBackend create() {
		return new MemoryStateBackend();
	}
}
