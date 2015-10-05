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

package org.apache.flink.streaming.api.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateHandle;

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
	 * @param job The ID of the job for which the state backend instance checkpoints data.
	 * @throws Exception Overwritten versions of this method may throw exceptions, in which
	 *                   case the job that uses the state backend is considered failed during
	 *                   deployment.
	 */
	public abstract void initializeForJob(JobID job) throws Exception;

	/**
	 * Disposes all state associated with the current job.
	 * 
	 * @throws Exception Exceptions may occur during disposal of the state and should be forwarded.
	 */
	public abstract void disposeAllStateForCurrentJob() throws Exception;
	
	// ------------------------------------------------------------------------
	//  key/value state
	// ------------------------------------------------------------------------

	/**
	 * Creates a key/value state backed by this state backend.
	 * 
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
	public abstract <K, V> KvState<K, V, Backend> createKvState(
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
}
