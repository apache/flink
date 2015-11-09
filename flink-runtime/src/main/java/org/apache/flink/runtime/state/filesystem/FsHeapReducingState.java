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

package org.apache.flink.runtime.state.filesystem;

import org.apache.flink.api.common.state.ReducingStateIdentifier;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;
import org.apache.flink.runtime.state.AbstractHeapReducingState;

import java.io.DataOutputStream;
import java.util.HashMap;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ListState} that is snapshotted
 * into files.
 * 
 * @param <K> The type of the key.
 * @param <V> The type of the value.
 */
public class FsHeapReducingState<K, V> extends AbstractHeapReducingState<K, V, FsStateBackend> {

	/** The file system state backend backing snapshots of this state */
	private final FsStateBackend backend;

	/**
	 * Creates a new and empty partitioned state.
	 *
	 * @param keySerializer The serializer for the key.
	 * @param stateIdentifier The state identifier for the state. This contains name
	 * and can create a default state value.
	 * @param backend The file system state backend backing snapshots of this state
	 */
	public FsHeapReducingState(TypeSerializer<K> keySerializer, ReducingStateIdentifier<V> stateIdentifier, FsStateBackend backend) {
		super(backend, keySerializer, stateIdentifier);
		this.backend = backend;
	}

	/**
	 * Creates a new key/value state with the given state contents.
	 * This method is used to re-create key/value state with existing data, for example from
	 * a snapshot.
	 *
	 * @param keySerializer The serializer for the key.
	 * @param stateIdentifier The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param state The map of key/value pairs to initialize the state with.
	 * @param backend The file system state backend backing snapshots of this state
	 */
	public FsHeapReducingState(TypeSerializer<K> keySerializer, ReducingStateIdentifier<V> stateIdentifier, HashMap<K, V> state, FsStateBackend backend) {
		super(backend, keySerializer, stateIdentifier, state);
		this.backend = backend;
	}



	@Override
	public FsHeapReducingStateSnapshot<K, V> snapshot(long checkpointId, long timestamp) throws Exception {
		// first, create an output stream to write to
		try (FsStateBackend.FsCheckpointStateOutputStream out = 
					backend.createCheckpointStateOutputStream(checkpointId, timestamp)) {

			// serialize the state to the output stream
			OutputViewDataOutputStreamWrapper outView = 
					new OutputViewDataOutputStreamWrapper(new DataOutputStream(out));
			outView.writeInt(size());
			writeStateToOutputView(outView);
			outView.flush();
			
			// create a handle to the state
			return new FsHeapReducingStateSnapshot<K, V>(getKeySerializer(), stateIdentifier, out.closeAndGetPath());
		}
	}
}
