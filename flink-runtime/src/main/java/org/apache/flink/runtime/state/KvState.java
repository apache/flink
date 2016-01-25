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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;

/**
 * Key/Value state implementation for user-defined state. The state is backed by a state
 * backend, which typically follows one of the following patterns: Either the state is stored
 * in the key/value state object directly (meaning in the executing JVM) and snapshotted by the
 * state backend into some store (during checkpoints), or the key/value state is in fact backed
 * by an external key/value store as the state backend, and checkpoints merely record the
 * metadata of what is considered part of the checkpoint.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <S> The type of {@link State} this {@code KvState} holds.
 * @param <SD> The type of the {@link StateDescriptor} for state {@code S}.
 * @param <Backend> The type of {@link AbstractStateBackend} that manages this {@code KvState}.
 */
public interface KvState<K, N, S extends State, SD extends StateDescriptor<S>, Backend extends AbstractStateBackend> {

	/**
	 * Sets the current key, which will be used when using the state access methods.
	 *
	 * @param key The key.
	 */
	void setCurrentKey(K key);

	/**
	 * Sets the current namespace, which will be used when using the state access methods.
	 *
	 * @param namespace The namespace.
	 */
	void setCurrentNamespace(N namespace);

	/**
	 * Creates a snapshot of this state.
	 * 
	 * @param checkpointId The ID of the checkpoint for which the snapshot should be created.
	 * @param timestamp The timestamp of the checkpoint.
	 * @return A snapshot handle for this key/value state.
	 * 
	 * @throws Exception Exceptions during snapshotting the state should be forwarded, so the system
	 *                   can react to failed snapshots.
	 */
	KvStateSnapshot<K, N, S, SD, Backend> snapshot(long checkpointId, long timestamp) throws Exception;

	/**
	 * Disposes the key/value state, releasing all occupied resources.
	 */
	void dispose();
}
