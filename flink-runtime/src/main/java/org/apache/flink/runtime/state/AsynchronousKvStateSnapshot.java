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

/**
 * {@link KvStateSnapshot} that asynchronously materializes the state that it represents. Instead
 * of representing a materialized handle to state this would normally hold the (immutable) state
 * internally and materializes it when {@link #materialize()} is called.
 *
 * @param <K> The type of the key
 * @param <N> The type of the namespace
 * @param <Backend> The type of the backend that can restore the state from this snapshot.
 */
public abstract class AsynchronousKvStateSnapshot<K, N, Backend extends PartitionedStateBackend<K>> implements KvStateSnapshot<K, N, Backend> {
	private static final long serialVersionUID = 1L;

	/**
	 * Materializes the state held by this {@code AsynchronousKvStateSnapshot}.
	 */
	public abstract KvStateSnapshot<K, N, Backend> materialize() throws Exception;

	@Override
	public final KvState<K, N, Backend> restoreState(
		Backend stateBackend,
		TypeSerializer<K> keySerializer,
		ClassLoader classLoader,
		long recoveryTimestamp) throws Exception {
		throw new RuntimeException("This should never be called and probably points to a bug.");
	}

	@Override
	public void discardState() throws Exception {
		throw new RuntimeException("This should never be called and probably points to a bug.");
	}

	@Override
	public long getStateSize() throws Exception {
		throw new RuntimeException("This should never be called and probably points to a bug.");
	}
}
