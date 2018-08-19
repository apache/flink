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

package org.apache.flink.runtime.state.ttl.cleanup;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.ttl.AbstractTtlState;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Internal service for queueing detected expired state keys between full snapshot thread and state processing thread.
 *
 * <p>If configured in {@link org.apache.flink.api.common.state.StateTtlConfig.FullSnapshotCleanupStrategy},
 * {@link TtlStateSnapshotTransformer} uses this service class to queue detected expired state keys if queue is not full.
 * {@link AbstractTtlState} wrappers give this service {@code stateAccessed()} callback
 * whenever this state is accessed for any key. This service then pulls up to certain number of potentially expired keys,
 * checks their state and clears it if it is still expired since having been queued:
 *
 * <p>Snapshot thread:
 * snapshotting -> |state key/value| -> TtlStateSnapshotTransformer -> |expired state keys| -> TtlIncrementalCleanup -> queue
 *
 * <p>Main record processing thread of operator:
 * touch state -> AbstractTtlState -> TtlIncrementalCleanup.stateAccessed() -> pull queue -> AbstractTtlState.cleanupIfExpired()
 *
 * @param <K> type of state key
 * @param <N> type of state namespace
 */
public class TtlIncrementalCleanup<K, N> {
	private final BlockingDeque<Tuple2<K, N>> cleanupQueue;
	private final int cleanupSize;
	private final KeyedStateBackend<K> keyContext;
	private AbstractTtlState<K, N, ?, ?, ?> ttlState;

	/**
	 * TtlIncrementalCleanup constructor.
	 *
	 * @param queueSize max cleanup queue size
	 * @param cleanupSize max number of queued keys to incrementally cleanup upon state access
	 * @param keyContext state backend to switch keys for cleanup
	 */
	public TtlIncrementalCleanup(
		int queueSize,
		int cleanupSize,
		KeyedStateBackend<K> keyContext) {
		Preconditions.checkArgument(cleanupSize > 0);
		this.cleanupQueue = queueSize > 0 ? new LinkedBlockingDeque<>(queueSize) : new LinkedBlockingDeque<>();
		this.cleanupSize = cleanupSize;
		this.keyContext = keyContext;
	}

	@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
	<V> void addIfExpired(K key, N namespace, V value, V filteredValue) {
		if (filteredValue == null || value != filteredValue) {
			addExpiredIfCapacityAvailable(key, namespace);
		}
	}

	private void addExpiredIfCapacityAvailable(K key, N namespace) {
		Tuple2<K, N> expired = Tuple2.of(key, namespace);
		if (!Objects.equals(cleanupQueue.peekLast(), expired)) {
			cleanupQueue.offerLast(expired);
		}
	}

	public void stateAccessed() {
		K currentKey = keyContext.getCurrentKey();
		N currentNamespace = ttlState.getCurrentNamespace();
		try {
			int polled = 0;
			Tuple2<K, N> state;
			while (polled < cleanupSize && (state = cleanupQueue.pollFirst()) != null) {
				runCleanup(state.f0, state.f1);
				polled++;
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Failed to lazily clean up state with TTL", e);
		} finally {
			keyContext.setCurrentKey(currentKey);
			ttlState.setCurrentNamespace(currentNamespace);
		}
	}

	private void runCleanup(K key, N namespace) throws Exception {
		keyContext.setCurrentKey(key);
		ttlState.setCurrentNamespace(namespace);
		ttlState.cleanupIfExpired();
	}

	/**
	 * As TTL state wrapper depends on this class through {@link TtlStateSnapshotTransformer.Factory},
	 * it has to be set here after its construction is done.
	 */
	public void setTtlState(AbstractTtlState<K, N, ?, ?, ?> ttlState) {
		this.ttlState = ttlState;
	}
}
