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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.ConcurrentModificationException;

/**
 * Incremental cleanup of state with TTL.
 *
 * @param <K> type of state key
 * @param <N> type of state namespace
 */
class TtlIncrementalCleanup<K, N> {
	private static final Logger LOG = LoggerFactory.getLogger(TtlIncrementalCleanup.class);

	@Nonnegative
	private final int cleanupSize;

	@Nonnull
	private final KeyedStateBackend<K> keyContext;

	private AbstractTtlState<K, N, ?, ?, ?> ttlState;
	private CloseableIterator<Tuple2<N, K>> keyNamespaceIterator;
	private boolean suppressCallback;

	/**
	 * TtlIncrementalCleanup constructor.
	 *
	 * @param cleanupSize max number of queued keys to incrementally cleanup upon state access
	 * @param keyContext state backend to switch keys for cleanup
	 */
	TtlIncrementalCleanup(@Nonnegative int cleanupSize, @Nonnull KeyedStateBackend<K> keyContext) {
		this.cleanupSize = cleanupSize;
		this.keyContext = keyContext;
		this.suppressCallback = false;
	}

	void stateAccessed() {
		// key is changed during cleanup, but this should not be called recursively in this case
		if (suppressCallback) {
			return;
		}
		suppressCallback = true;
		initIteratorIfNot();
		K currentKey = keyContext.getCurrentKey();
		N currentNamespace = ttlState.getCurrentNamespace();
		try {
			runCleanup();
		} catch (ConcurrentModificationException e) {
			// just start over next time
			closeIterator();
		} catch (Throwable t) {
			throw new FlinkRuntimeException("Failed to incrementally clean up state with TTL", t);
		} finally {
			if (currentKey != null) {
				keyContext.setCurrentKey(currentKey);
			}
			if (currentNamespace != null) {
				ttlState.setCurrentNamespace(currentNamespace);
			}
			suppressCallback = false;
		}
	}

	private void initIteratorIfNot() {
		if (keyNamespaceIterator == null || !keyNamespaceIterator.hasNext()) {
			closeIterator();
			keyNamespaceIterator = ttlState.original.getNamespaceKeyIterator();
		}
	}

	private void closeIterator() {
		if (keyNamespaceIterator != null) {
			try {
				keyNamespaceIterator.close();
			} catch (Exception e) {
				LOG.warn("Failed to close namespace/key iterator");
			}
			keyNamespaceIterator = null;
		}
	}

	private void runCleanup() throws Exception {
		int entryNum = 0;
		while (entryNum < cleanupSize && keyNamespaceIterator.hasNext()) {
			Tuple2<N, K> state = keyNamespaceIterator.next();
			if (cleanupState(state)) {
				keyNamespaceIterator.remove();
			}
			entryNum++;
		}
	}

	private boolean cleanupState(Tuple2<N, K> state) throws Exception {
		N namespace = state.f0;
		K key = state.f1;
		keyContext.setCurrentKey(key);
		ttlState.setCurrentNamespace(namespace);
		return ttlState.cleanupIfExpired();
	}

	/**
	 * As TTL state wrapper depends on this class through access callback,
	 * it has to be set here after its construction is done.
	 */
	public void setTtlState(@Nonnull AbstractTtlState<K, N, ?, ?, ?> ttlState) {
		this.ttlState = ttlState;
	}
}
