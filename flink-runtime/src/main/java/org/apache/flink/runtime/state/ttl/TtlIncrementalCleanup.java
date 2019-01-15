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

import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIteratorWithUpdate;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * Incremental cleanup of state with TTL.
 *
 * @param <K> type of state key
 * @param <N> type of state namespace
 */
class TtlIncrementalCleanup<K, N, S> {
	/** Global state entry iterator is advanced for {@code cleanupSize} entries. */
	@Nonnegative
	private final int cleanupSize;

	/** Particular state with TTL object is used to check whether currently iterated entry has expired. */
	private AbstractTtlState<K, N, ?, S, ?> ttlState;

	/** Global state entry iterator, advanced for {@code cleanupSize} entries every state and/or record processing. */
	private StateIteratorWithUpdate<K, N, S> stateIterator;

	/**
	 * TtlIncrementalCleanup constructor.
	 *
	 * @param cleanupSize max number of queued keys to incrementally cleanup upon state access
	 */
	TtlIncrementalCleanup(@Nonnegative int cleanupSize) {
		this.cleanupSize = cleanupSize;
	}

	void stateAccessed() {
		initIteratorIfNot();
		try {
			runCleanup();
		} catch (Throwable t) {
			throw new FlinkRuntimeException("Failed to incrementally clean up state with TTL", t);
		}
	}

	private void initIteratorIfNot() {
		if (stateIterator == null || !stateIterator.hasNext()) {
			stateIterator = ttlState.original.getStateEntryIterator();
		}
	}

	private void runCleanup() {
		int entryNum = 0;
		while (entryNum < cleanupSize && stateIterator.hasNext()) {
			StateEntry<K, N, S> state = stateIterator.next();
			S cleanState = ttlState.getUnexpiredOrNull(state.getState());
			if (cleanState == null) {
				stateIterator.remove();
			} else if (cleanState != state.getState()) {
				stateIterator.update(cleanState);
			}
			entryNum++;
		}
	}

	/**
	 * As TTL state wrapper depends on this class through access callback,
	 * it has to be set here after its construction is done.
	 */
	public void setTtlState(@Nonnull AbstractTtlState<K, N, ?, S, ?> ttlState) {
		this.ttlState = ttlState;
	}
}
