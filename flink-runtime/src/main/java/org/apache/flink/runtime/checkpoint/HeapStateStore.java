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

package org.apache.flink.runtime.checkpoint;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Java heap backed {@link StateStore}.
 *
 * @param <T> Type of state
 */
class HeapStateStore<T extends Serializable> implements StateStore<T> {

	private final ConcurrentMap<String, T> stateMap = new ConcurrentHashMap<>();

	private final AtomicInteger idCounter = new AtomicInteger();

	@Override
	public String putState(T state) throws Exception {
		checkNotNull(state, "State");

		String key = "jobmanager://savepoints/" + idCounter.incrementAndGet();
		stateMap.put(key, state);
		return key;
	}

	@Override
	public T getState(String path) throws Exception {
		T state = stateMap.get(path);

		if (state != null) {
			return state;
		}
		else {
			throw new IllegalArgumentException("Invalid path '" + path + "'.");
		}
	}

	@Override
	public void disposeState(String path) throws Exception {
		T state = stateMap.remove(path);

		if (state == null) {
			throw new IllegalArgumentException("Invalid path '" + path + "'.");
		}
	}

	/**
	 * Returns all stored state.
	 */
	Collection<T> getAll() {
		return stateMap.values();
	}

	/**
	 * Clears all stored state.
	 */
	void clearAll() {
		stateMap.clear();
	}

}
