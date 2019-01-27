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

package org.apache.flink.api.common.accumulators;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Accumulator registry encapsulates user-defined accumulators.
 */
public abstract class AbstractAccumulatorRegistry {
	/* User-defined accumulator values stored for the executing task. */
	protected final Map<String, Accumulator<?, ?>> userAccumulators =
		new ConcurrentHashMap<>(4);

	/**
	 * Adds an ordinary accumulator.
	 */
	public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {
		if (userAccumulators.containsKey(name)) {
			throw new UnsupportedOperationException("The accumulator '" + name
				+ "' already exists and cannot be added.");
		}

		userAccumulators.put(name, accumulator);
	}

	/**
	 * Gets all the user-defined accumulators.
	 */
	public Map<String, Accumulator<?, ?>> getAccumulators() {
		return Collections.unmodifiableMap(userAccumulators);
	}

	/**
	 * Adds a pre-aggregated accumulator.
	 */
	public abstract  <V, A extends Serializable> void addPreAggregatedAccumulator(String name, Accumulator<V, A> accumulator);

	/**
	 * Gets all the uncommitted pre-aggregated accumulators.
	 */
	public abstract Map<String, Accumulator<?, ?>> getPreAggregatedAccumulators();

	/**
	 * Commits a pre-aggregated accumulator with the specific name.
	 */
	public abstract void commitPreAggregatedAccumulator(String name);

	/**
	 * Queries a pre-aggregated accumulator with the specific name asynchronously.
	 */
	public abstract <V, A extends Serializable> CompletableFuture<Accumulator<V, A>> queryPreAggregatedAccumulator(String name);
}
