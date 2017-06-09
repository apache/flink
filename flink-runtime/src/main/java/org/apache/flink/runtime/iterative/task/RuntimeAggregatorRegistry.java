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

package org.apache.flink.runtime.iterative.task;

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorWithName;
import org.apache.flink.types.Value;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 *
 */
public class RuntimeAggregatorRegistry {

	private final Map<String, Aggregator<?>> aggregators;

	private final Map<String, Value> previousGlobalAggregate;

	public RuntimeAggregatorRegistry(Collection<AggregatorWithName<?>> aggs) {
		this.aggregators = new HashMap<String, Aggregator<?>>();
		this.previousGlobalAggregate = new HashMap<String, Value>();

		for (AggregatorWithName<?> agg : aggs) {
			this.aggregators.put(agg.getName(), agg.getAggregator());
		}
	}

	public Value getPreviousGlobalAggregate(String name) {
		return this.previousGlobalAggregate.get(name);
	}

	@SuppressWarnings("unchecked")
	public <T extends Aggregator<?>> T getAggregator(String name) {
		return (T) this.aggregators.get(name);
	}

	public Map<String, Aggregator<?>> getAllAggregators() {
		return this.aggregators;
	}

	public void updateGlobalAggregatesAndReset(String[] names, Value[] aggregates) {
		if (names == null || aggregates == null || names.length != aggregates.length) {
			throw new IllegalArgumentException();
		}

		// add global aggregates
		for (int i = 0; i < names.length; i++) {
			this.previousGlobalAggregate.put(names[i], aggregates[i]);
		}

		// reset all aggregators
		for (Aggregator<?> agg : this.aggregators.values()) {
			agg.reset();
		}
	}
}
