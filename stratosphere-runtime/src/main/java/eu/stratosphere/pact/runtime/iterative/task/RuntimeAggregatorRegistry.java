/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.pact.runtime.iterative.task;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.api.common.aggregators.Aggregator;
import eu.stratosphere.api.common.aggregators.AggregatorWithName;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.InstantiationUtil;


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
			Aggregator<?> aggregator = InstantiationUtil.instantiate(agg.getAggregator(), Aggregator.class);
			this.aggregators.put(agg.getName(), aggregator);
		}
	}
	
	public Value getPreviousGlobalAggregate(String name) {
		return this.previousGlobalAggregate.get(name);
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Value> Aggregator<T> getAggregator(String name) {
		return (Aggregator<T>) this.aggregators.get(name);
	}
	
	public Map<String, Aggregator<?>> getAllAggregators() {
		return this.aggregators;
	}
	
	public void updateGlobalAggregatesAndReset(String[] names, Value[] aggregates) {
		if (names == null || aggregates == null || names.length != aggregates.length) {
			throw new IllegalArgumentException();
		}
		
		// add global aggregates
		for (int i = 0 ; i < names.length; i++) {
			this.previousGlobalAggregate.put(names[i], aggregates[i]);
		}
		
		// reset all aggregators
		for (Aggregator<?> agg : this.aggregators.values()) {
			agg.reset();
		}
	}
}
