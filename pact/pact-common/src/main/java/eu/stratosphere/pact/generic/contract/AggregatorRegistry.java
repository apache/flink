/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.pact.generic.contract;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.pact.common.stubs.aggregators.Aggregator;
import eu.stratosphere.pact.common.stubs.aggregators.AggregatorWithName;
import eu.stratosphere.pact.common.stubs.aggregators.ConvergenceCriterion;
import eu.stratosphere.pact.common.type.Value;

/**
 *
 */
public class AggregatorRegistry {
	
	private final Map<String, Class<Aggregator<?>>> registry = new HashMap<String, Class<Aggregator<?>>>();
	
	private ConvergenceCriterion<? extends Value> convergenceCriterion;
	
	private String convergenceCriterionAggregatorName;
	
	// --------------------------------------------------------------------------------------------
	
	public void registerAggregator(String name, Class<Aggregator<?>> aggregator) {
		if (name == null || aggregator == null) {
			throw new IllegalArgumentException("Name or aggregator must not be null");
		}
		if (this.registry.containsKey(name)) {
			throw new RuntimeException("An aggregator is already registered under the given name.");
		}
		this.registry.put(name, aggregator);
	}
	
	public Class<Aggregator<?>> unregisterAggregator(String name) {
		return this.registry.remove(name);
	}
	
	public Collection<AggregatorWithName<?>> getAllRegisteredAggregators() {
		ArrayList<AggregatorWithName<?>> list = new ArrayList<AggregatorWithName<?>>(this.registry.size());
		
		for (Map.Entry<String, Class<Aggregator<?>>> entry : this.registry.entrySet()) {
			@SuppressWarnings("unchecked")
			Class<Aggregator<Value>> valAgg = (Class<Aggregator<Value>>) (Class<?>) entry.getValue();
			list.add(new AggregatorWithName<Value>(entry.getKey(), valAgg));
		}
		return list;
	}
	
	public <T extends Value> void registerAggregationConvergenceCriterion(
			String name, Class<Aggregator<T>> aggregator, ConvergenceCriterion<T> convergenceCheck)
	{
		if (name == null || aggregator == null || convergenceCheck == null) {
			throw new IllegalArgumentException("Name, aggregator, or convergence criterion must not be null");
		}
		
		@SuppressWarnings("unchecked")
		Class<Aggregator<?>> genAgg = (Class<Aggregator<?>>) (Class<?>) aggregator;
		
		Class<Aggregator<?>> previous = this.registry.get(name);
		if (previous != null && previous != genAgg) {
			throw new RuntimeException("An aggregator is already registered under the given name.");
		}
		
		this.registry.put(name, genAgg);
		this.convergenceCriterion = convergenceCheck;
		this.convergenceCriterionAggregatorName = name;
	}
	
	public String getConvergenceCriterionAggregatorName() {
		return this.convergenceCriterionAggregatorName;
	}
	
	public ConvergenceCriterion<?> getConvergenceCriterion() {
		return this.convergenceCriterion;
	}
}
