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

package org.apache.flink.api.common.aggregators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Value;

/**
 * A registry for iteration {@link Aggregator}s.
 */
@Internal
public class AggregatorRegistry {
	
	private final Map<String, Aggregator<?>> registry = new HashMap<String, Aggregator<?>>();
	
	private ConvergenceCriterion<? extends Value> convergenceCriterion;
	
	private String convergenceCriterionAggregatorName;
	
	// --------------------------------------------------------------------------------------------
	
	public void registerAggregator(String name, Aggregator<?> aggregator) {
		if (name == null || aggregator == null) {
			throw new IllegalArgumentException("Name and aggregator must not be null");
		}
		if (this.registry.containsKey(name)) {
			throw new RuntimeException("An aggregator is already registered under the given name.");
		}
		this.registry.put(name, aggregator);
	}

	public Collection<AggregatorWithName<?>> getAllRegisteredAggregators() {
		ArrayList<AggregatorWithName<?>> list = new ArrayList<AggregatorWithName<?>>(this.registry.size());
		
		for (Map.Entry<String, Aggregator<?>> entry : this.registry.entrySet()) {
			@SuppressWarnings("unchecked")
			Aggregator<Value> valAgg = (Aggregator<Value>) entry.getValue();
			list.add(new AggregatorWithName<>(entry.getKey(), valAgg));
		}
		return list;
	}
	
	public <T extends Value> void registerAggregationConvergenceCriterion(
			String name, Aggregator<T> aggregator, ConvergenceCriterion<T> convergenceCheck)
	{
		if (name == null || aggregator == null || convergenceCheck == null) {
			throw new IllegalArgumentException("Name, aggregator, or convergence criterion must not be null");
		}
		
		Aggregator<?> genAgg = aggregator;
		
		Aggregator<?> previous = this.registry.get(name);
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
	
	public void addAll(AggregatorRegistry registry) {
		this.registry.putAll(registry.registry);
		this.convergenceCriterion = registry.convergenceCriterion;
		this.convergenceCriterionAggregatorName = registry.convergenceCriterionAggregatorName;
	}
}
