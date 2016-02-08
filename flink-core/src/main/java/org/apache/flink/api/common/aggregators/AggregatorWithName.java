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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.types.Value;

/**
 * Simple utility class holding an {@link Aggregator} with the name it is registered under.
 */
@PublicEvolving
public class AggregatorWithName<T extends Value> {

	private final String name;
	
	private final Aggregator<T> aggregator;

	/**
	 * Creates a new instance for the given aggregator and name.
	 * 
	 * @param name The name that the aggregator is registered under.
	 * @param aggregator The aggregator.
	 */
	public AggregatorWithName(String name, Aggregator<T> aggregator) {
		this.name = name;
		this.aggregator = aggregator;
	}
	
	/**
	 * Gets the name that the aggregator is registered under.
	 * 
	 * @return The name that the aggregator is registered under.
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Gets the aggregator.
	 * 
	 * @return The aggregator.
	 */
	public Aggregator<T> getAggregator() {
		return aggregator;
	}
}
