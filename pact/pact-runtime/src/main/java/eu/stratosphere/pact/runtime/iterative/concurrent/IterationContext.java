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

package eu.stratosphere.pact.runtime.iterative.concurrent;

import eu.stratosphere.pact.common.type.Value;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

//TODO generify
public class IterationContext {

	private ConcurrentMap<Integer, Value> aggregates;

	private ConcurrentMap<Integer, Value> globalAggregates;

	/** single instance */
	private static final IterationContext INSTANCE = new IterationContext();

	private IterationContext() {
		aggregates = new ConcurrentHashMap<Integer, Value>();
		globalAggregates = new ConcurrentHashMap<Integer, Value>();
	}

	/** retrieve singleton instance */
	public static IterationContext instance() {
		return INSTANCE;
	}

	public void setAggregate(int index, Value aggregate) {
		aggregates.put(index, aggregate);
	}

	public Value getAggregateAndReset(int index) {
		return aggregates.remove(index);
	}

	public void setGlobalAggregate(int index, Value aggregate) {
		globalAggregates.put(index, aggregate);
	}

	public Value getGlobalAggregate(int index) {
		return globalAggregates.get(index);
	}
}
