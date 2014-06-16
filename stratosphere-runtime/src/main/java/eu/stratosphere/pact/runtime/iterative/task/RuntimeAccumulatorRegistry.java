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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import eu.stratosphere.api.common.accumulators.Accumulator;


/**
 *
 */
public class RuntimeAccumulatorRegistry {
	
	private final Map<String, Accumulator<?, ?>> accumulators;
	
	private final Map<String, Accumulator<?, ?>> previousGlobalAccumulator;
	
	public RuntimeAccumulatorRegistry() {
		this.accumulators = new ConcurrentHashMap<String, Accumulator<?,?>>();
		this.previousGlobalAccumulator = new ConcurrentHashMap<String, Accumulator<?,?>>();
	}
	
	@SuppressWarnings("unchecked")
	public <V, A> void addAccumulator(String name, Accumulator<V, A> accumulator) {
		/* 
		 * if already there and not the same instance -> take new instance but copy over old value.
		 * This hack is needed because the open methods of operators inside of iterations can be called again
		 * before the result of the accumulators was sent by the head task
		 * 
		 */
		if (accumulators.containsKey(name) && accumulators.get(name) != accumulator) {
			accumulator.add((V) accumulators.get(name).getLocalValue());
		}
		accumulators.put(name, accumulator);
		
		/*
		 *  this is a workaround that helps to prevent a nullpointer exception when requesting a previous
		 *  accumulator in the open method of an operator and the iteration terminates directlly after the first
		 *  iteration, before the first global accumulators are set
		 */
		if(!previousGlobalAccumulator.containsKey(name)) {
			previousGlobalAccumulator.put(name, accumulator);
		}
	}
	
	public Accumulator<?, ?> getPreviousGlobalAccumulator(String name) {
		return this.previousGlobalAccumulator.get(name);
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Accumulator<?, ?>> T getAccumulator(String name) {
		return (T) this.accumulators.get(name);
	}
	
	public Map<String, Accumulator<?, ?>> getAllAccumulators() {
		return this.accumulators;
	}
	
	public void updateGlobalAccumulatorsAndReset(Map<String, Accumulator<?, ?>> accumulators) {
		if (accumulators == null) {
			throw new IllegalArgumentException();
		}
		
		// add global accumulators
		this.previousGlobalAccumulator.putAll(accumulators);
		
		// reset all accumulators
		for (Accumulator<?, ?> acc : this.accumulators.values()) {
			acc.resetLocal();
		}
	}
}
