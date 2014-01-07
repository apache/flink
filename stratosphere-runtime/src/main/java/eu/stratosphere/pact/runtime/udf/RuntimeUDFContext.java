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
package eu.stratosphere.pact.runtime.udf;

import java.util.Collection;
import java.util.HashMap;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.accumulators.AccumulatorHelper;
import eu.stratosphere.api.common.accumulators.DoubleCounter;
import eu.stratosphere.api.common.accumulators.Histogram;
import eu.stratosphere.api.common.accumulators.IntCounter;
import eu.stratosphere.api.common.accumulators.LongCounter;
import eu.stratosphere.api.common.functions.RuntimeContext;
import eu.stratosphere.types.Record;

/**
 *
 */
public class RuntimeUDFContext implements RuntimeContext {

	private final String name;

	private final int numParallelSubtasks;

	private final int subtaskIndex;

	private HashMap<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();

	private HashMap<String, Collection<?>> broadcastVars = new HashMap<String, Collection<?>>();

	public RuntimeUDFContext(String name, int numParallelSubtasks, int subtaskIndex) {
		this.name = name;
		this.numParallelSubtasks = numParallelSubtasks;
		this.subtaskIndex = subtaskIndex;
	}

	@Override
	public String getTaskName() {
		return this.name;
	}

	@Override
	public int getNumberOfParallelSubtasks() {
		return this.numParallelSubtasks;
	}

	@Override
	public int getIndexOfThisSubtask() {
		return this.subtaskIndex;
	}

	@Override
	public IntCounter getIntCounter(String name) {
		return (IntCounter) getAccumulator(name, IntCounter.class);
	}

	@Override
	public LongCounter getLongCounter(String name) {
		return (LongCounter) getAccumulator(name, LongCounter.class);
	}

	@Override
	public Histogram getHistogram(String name) {
		return (Histogram) getAccumulator(name, Histogram.class);
	}

	@Override
	public DoubleCounter getDoubleCounter(String name) {
		return (DoubleCounter) getAccumulator(name, DoubleCounter.class);
	}

	@Override
	public <V, A> void addAccumulator(String name, Accumulator<V, A> accumulator) {
		if (accumulators.containsKey(name)) {
			throw new UnsupportedOperationException("The counter '" + name
					+ "' already exists and cannot be added.");
		}
		accumulators.put(name, accumulator);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V, A> Accumulator<V, A> getAccumulator(String name) {
		return (Accumulator<V, A>) accumulators.get(name);
	}

	@SuppressWarnings("unchecked")
	private <V, A> Accumulator<V, A> getAccumulator(String name,
			Class<? extends Accumulator<V, A>> accumulatorClass) {

		Accumulator<?, ?> accumulator = accumulators.get(name);

		if (accumulator != null) {
			AccumulatorHelper.compareAccumulatorTypes(name, accumulator.getClass(), accumulatorClass);
		} else {
			// Create new accumulator
			try {
				accumulator = accumulatorClass.newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			accumulators.put(name, accumulator);
		}
		return (Accumulator<V, A>) accumulator;
	}

	@Override
	public HashMap<String, Accumulator<?, ?>> getAllAccumulators() {
		return this.accumulators;
	}

	@Override
	public void setBroadcastVariable(String name, Collection<?> value) {
		if (this.broadcastVars.containsKey(name)) {
			throw new UnsupportedOperationException("The broadcast variable '" + name
					+ "' already exists and cannot be added.");
		}
		this.broadcastVars.put(name, value);
	}


	@Override
	@SuppressWarnings("unchecked")
	public <RT> Collection<RT> getBroadcastVariable(String name) {
		if (!this.broadcastVars.containsKey(name)) {
			throw new UnsupportedOperationException("Trying to access an unbound broadcast variable '" 
					+ name + "'.");
		}
		return (Collection<RT>) this.broadcastVars.get(name);
	}
}
