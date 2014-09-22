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

package org.apache.flink.api.common.functions.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.FutureTask;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.fs.Path;

/**
 * Implementation of the {@link RuntimeContext}, created by runtime UDF operators.
 */
public class RuntimeUDFContext implements RuntimeContext {

	private final String name;

	private final int numParallelSubtasks;

	private final int subtaskIndex;

	private final ClassLoader userCodeClassLoader;
	
	private final DistributedCache distributedCache = new DistributedCache();

	private final HashMap<String, Accumulator<?, ?>> accumulators = new HashMap<String, Accumulator<?, ?>>();

	private final HashMap<String, List<?>> broadcastVars = new HashMap<String, List<?>>();

	public RuntimeUDFContext(String name, int numParallelSubtasks, int subtaskIndex, ClassLoader userCodeClassLoader) {
		this.name = name;
		this.numParallelSubtasks = numParallelSubtasks;
		this.subtaskIndex = subtaskIndex;
		this.userCodeClassLoader = userCodeClassLoader;
	}

	public RuntimeUDFContext(String name, int numParallelSubtasks, int subtaskIndex, ClassLoader userCodeClassLoader, Map<String, FutureTask<Path>> cpTasks) {
		this.name = name;
		this.numParallelSubtasks = numParallelSubtasks;
		this.subtaskIndex = subtaskIndex;
		this.userCodeClassLoader = userCodeClassLoader;
		this.distributedCache.setCopyTasks(cpTasks);
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

	public void setBroadcastVariable(String name, List<?> value) {
		this.broadcastVars.put(name, value);
	}


	@Override
	@SuppressWarnings("unchecked")
	public <RT> List<RT> getBroadcastVariable(String name) {
		if (!this.broadcastVars.containsKey(name)) {
			throw new IllegalArgumentException("Trying to access an unbound broadcast variable '" 
					+ name + "'.");
		}
		return (List<RT>) this.broadcastVars.get(name);
	}

	@Override
	public DistributedCache getDistributedCache() {
		return this.distributedCache;
	}
	
	@Override
	public ClassLoader getUserCodeClassLoader() {
		return this.userCodeClassLoader;
	}
}
