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

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;

/**
 * A standalone implementation of the {@link RuntimeContext}, created by runtime UDF operators.
 */
public abstract class AbstractRuntimeUDFContext implements RuntimeContext {

	private final TaskInfo taskInfo;

	private final ClassLoader userCodeClassLoader;

	private final ExecutionConfig executionConfig;

	private final Map<String, Accumulator<?, ?>> accumulators;

	private final DistributedCache distributedCache;

	public AbstractRuntimeUDFContext(TaskInfo taskInfo,
										ClassLoader userCodeClassLoader,
										ExecutionConfig executionConfig,
										Map<String, Accumulator<?,?>> accumulators,
										Map<String, Future<Path>> cpTasks) {
		this.taskInfo = Preconditions.checkNotNull(taskInfo);
		this.userCodeClassLoader = userCodeClassLoader;
		this.executionConfig = executionConfig;
		this.distributedCache = new DistributedCache(Preconditions.checkNotNull(cpTasks));
		this.accumulators = Preconditions.checkNotNull(accumulators);
	}

	@Override
	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	@Override
	public String getTaskName() {
		return taskInfo.getTaskName();
	}

	@Override
	public int getNumberOfParallelSubtasks() {
		return taskInfo.getNumberOfParallelSubtasks();
	}

	@Override
	public int getIndexOfThisSubtask() {
		return taskInfo.getIndexOfThisSubtask();
	}

	@Override
	public int getAttemptNumber() {
		return taskInfo.getAttemptNumber();
	}

	@Override
	public String getTaskNameWithSubtasks() {
		return taskInfo.getTaskNameWithSubtasks();
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
	public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {
		if (accumulators.containsKey(name)) {
			throw new UnsupportedOperationException("The accumulator '" + name
					+ "' already exists and cannot be added.");
		}
		accumulators.put(name, accumulator);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
		return (Accumulator<V, A>) accumulators.get(name);
	}

	@Override
	public Map<String, Accumulator<?, ?>> getAllAccumulators() {
		return Collections.unmodifiableMap(this.accumulators);
	}
	
	@Override
	public ClassLoader getUserCodeClassLoader() {
		return this.userCodeClassLoader;
	}
	
	@Override
	public DistributedCache getDistributedCache() {
		return this.distributedCache;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	private <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name,
			Class<? extends Accumulator<V, A>> accumulatorClass)
	{
		Accumulator<?, ?> accumulator = accumulators.get(name);

		if (accumulator != null) {
			AccumulatorHelper.compareAccumulatorTypes(name, accumulator.getClass(), accumulatorClass);
		} else {
			// Create new accumulator
			try {
				accumulator = accumulatorClass.newInstance();
			}
			catch (Exception e) {
				throw new RuntimeException("Cannot create accumulator " + accumulatorClass.getName());
			}
			accumulators.put(name, accumulator);
		}
		return (Accumulator<V, A>) accumulator;
	}

	@Override
	public <S> OperatorState<S> getKeyValueState(String name, Class<S> stateType, S defaultState) {
		throw new UnsupportedOperationException(
				"This state is only accessible by functions executed on a KeyedStream");
	}

	@Override
	public <S> OperatorState<S> getKeyValueState(String name, TypeInformation<S> stateType, S defaultState) {
		throw new UnsupportedOperationException(
				"This state is only accessible by functions executed on a KeyedStream");
	}

}
