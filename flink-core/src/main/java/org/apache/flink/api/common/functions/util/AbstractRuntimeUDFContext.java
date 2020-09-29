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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.Future;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A standalone implementation of the {@link RuntimeContext}, created by runtime UDF operators.
 */
@Internal
public abstract class AbstractRuntimeUDFContext implements RuntimeContext {

	private final TaskInfo taskInfo;

	private final UserCodeClassLoader userCodeClassLoader;

	private final ExecutionConfig executionConfig;

	private final Map<String, Accumulator<?, ?>> accumulators;

	private final DistributedCache distributedCache;

	private final MetricGroup metrics;

	public AbstractRuntimeUDFContext(TaskInfo taskInfo,
										UserCodeClassLoader userCodeClassLoader,
										ExecutionConfig executionConfig,
										Map<String, Accumulator<?, ?>> accumulators,
										Map<String, Future<Path>> cpTasks,
										MetricGroup metrics) {
		this.taskInfo = checkNotNull(taskInfo);
		this.userCodeClassLoader = userCodeClassLoader;
		this.executionConfig = executionConfig;
		this.distributedCache = new DistributedCache(checkNotNull(cpTasks));
		this.accumulators = checkNotNull(accumulators);
		this.metrics = metrics;
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
	public int getMaxNumberOfParallelSubtasks() {
		return taskInfo.getMaxNumberOfParallelSubtasks();
	}

	@Override
	public int getIndexOfThisSubtask() {
		return taskInfo.getIndexOfThisSubtask();
	}

	@Override
	public MetricGroup getMetricGroup() {
		return metrics;
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
	public ClassLoader getUserCodeClassLoader() {
		return this.userCodeClassLoader.asClassLoader();
	}

	@Override
	public void registerUserCodeClassLoaderReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {
		userCodeClassLoader.registerReleaseHookIfAbsent(releaseHookName, releaseHook);
	}

	@Override
	public DistributedCache getDistributedCache() {
		return this.distributedCache;
	}

	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name,
			Class<? extends Accumulator<V, A>> accumulatorClass) {

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
	@PublicEvolving
	public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
		throw new UnsupportedOperationException(
				"This state is only accessible by functions executed on a KeyedStream");
	}

	@Override
	@PublicEvolving
	public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
		throw new UnsupportedOperationException(
				"This state is only accessible by functions executed on a KeyedStream");
	}

	@Override
	@PublicEvolving
	public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
		throw new UnsupportedOperationException(
				"This state is only accessible by functions executed on a KeyedStream");
	}

	@Override
	@PublicEvolving
	public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
		throw new UnsupportedOperationException(
				"This state is only accessible by functions executed on a KeyedStream");
	}

	@Override
	@PublicEvolving
	public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
		throw new UnsupportedOperationException(
				"This state is only accessible by functions executed on a KeyedStream");
	}

	@Internal
	@VisibleForTesting
	public String getAllocationIDAsString() {
		return taskInfo.getAllocationIDAsString();
	}
}
