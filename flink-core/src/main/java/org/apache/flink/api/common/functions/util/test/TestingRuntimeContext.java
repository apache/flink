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

package org.apache.flink.api.common.functions.util.test;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Testing class for user-defined functions testing.
 */
@SuppressWarnings("unchecked")
public class TestingRuntimeContext implements RuntimeContext {

	private MetricGroup metricGroup;
	private ExecutionConfig executionConfig;
	private ClassLoader userCodeClassLoader;
	private Map<StateDescriptor<?, ?>, State> ctxStates = new HashMap<>();
	private DistributedCache distributedCache;
	private TaskInfo taskInfo;
	private boolean isStreaming;

	// Broadcast variables
	private final HashMap<String, Object> initializedBroadcastVars = new HashMap<String, Object>();
	private final HashMap<String, List<?>> uninitializedBroadcastVars = new HashMap<String, List<?>>();

	// Accumulators
	private final Map<String, Accumulator<?, ?>> accumulators = new HashMap<>();

	// Collector
	private TestingCollector<?> collector = new TestingCollector<>();
	private Map<OutputTag<?>, List<?>> sideOutput = new HashMap<>();

	public TestingRuntimeContext(boolean isStreaming) {
		this.isStreaming = isStreaming;
	}

	public void setTaskInfo(TaskInfo taskInfo) {
		this.taskInfo = taskInfo;
	}

	public void setMetricGroup(MetricGroup metricGroup) {
		this.metricGroup = metricGroup;
	}

	public void setExecutionConfig(ExecutionConfig executionConfig) {
		this.executionConfig = executionConfig;
	}

	public void setUserCodeClassLoader(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = userCodeClassLoader;
	}

	public void setDistributedCache(DistributedCache distributedCache) {
		this.distributedCache = distributedCache;
	}

	@Override
	public String getTaskName() {
		return taskInfo.getTaskName();
	}

	@Override
	public MetricGroup getMetricGroup() {
		return metricGroup;
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
	public int getAttemptNumber() {
		return taskInfo.getAttemptNumber();
	}

	@Override
	public String getTaskNameWithSubtasks() {
		return taskInfo.getTaskNameWithSubtasks();
	}

	@Override
	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	@Override
	public ClassLoader getUserCodeClassLoader() {
		return userCodeClassLoader;
	}

	@Override
	public DistributedCache getDistributedCache() {
		return distributedCache;
	}

	// Accumulators operations.

	@Override
	public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {
		if (accumulators.containsKey(name)) {
			throw new UnsupportedOperationException("The accumulator '" + name
				+ "' already exists and cannot be added.");
		}
		accumulators.put(name, accumulator);
	}

	@Override
	public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
		return (Accumulator<V, A>) accumulators.get(name);
	}

	@Override
	public Map<String, Accumulator<?, ?>> getAllAccumulators() {
		return Collections.unmodifiableMap(this.accumulators);
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

	@SuppressWarnings("unchecked")
	private <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name, Class<? extends Accumulator<V, A>> accumulatorClass) {
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

	// Broadcast operations.

	@Override
	public boolean hasBroadcastVariable(String name) {
		if (isStreaming) {
			throw new UnsupportedOperationException("This broadcastVariable is only accessible by functions executed on a DataSet");
		}
		return false;
	}

	@Override
	public <RT> List<RT> getBroadcastVariable(String name) {
		if (isStreaming) {
			throw new UnsupportedOperationException("This broadcastVariable is only accessible by functions executed on a DataSet");
		}
		// check if we have an initialized version
		Object o = this.initializedBroadcastVars.get(name);
		if (o != null) {
			if (o instanceof List) {
				return (List<RT>) o;
			}
			else {
				throw new IllegalStateException("The broadcast variable with name '" + name +
					"' is not a List. A different call must have requested this variable with a BroadcastVariableInitializer.");
			}
		}
		else {
			List<?> uninitialized = this.uninitializedBroadcastVars.remove(name);
			if (uninitialized != null) {
				this.initializedBroadcastVars.put(name, uninitialized);
				return (List<RT>) uninitialized;
			}
			else {
				throw new IllegalArgumentException("The broadcast variable with name '" + name + "' has not been set.");
			}
		}
	}

	@Override
	public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
		if (isStreaming) {
			throw new UnsupportedOperationException("This broadcastVariable is only accessible by functions executed on a DataSet");
		}
		// check if we have an initialized version
		Object o = this.initializedBroadcastVars.get(name);
		if (o != null) {
			return (C) o;
		}
		else {
			List<T> uninitialized = (List<T>) this.uninitializedBroadcastVars.remove(name);
			if (uninitialized != null) {
				C result = initializer.initializeBroadcastVariable(uninitialized);
				this.initializedBroadcastVars.put(name, result);
				return result;
			}
			else {
				throw new IllegalArgumentException("The broadcast variable with name '" + name + "' has not been set.");
			}
		}
	}

	public void setBroadcastVariable(String name, List<?> value) {
		if (isStreaming) {
			throw new UnsupportedOperationException("This broadcastVariable is only accessible by functions executed on a DataSet");
		}
		this.uninitializedBroadcastVars.put(name, value);
		this.initializedBroadcastVars.remove(name);
	}

	public void clearBroadcastVariable(String name) {
		if (isStreaming) {
			throw new UnsupportedOperationException("This broadcastVariable is only accessible by functions executed on a DataSet");
		}
		this.uninitializedBroadcastVars.remove(name);
		this.initializedBroadcastVars.remove(name);
	}

	public void clearAllBroadcastVariables() {
		if (isStreaming) {
			throw new UnsupportedOperationException("This broadcastVariable is only accessible by functions executed on a DataSet");
		}
		this.uninitializedBroadcastVars.clear();
		this.initializedBroadcastVars.clear();
	}


	// State operations.

	public <S extends State, T> void setState(StateDescriptor<S, T> stateProperties, State state) {
		ctxStates.put(stateProperties, state);
	}

	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
		if (isStreaming) {
			return (ValueState<T>) ctxStates.get(stateProperties);
		} else {
			throw new UnsupportedOperationException("This state is only accessible by functions executed on a KeyedStream");
		}
	}

	@Override
	public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
		if (isStreaming) {

			return (ListState<T>) ctxStates.get(stateProperties);
		} else {
			throw new UnsupportedOperationException("This state is only accessible by functions executed on a KeyedStream");
		}
	}

	@Override
	public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
		if (isStreaming) {
			return (ReducingState<T>) ctxStates.get(stateProperties);
		} else {
			throw new UnsupportedOperationException("This state is only accessible by functions executed on a KeyedStream");
		}
	}

	@Override
	public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
		if (isStreaming) {
			return (AggregatingState<IN, OUT>) ctxStates.get(stateProperties);
		} else {
			throw new UnsupportedOperationException("This state is only accessible by functions executed on a KeyedStream");
		}
	}

	@Override
	public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
		if (isStreaming) {
			return (FoldingState<T, ACC>) ctxStates.get(stateProperties);
		} else {
			throw new UnsupportedOperationException("This state is only accessible by functions executed on a KeyedStream");
		}
	}

	@Override
	public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
		if (isStreaming) {
			return (MapState<UK, UV>) ctxStates.get(stateProperties);
		} else {
			throw new UnsupportedOperationException("This state is only accessible by functions executed on a KeyedStream");
		}
	}

	public <T> TestingCollector<T> getCollector() {
		return (TestingCollector<T>) collector;
	}

	public <T> List<T> getCollectorOutput() {
		return (List<T>) collector.output;
	}

	public <T> void addSideOutput(OutputTag<T> tag, T value) {
		if (sideOutput.containsKey(tag)) {
			List<T> originList = (List<T>) sideOutput.get(tag);
			originList.add(value);
			sideOutput.put(tag, originList);
		} else {
			sideOutput.put(tag, Collections.singletonList(value));
		}
	}

	public <T> List<T> getSideOutput(OutputTag<T> tag) {
		return (List<T>) sideOutput.getOrDefault(tag, Collections.emptyList());
	}

}
