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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.TimeServiceProvider;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@SuppressWarnings("deprecation")
public class MockRuntimeContext extends StreamingRuntimeContext {

	private final int numberOfParallelSubtasks;
	private final int indexOfThisSubtask;

	private final ExecutionConfig execConfig;

	private final TimeServiceProvider timeServiceProvider;
	
	public MockRuntimeContext(int numberOfParallelSubtasks, int indexOfThisSubtask) {
		this(numberOfParallelSubtasks, indexOfThisSubtask, new ExecutionConfig());
	}

	public MockRuntimeContext(
			int numberOfParallelSubtasks,
			int indexOfThisSubtask,
			ExecutionConfig execConfig) {
		this(numberOfParallelSubtasks, indexOfThisSubtask, execConfig, null);
	}
	
	public MockRuntimeContext(
			int numberOfParallelSubtasks,
			int indexOfThisSubtask,
			ExecutionConfig execConfig,
			TimeServiceProvider timeServiceProvider) {
		
		super(new MockStreamOperator(),
			new MockEnvironment("no", 4 * MemoryManager.DEFAULT_PAGE_SIZE, null, 16),
			Collections.<String, Accumulator<?, ?>>emptyMap());

		this.numberOfParallelSubtasks = numberOfParallelSubtasks;
		this.indexOfThisSubtask = indexOfThisSubtask;
		this.execConfig = execConfig;
		this.timeServiceProvider = timeServiceProvider;
	}

	@Override
	public boolean isCheckpointingEnabled() {
		return true;
	}

	@Override
	public String getTaskName() {
		return "mock task";
	}

	@Override
	public int getNumberOfParallelSubtasks() {
		return numberOfParallelSubtasks;
	}

	@Override
	public int getIndexOfThisSubtask() {
		return indexOfThisSubtask;
	}

	@Override
	public int getAttemptNumber() {
		return 0;
	}

	@Override
	public ExecutionConfig getExecutionConfig() {
		return execConfig;
	}

	@Override
	public ClassLoader getUserCodeClassLoader() {
		return getClass().getClassLoader();
	}

	@Override
	public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {
		// noop
	}

	@Override
	public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Accumulator<?, ?>> getAllAccumulators() {
		throw new UnsupportedOperationException();
	}

	@Override
	public IntCounter getIntCounter(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public LongCounter getLongCounter(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DoubleCounter getDoubleCounter(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Histogram getHistogram(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MetricGroup getMetricGroup() {
		return new UnregisteredTaskMetricsGroup.DummyIOMetricGroup();
	}

	@Override
	public <RT> List<RT> getBroadcastVariable(String name) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
		throw new UnsupportedOperationException();
	}

	@Override
	public DistributedCache getDistributedCache() {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
		throw new UnsupportedOperationException();
	}

	@Override
	public TimeServiceProvider getTimeServiceProvider() {
		if (timeServiceProvider == null) {
			throw new UnsupportedOperationException();
		} else {
			return timeServiceProvider;
		}
	}

	// ------------------------------------------------------------------------

	private static class MockStreamOperator extends AbstractStreamOperator<Integer> {
		private static final long serialVersionUID = -1153976702711944427L;

		@Override
		public ExecutionConfig getExecutionConfig() {
			return new ExecutionConfig();
		}
	}
}
