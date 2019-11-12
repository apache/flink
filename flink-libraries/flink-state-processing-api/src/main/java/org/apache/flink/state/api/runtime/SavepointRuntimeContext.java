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

package org.apache.flink.state.api.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
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
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A streaming {@link RuntimeContext} which delegates to the underlying batch {@code RuntimeContext}
 * along with a specified {@link KeyedStateStore}.
 *
 * <p>This {@code RuntimeContext} has the ability to force eager state registration by
 * throwing an exception if state is registered outside of open.
 */
@Internal
public final class SavepointRuntimeContext implements RuntimeContext {
	private static final String REGISTRATION_EXCEPTION_MSG =
		"State Descriptors may only be registered inside of open";

	private final RuntimeContext ctx;

	private final KeyedStateStore keyedStateStore;

	private final List<StateDescriptor<?, ?>> registeredDescriptors;

	private boolean stateRegistrationAllowed;

	public SavepointRuntimeContext(RuntimeContext ctx, KeyedStateStore keyedStateStore) {
		this.ctx = Preconditions.checkNotNull(ctx);
		this.keyedStateStore = Preconditions.checkNotNull(keyedStateStore);
		this.stateRegistrationAllowed = true;

		this.registeredDescriptors = new ArrayList<>();
	}

	@Override
	public String getTaskName() {
		return ctx.getTaskName();
	}

	@Override
	public MetricGroup getMetricGroup() {
		return ctx.getMetricGroup();
	}

	@Override
	public int getNumberOfParallelSubtasks() {
		return ctx.getNumberOfParallelSubtasks();
	}

	@Override
	public int getMaxNumberOfParallelSubtasks() {
		return ctx.getMaxNumberOfParallelSubtasks();
	}

	@Override
	public int getIndexOfThisSubtask() {
		return ctx.getIndexOfThisSubtask();
	}

	@Override
	public int getAttemptNumber() {
		return ctx.getAttemptNumber();
	}

	@Override
	public String getTaskNameWithSubtasks() {
		return ctx.getTaskNameWithSubtasks();
	}

	@Override
	public ExecutionConfig getExecutionConfig() {
		return ctx.getExecutionConfig();
	}

	@Override
	public ClassLoader getUserCodeClassLoader() {
		return ctx.getUserCodeClassLoader();
	}

	@Override
	public <V, A extends Serializable> void addAccumulator(
		String name, Accumulator<V, A> accumulator) {
		ctx.addAccumulator(name, accumulator);
	}

	@Override
	public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
		return ctx.getAccumulator(name);
	}

	@Override
	@Deprecated
	public Map<String, Accumulator<?, ?>> getAllAccumulators() {
		return ctx.getAllAccumulators();
	}

	@Override
	public IntCounter getIntCounter(String name) {
		return ctx.getIntCounter(name);
	}

	@Override
	public LongCounter getLongCounter(String name) {
		return ctx.getLongCounter(name);
	}

	@Override
	public DoubleCounter getDoubleCounter(String name) {
		return ctx.getDoubleCounter(name);
	}

	@Override
	public Histogram getHistogram(String name) {
		return ctx.getHistogram(name);
	}

	@Override
	public boolean hasBroadcastVariable(String name) {
		return ctx.hasBroadcastVariable(name);
	}

	@Override
	public <RT> List<RT> getBroadcastVariable(String name) {
		return ctx.getBroadcastVariable(name);
	}

	@Override
	public <T, C> C getBroadcastVariableWithInitializer(
		String name, BroadcastVariableInitializer<T, C> initializer) {
		return ctx.getBroadcastVariableWithInitializer(name, initializer);
	}

	@Override
	public DistributedCache getDistributedCache() {
		return ctx.getDistributedCache();
	}

	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
		if (!stateRegistrationAllowed) {
			throw new RuntimeException(REGISTRATION_EXCEPTION_MSG);
		}

		registeredDescriptors.add(stateProperties);
		return keyedStateStore.getState(stateProperties);
	}

	@Override
	public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
		if (!stateRegistrationAllowed) {
			throw new RuntimeException(REGISTRATION_EXCEPTION_MSG);
		}

		registeredDescriptors.add(stateProperties);
		return keyedStateStore.getListState(stateProperties);
	}

	@Override
	public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
		if (!stateRegistrationAllowed) {
			throw new RuntimeException(REGISTRATION_EXCEPTION_MSG);
		}

		registeredDescriptors.add(stateProperties);
		return keyedStateStore.getReducingState(stateProperties);
	}

	@Override
	public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
		if (!stateRegistrationAllowed) {
			throw new RuntimeException(REGISTRATION_EXCEPTION_MSG);
		}

		registeredDescriptors.add(stateProperties);
		return keyedStateStore.getAggregatingState(stateProperties);
	}

	@Override
	@Deprecated
	public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
		if (!stateRegistrationAllowed) {
			throw new RuntimeException(REGISTRATION_EXCEPTION_MSG);
		}

		registeredDescriptors.add(stateProperties);
		return keyedStateStore.getFoldingState(stateProperties);
	}

	@Override
	public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
		if (!stateRegistrationAllowed) {
			throw new RuntimeException(REGISTRATION_EXCEPTION_MSG);
		}

		registeredDescriptors.add(stateProperties);
		return keyedStateStore.getMapState(stateProperties);
	}

	public List<StateDescriptor<?, ?>> getStateDescriptors() {
		if (registeredDescriptors.isEmpty()) {
			return Collections.emptyList();
		}
		return new ArrayList<>(registeredDescriptors);
	}

	public void disableStateRegistration() throws Exception {
		stateRegistrationAllowed = false;
	}
}

