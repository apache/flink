/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.state.util.DuplicateStateNameException;
import org.apache.flink.util.Preconditions;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation of KeyedStateStore that currently forwards state registration to a {@link RuntimeContext}.
 */
public class DefaultKeyedStateStore implements KeyedStateStore {

	private final KeyedStateBackend<?> keyedStateBackend;
	private final ExecutionConfig executionConfig;

	public DefaultKeyedStateStore(KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
		this.keyedStateBackend = Preconditions.checkNotNull(keyedStateBackend);
		this.executionConfig = Preconditions.checkNotNull(executionConfig);
	}

	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
		try {
			return getPartitionedState(stateProperties);
		} catch (ClassCastException cce) {
			throw new DuplicateStateNameException("Duplicate state name: " + stateProperties, cce);
		}
	}

	@Override
	public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
		try {
			return new UserFacingListState<>(getPartitionedState(stateProperties));
		} catch (ClassCastException cce) {
			throw new DuplicateStateNameException("Duplicate state name: " + stateProperties, cce);
		}
	}

	@Override
	public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
		try {
			return getPartitionedState(stateProperties);
		} catch (ClassCastException cce) {
			throw new DuplicateStateNameException("Duplicate state name: " + stateProperties, cce);
		}
	}

	@Override
	public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
		try {
			return getPartitionedState(stateProperties);
		} catch (ClassCastException cce) {
			throw new DuplicateStateNameException("Duplicate state name: " + stateProperties, cce);
		}
	}

	@Override
	public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
		try {
			return getPartitionedState(stateProperties);
		} catch (ClassCastException cce) {
			throw new DuplicateStateNameException("Duplicate state name: " + stateProperties, cce);
		}
	}

	@Override
	public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
		try {
			return new UserFacingMapState<>(getPartitionedState(stateProperties));
		} catch (ClassCastException cce) {
			throw new DuplicateStateNameException("Duplicate state name: " + stateProperties, cce);
		}
	}

	private <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) {
		requireNonNull(stateDescriptor, "The state properties must not be null");
		try {
			stateDescriptor.initializeSerializerUnlessSet(executionConfig);
			return keyedStateBackend.getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					stateDescriptor);
		} catch (ClassCastException cce) {
			throw cce;
		} catch (Exception e) {
			throw new RuntimeException("Error while getting state", e);
		}
	}
}
