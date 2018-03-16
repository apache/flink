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

package org.apache.flink.queryablestate.client.state;

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
import org.apache.flink.api.common.state.StateBinder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Preconditions;

/**
 * A {@link StateBinder} used to deserialize the results returned by the
 * {@link org.apache.flink.queryablestate.client.QueryableStateClient}.
 *
 * <p>The result is an immutable {@link org.apache.flink.api.common.state.State State}
 * object containing the requested result.
 */
public class ImmutableStateBinder implements StateBinder {

	private final byte[] serializedState;

	public ImmutableStateBinder(final byte[] content) {
		serializedState = Preconditions.checkNotNull(content);
	}

	@Override
	public <T> ValueState<T> createValueState(ValueStateDescriptor<T> stateDesc) throws Exception {
		return ImmutableValueState.createState(stateDesc, serializedState);
	}

	@Override
	public <T> ListState<T> createListState(ListStateDescriptor<T> stateDesc) throws Exception {
		return ImmutableListState.createState(stateDesc, serializedState);
	}

	@Override
	public <T> ReducingState<T> createReducingState(ReducingStateDescriptor<T> stateDesc) throws Exception {
		return ImmutableReducingState.createState(stateDesc, serializedState);
	}

	@Override
	public <IN, ACC, OUT> AggregatingState<IN, OUT> createAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateDesc) throws Exception {
		return ImmutableAggregatingState.createState(stateDesc, serializedState);
	}

	@Override
	public <T, ACC> FoldingState<T, ACC> createFoldingState(FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
		return ImmutableFoldingState.createState(stateDesc, serializedState);
	}

	@Override
	public <MK, MV> MapState<MK, MV> createMapState(MapStateDescriptor<MK, MV> stateDesc) throws Exception {
		return ImmutableMapState.createState(stateDesc, serializedState);
	}
}
