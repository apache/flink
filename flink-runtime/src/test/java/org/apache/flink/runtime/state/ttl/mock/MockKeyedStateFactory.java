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

package org.apache.flink.runtime.state.ttl.mock;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyedStateFactory;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** State factory which produces in memory mock state objects. */
public class MockKeyedStateFactory implements KeyedStateFactory {
	@SuppressWarnings("deprecation")
	private static final Map<Class<? extends StateDescriptor>, KeyedStateFactory> STATE_FACTORIES =
		Stream.of(
			Tuple2.of(ValueStateDescriptor.class, (KeyedStateFactory) MockInternalValueState::createState),
			Tuple2.of(ListStateDescriptor.class, (KeyedStateFactory) MockInternalListState::createState),
			Tuple2.of(MapStateDescriptor.class, (KeyedStateFactory) MockInternalMapState::createState),
			Tuple2.of(ReducingStateDescriptor.class, (KeyedStateFactory) MockInternalReducingState::createState),
			Tuple2.of(AggregatingStateDescriptor.class, (KeyedStateFactory) MockInternalAggregatingState::createState),
			Tuple2.of(FoldingStateDescriptor.class, (KeyedStateFactory) MockInternalFoldingState::createState)
		).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

	@Override
	public <N, SV, S extends State, IS extends S> IS createState(
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, SV> stateDesc) throws Exception {
		KeyedStateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
		if (stateFactory == null) {
			String message = String.format("State %s is not supported by %s",
				stateDesc.getClass(), TtlStateFactory.class);
			throw new FlinkRuntimeException(message);
		}
		return stateFactory.createState(namespaceSerializer, stateDesc);
	}
}
