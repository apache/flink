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

package org.apache.flink.streaming.tests.artificialstate.builder;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;

import java.util.Iterator;
import java.util.Map;

/**
 * An {@link ArtificialStateBuilder} for user {@link MapState}s.
 */
public class ArtificialMapStateBuilder<IN, K, V> extends ArtificialStateBuilder<IN> {

	private static final long serialVersionUID = -143079058769306954L;

	private transient MapState<K, V> mapState;
	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<V> valueSerializer;
	private final JoinFunction<IN, Iterator<Map.Entry<K, V>>, Iterator<Map.Entry<K, V>>> stateValueGenerator;

	public ArtificialMapStateBuilder(
		String stateName,
		JoinFunction<IN, Iterator<Map.Entry<K, V>>, Iterator<Map.Entry<K, V>>> stateValueGenerator,
		TypeSerializer<K> keySerializer,
		TypeSerializer<V> valueSerializer) {

		super(stateName);
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.stateValueGenerator = stateValueGenerator;
	}

	@Override
	public void artificialStateForElement(IN event) throws Exception {
		Iterator<Map.Entry<K, V>> update = stateValueGenerator.join(event, mapState.iterator());
		while (update.hasNext()) {
			Map.Entry<K, V> updateEntry = update.next();
			mapState.put(updateEntry.getKey(), updateEntry.getValue());
		}
	}

	@Override
	public void initialize(FunctionInitializationContext initializationContext) {
		MapStateDescriptor<K, V> mapStateDescriptor =
			new MapStateDescriptor<>(stateName, keySerializer, valueSerializer);
		mapState = initializationContext.getKeyedStateStore().getMapState(mapStateDescriptor);
	}
}
