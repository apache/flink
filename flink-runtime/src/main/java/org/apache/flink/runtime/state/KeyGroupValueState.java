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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.KeyGroupAssigner;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.IOException;

public class KeyGroupValueState<K, N, V> extends KeyGroupKVState<K, N, ValueState<V>, ValueStateDescriptor<V>> implements ValueState<V> {

	public KeyGroupValueState(
		KeyGroupStateBackend backend,
		KeyGroupAssigner<K> keyGroupAssigner,
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<ValueState<V>, ?> stateDescriptor) {
		super(backend, keyGroupAssigner, namespaceSerializer, stateDescriptor);
	}

	@Override
	public V value() throws IOException {
		return state.value();
	}

	@Override
	public void update(V value) throws IOException {
		state.update(value);
	}

	@Override
	public void clear() {
		state.clear();
	}
}
