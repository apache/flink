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

package org.apache.flink.runtime.state.memory;

import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.AbstractPartitionedStateBackend;

public class PartitionedMemoryStateBackend<KEY> extends AbstractPartitionedStateBackend<KEY> {

	public PartitionedMemoryStateBackend(TypeSerializer<KEY> keySerializer, ClassLoader classLoader) {
		super(keySerializer, classLoader);
	}

	@Override
	public void disposeAllStateForCurrentJob() throws Exception {
		// nothing to do here, GC will do it
	}

	@Override
	public <N, V> ValueState<V> createValueState(TypeSerializer<N> namespaceSerializer, ValueStateDescriptor<V> stateDesc) throws Exception {
		return new MemValueState<>(keySerializer, namespaceSerializer, stateDesc);
	}

	@Override
	public <N, T> ListState<T> createListState(TypeSerializer<N> namespaceSerializer, ListStateDescriptor<T> stateDesc) throws Exception {
		return new MemListState<>(keySerializer, namespaceSerializer, stateDesc);
	}

	@Override
	public <N, T> ReducingState<T> createReducingState(TypeSerializer<N> namespaceSerializer, ReducingStateDescriptor<T> stateDesc) throws Exception {
		return new MemReducingState<>(keySerializer, namespaceSerializer, stateDesc);
	}

	@Override
	public <N, T, ACC> FoldingState<T, ACC> createFoldingState(TypeSerializer<N> namespaceSerializer, FoldingStateDescriptor<T, ACC> stateDesc) throws Exception {
		return new MemFoldingState<>(keySerializer, namespaceSerializer, stateDesc);
	}

	@Override
	public void close() throws Exception {

	}
}
