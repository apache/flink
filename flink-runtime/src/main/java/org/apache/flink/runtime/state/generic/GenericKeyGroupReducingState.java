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

package org.apache.flink.runtime.state.generic;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Generic key group {@link ReducingState} implementation returned by the
 * {@link GenericKeyGroupStateBackend}. This class is a proxy for reducing states which are backed
 * by different {@link org.apache.flink.runtime.state.PartitionedStateBackend}.
 *
 * @param <K> Type of the key
 * @param <T> Type of the elements to be reduced and the reduction result
 * @param <N> Type of the namespace
 */
public class GenericKeyGroupReducingState<K, T, N> extends GenericKeyGroupKVState<K, T, N, ReducingState<T>> implements ReducingState<T> {

	public GenericKeyGroupReducingState(ReducingStateDescriptor<T> stateDescriptor, TypeSerializer<N> namespaceSerializer) {
		super(stateDescriptor, namespaceSerializer);
	}

	@Override
	public T get() throws Exception {
		if (state != null) {
			return state.get();
		} else {
			throw new RuntimeException("Could not retrieve the state's value, because the state has not been set.");
		}
	}

	@Override
	public void add(T value) throws Exception {
		if (state != null) {
			state.add(value);
		} else {
			throw new RuntimeException("Could not update the state's value, because the state has not been set.");
		}
	}

	@Override
	public void clear() {
		if (state != null) {
			state.clear();
		} else {
			throw new RuntimeException("Could not clear the state, because the state has not been set.");
		}
	}
}
