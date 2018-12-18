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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalReducingState;

/** In memory mock internal reducing state. */
class MockInternalReducingState<K, N, T>
	extends MockInternalMergingState<K, N, T, T, T> implements InternalReducingState<K, N, T> {
	private final ReduceFunction<T> reduceFunction;

	private MockInternalReducingState(ReduceFunction<T> reduceFunction) {
		this.reduceFunction = reduceFunction;
	}

	@Override
	public T get() {
		return getInternal();
	}

	@Override
	public void add(T value) throws Exception {
		updateInternal(reduceFunction.reduce(get(), value));
	}

	@Override
	T mergeState(T t, T nAcc) throws Exception {
		return reduceFunction.reduce(t, nAcc);
	}

	@SuppressWarnings({"unchecked", "unused"})
	static <N, T, S extends State, IS extends S> IS createState(
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, T> stateDesc) {
		ReducingStateDescriptor<T> reducingStateDesc = (ReducingStateDescriptor<T>) stateDesc;
		return (IS) new MockInternalReducingState<>(reducingStateDesc.getReduceFunction());
	}
}
