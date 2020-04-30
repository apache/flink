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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalFoldingState;

/** In memory mock internal folding state. */
@SuppressWarnings("deprecation")
class MockInternalFoldingState<K, N, T, ACC>
	extends MockInternalKvState<K, N, ACC> implements InternalFoldingState<K, N, T, ACC> {
	private final FoldFunction<T, ACC> foldFunction;

	private MockInternalFoldingState(FoldFunction<T, ACC> foldFunction) {
		this.foldFunction = foldFunction;
	}

	@Override
	public ACC get() {
		return getInternal();
	}

	@Override
	public void add(T value) throws Exception {
		updateInternal(foldFunction.fold(get(), value));
	}

	@SuppressWarnings({"unchecked", "unused"})
	static <T, N, ACC, S extends State, IS extends S> IS createState(
		TypeSerializer<N> namespaceSerializer,
		StateDescriptor<S, ACC> stateDesc) {
		FoldingStateDescriptor<T, ACC> foldingStateDesc = (FoldingStateDescriptor<T, ACC>) stateDesc;
		return (IS) new MockInternalFoldingState<>(foldingStateDesc.getFoldFunction());
	}
}
