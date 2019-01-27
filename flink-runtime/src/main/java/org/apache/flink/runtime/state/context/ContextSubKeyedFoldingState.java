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

package org.apache.flink.runtime.state.context;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.util.Preconditions;

/**
 * used for folding state.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> Type of the values folded into the state
 * @param <ACC> Type of the value in the state
 */
public class ContextSubKeyedFoldingState<K, N, T, ACC>
	implements ContextSubKeyedAppendingState<K, N, T, ACC, ACC>, InternalFoldingState<K, N, T, ACC> {

	private N namespace;

	private final KeyContextImpl<K> operator;

	private final SubKeyedValueState<Object, N, ACC> subKeyedValueState;

	private final FoldingStateDescriptor<T, ACC> stateDescriptor;

	private final FoldTransformation foldTransformation;

	public ContextSubKeyedFoldingState(
		KeyContextImpl<K> operator,
		SubKeyedValueState<Object, N, ACC> subKeyedValueState,
		FoldingStateDescriptor<T, ACC> stateDescriptor) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(subKeyedValueState);
		Preconditions.checkNotNull(stateDescriptor);
		this.operator = operator;
		this.subKeyedValueState = subKeyedValueState;
		this.stateDescriptor = stateDescriptor;
		this.foldTransformation = new FoldTransformation(stateDescriptor.getFoldFunction());
	}

	@Override
	public ACC get() {
		return subKeyedValueState.get(operator.getCurrentKey(), namespace);
	}

	@Override
	public void add(T value) {
		subKeyedValueState.transform(operator.getCurrentKey(), namespace, value, foldTransformation);
	}

	@Override
	public void clear() {
		subKeyedValueState.remove(operator.getCurrentKey(), namespace);
	}

	private ACC getInitialValue() {
		return stateDescriptor.getInitialValue();
	}

	@Override
	public SubKeyedState getSubKeyedState() {
		return subKeyedValueState;
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return operator.getKeySerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return subKeyedValueState.getDescriptor().getNamespaceSerializer();
	}

	@Override
	public TypeSerializer<ACC> getValueSerializer() {
		return subKeyedValueState.getDescriptor().getValueSerializer();
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.namespace = namespace;
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<ACC> safeValueSerializer) throws Exception {
		return new byte[0];
	}

	private final class FoldTransformation implements StateTransformationFunction<ACC, T> {

		private final FoldFunction<T, ACC> foldFunction;

		FoldTransformation(FoldFunction<T, ACC> foldFunction) {
			this.foldFunction = Preconditions.checkNotNull(foldFunction);
		}

		@Override
		public ACC apply(ACC previousState, T value) throws Exception {
			return foldFunction.fold((previousState != null) ? previousState : getInitialValue(), value);
		}
	}
}
