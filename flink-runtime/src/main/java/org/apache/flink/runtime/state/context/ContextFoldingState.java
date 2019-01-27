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
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalFoldingState;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.util.Preconditions;

/**
 * An implementation of {@link FoldingState} which is backed by a
 * {@link KeyedValueState}. The values of the states depend on the current key
 * of the keyContext. That is, when the current key of the keyContext changes, the
 * values accessed will be changed as well.
 *
 * @param <T> The type of the values that can be folded into the state.
 * @param <ACC> The type of the value in the folding state.
 */
public class ContextFoldingState<K, T, ACC>
	implements ContextKeyedState<K, ACC>, InternalFoldingState<K, VoidNamespace, T, ACC> {

	/** The keyContext to which the state belongs. */
	private final KeyContextImpl<K> keyContext;

	/** The keyed state backing the state. */
	private final KeyedValueState<Object, ACC> keyedState;

	/** The descriptor of the state. */
	private final FoldingStateDescriptor<T, ACC> stateDescriptor;

	/** The transformation for the state. */
	private final FoldTransformation foldTransformation;

	public ContextFoldingState(
		final KeyContextImpl<K> keyContext,
		final KeyedValueState<Object, ACC> keyedState,
		final FoldingStateDescriptor<T, ACC> stateDescriptor
	) {
		Preconditions.checkNotNull(keyContext);
		Preconditions.checkNotNull(keyedState);
		Preconditions.checkNotNull(stateDescriptor);

		this.keyContext = keyContext;
		this.keyedState = keyedState;
		this.stateDescriptor = stateDescriptor;
		this.foldTransformation = new FoldTransformation(stateDescriptor.getFoldFunction());
	}

	@Override
	public ACC get() {
		return keyedState.get(keyContext.getCurrentKey());
	}

	@Override
	public void add(T value) {
		keyedState.transform(keyContext.getCurrentKey(), value, foldTransformation);
	}

	@Override
	public void clear() {
		keyedState.remove(keyContext.getCurrentKey());
	}

	private ACC getInitialValue() {
		return stateDescriptor.getInitialValue();
	}

	@Override
	public KeyedState getKeyedState() {
		return keyedState;
	}

	@Override
	public TypeSerializer getKeySerializer() {
		return keyContext.getKeySerializer();
	}

	@Override
	public TypeSerializer<ACC> getValueSerializer() {
		return keyedState.getDescriptor().getValueSerializer();
	}

	@Override
	public byte[] getSerializedValue(
		byte[] serializedKeyAndNamespace,
		TypeSerializer safeKeySerializer,
		TypeSerializer safeNamespaceSerializer,
		TypeSerializer safeValueSerializer) throws Exception {

		return keyedState.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer, safeValueSerializer);
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
