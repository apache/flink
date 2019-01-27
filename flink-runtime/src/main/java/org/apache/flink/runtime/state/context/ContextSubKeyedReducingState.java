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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * used for reducing state.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <T> The type of elements in the aggregated by the ReduceFunction
 */
public class ContextSubKeyedReducingState<K, N, T>
	implements ContextSubKeyedAppendingState<K, N, T, T, T>, InternalReducingState<K, N, T> {

	private N namespace;

	private final KeyContextImpl<K> operator;

	private final SubKeyedValueState<Object, N, T> subKeyedValueState;

	private final ReduceTransformation transformation;

	public ContextSubKeyedReducingState(
		KeyContextImpl<K> operator,
		SubKeyedValueState<Object, N, T> subKeyedValueState,
		ReduceFunction<T> reduceFunction) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(subKeyedValueState);
		Preconditions.checkNotNull(reduceFunction);
		this.operator = operator;
		this.subKeyedValueState = subKeyedValueState;
		this.transformation = new ReduceTransformation(reduceFunction);
	}

	@Override
	public T get() {
		return subKeyedValueState.get(getCurrentKey(), namespace);
	}

	@Override
	public void add(T value) {
		subKeyedValueState.transform(operator.getCurrentKey(), namespace, value, transformation);
	}

	@Override
	public void clear() {
		subKeyedValueState.remove(getCurrentKey(), namespace);
	}

	private Object getCurrentKey() {
		return operator.getCurrentKey();
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources == null || sources.isEmpty()) {
			return; // nothing to do
		}

		Object currentKey = getCurrentKey();
		T merged = null;

		// merge the sources
		for (N source : sources) {

			// get and remove the next source per namespace/key
			T sourceState = subKeyedValueState.getAndRemove(currentKey, source);

			if (merged != null && sourceState != null) {
				merged = transformation.reduceFunction.reduce(merged, sourceState);
			} else if (merged == null) {
				merged = sourceState;
			}
		}

		// merge into the target, if needed
		if (merged != null) {
			subKeyedValueState.transform(currentKey, target, merged, transformation);
		}
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
	public TypeSerializer getValueSerializer() {
		return subKeyedValueState.getDescriptor().getValueSerializer();
	}

	@Override
	public void setCurrentNamespace(N namespace) {
		this.namespace = namespace;
	}

	@Override
	public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<T> safeValueSerializer) throws Exception {
		return new byte[0];
	}

	private class ReduceTransformation implements StateTransformationFunction<T, T> {

		private final ReduceFunction<T> reduceFunction;

		public ReduceTransformation(ReduceFunction<T> reduceFunction) {
			this.reduceFunction = Preconditions.checkNotNull(reduceFunction);
		}

		@Override
		public T apply(T previousState, T value) throws Exception {
			return previousState == null ? value : reduceFunction.reduce(previousState, value);
		}
	}
}
