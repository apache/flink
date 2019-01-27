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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * An implementation of {@link AggregatingState} which is backed by a
 * {@link KeyedValueState}. The values of the states depend on the current key
 * of the keyContext. That is, when the current key of the keyContext changes, the
 * values accessed will be changed as well.
 *
 * @param <IN> The type of the values aggregated by the state.
 * @param <ACC> The type of the values stored in the state.
 * @param <OUT> The type of the values returned by the state.
 */
public class ContextAggregatingState<K, IN, ACC, OUT>
	implements ContextKeyedState<K, ACC>, InternalAggregatingState<K, VoidNamespace, IN, ACC, OUT> {

	/** The keyContext to which the state belongs. */
	private final KeyContextImpl<K> keyContext;

	/** The keyed state backing the state. */
	private final KeyedValueState<Object, ACC> keyedState;

	/** The transformation for the state. */
	private final AggregateTransformation transformation;

	public ContextAggregatingState(
		final KeyContextImpl<K> keyContext,
		final KeyedValueState<Object, ACC> keyedState,
		final AggregateFunction<IN, ACC, OUT> aggregateFunction
	) {
		Preconditions.checkNotNull(keyContext);
		Preconditions.checkNotNull(keyedState);
		Preconditions.checkNotNull(aggregateFunction);

		this.keyContext = keyContext;
		this.keyedState = keyedState;
		this.transformation = new AggregateTransformation(aggregateFunction);
	}

	@Override
	public OUT get() {
		ACC accumulator = keyedState.get(keyContext.getCurrentKey());
		return accumulator == null ? null : transformation.aggregateFunction.getResult(accumulator);
	}

	@Override
	public void add(IN value) {
		keyedState.transform(keyContext.getCurrentKey(), value, transformation);
	}

	@Override
	public void clear() {
		keyedState.remove(keyContext.getCurrentKey());
	}

	@Override
	public void mergeNamespaces(VoidNamespace target, Collection<VoidNamespace> sources) throws Exception {
		throw new UnsupportedOperationException("mergeNamespaces should not be called within keyed state.");
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

	private class AggregateTransformation implements StateTransformationFunction<ACC, IN> {

		private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

		public AggregateTransformation(AggregateFunction<IN, ACC, OUT> aggregateFunction) {
			this.aggregateFunction = Preconditions.checkNotNull(aggregateFunction);
		}

		@Override
		public ACC apply(ACC accumulator, IN value) {
			if (accumulator == null) {
				accumulator = aggregateFunction.createAccumulator();
			}
			return aggregateFunction.add(value, accumulator);
		}
	}
}
