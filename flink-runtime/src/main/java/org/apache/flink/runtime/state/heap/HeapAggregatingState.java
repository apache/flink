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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Heap-backed partitioned {@link AggregatingState} that is
 * snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <IN> The type of the value added to the state.
 * @param <ACC> The type of the value stored in the state (the accumulator type).
 * @param <OUT> The type of the value returned from the state.
 */
class HeapAggregatingState<K, N, IN, ACC, OUT>
	extends AbstractHeapMergingState<K, N, IN, ACC, OUT>
	implements InternalAggregatingState<K, N, IN, ACC, OUT> {
	private final AggregateTransformation<IN, ACC, OUT> aggregateTransformation;

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateTable The state table for which this state is associated to.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param defaultValue The default value for the state.
	 * @param aggregateFunction The aggregating function used for aggregating state.
	 */
	private HeapAggregatingState(
		StateTable<K, N, ACC> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<ACC> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		ACC defaultValue,
		AggregateFunction<IN, ACC, OUT> aggregateFunction) {

		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
		this.aggregateTransformation = new AggregateTransformation<>(aggregateFunction);
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<ACC> getValueSerializer() {
		return valueSerializer;
	}

	// ------------------------------------------------------------------------
	//  state access
	// ------------------------------------------------------------------------

	@Override
	public OUT get() {
		ACC accumulator = getInternal();
		return accumulator != null ? aggregateTransformation.aggFunction.getResult(accumulator) : null;
	}

	@Override
	public void add(IN value) throws IOException {
		final N namespace = currentNamespace;

		if (value == null) {
			clear();
			return;
		}

		try {
			stateTable.transform(namespace, value, aggregateTransformation);
		} catch (Exception e) {
			throw new IOException("Exception while applying AggregateFunction in aggregating state", e);
		}
	}

	// ------------------------------------------------------------------------
	//  state merging
	// ------------------------------------------------------------------------

	@Override
	protected ACC mergeState(ACC a, ACC b) {
		return aggregateTransformation.aggFunction.merge(a, b);
	}

	static final class AggregateTransformation<IN, ACC, OUT> implements StateTransformationFunction<ACC, IN> {

		private final AggregateFunction<IN, ACC, OUT> aggFunction;

		AggregateTransformation(AggregateFunction<IN, ACC, OUT> aggFunction) {
			this.aggFunction = Preconditions.checkNotNull(aggFunction);
		}

		@Override
		public ACC apply(ACC accumulator, IN value) {
			if (accumulator == null) {
				accumulator = aggFunction.createAccumulator();
			}
			return aggFunction.add(value, accumulator);
		}
	}

	@SuppressWarnings("unchecked")
	static <T, K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		StateTable<K, N, SV> stateTable,
		TypeSerializer<K> keySerializer) {
		return (IS) new HeapAggregatingState<>(
			stateTable,
			keySerializer,
			stateTable.getStateSerializer(),
			stateTable.getNamespaceSerializer(),
			stateDesc.getDefaultValue(),
			((AggregatingStateDescriptor<T, SV, ?>) stateDesc).getAggregateFunction());
	}
}
