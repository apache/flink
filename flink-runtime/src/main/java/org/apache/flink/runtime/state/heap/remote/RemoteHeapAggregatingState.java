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

package org.apache.flink.runtime.state.heap.remote;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
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
class RemoteHeapAggregatingState<K, N, IN, ACC, OUT>
	extends AbstractRemoteHeapMergingState<K, N, IN, ACC, OUT>
	implements InternalAggregatingState<K, N, IN, ACC, OUT> {
	private final AggregateTransformation<IN, ACC, OUT> aggregateTransformation;

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param kvStateInfo StateInfo containing descriptors
	 * @param defaultValue The default value for the state.
	 * @param backend KeyBackend
	 */
	private RemoteHeapAggregatingState(
		TypeSerializer<K> keySerializer,
		TypeSerializer<ACC> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvStateInfo,
		ACC defaultValue,
		AggregateFunction<IN, ACC, OUT> aggregateFunction,
		RemoteHeapKeyedStateBackend<K> backend) {
		super(
			keySerializer,
			valueSerializer,
			namespaceSerializer,
			kvStateInfo,
			defaultValue,
			backend);

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
		return
			accumulator != null ? aggregateTransformation.aggFunction.getResult(accumulator) : null;
	}

	@Override
	public void add(IN value) throws IOException {
		byte[] key = getKeyBytes();
		ACC accumulator = getInternal(key);
		accumulator = accumulator
			== null ? aggregateTransformation.aggFunction.createAccumulator() : accumulator;
		updateInternal(key, aggregateTransformation.aggFunction.add(value, accumulator));
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
		RegisteredKeyValueStateBackendMetaInfo<N, SV> metaInfo,
		TypeSerializer<K> keySerializer,
		RemoteHeapKeyedStateBackend backend) {
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvState = backend.getRemoteHeapKvStateInfo(
			stateDesc.getName());
		return (IS) new RemoteHeapAggregatingState<>(
			keySerializer,
			metaInfo.getStateSerializer(),
			metaInfo.getNamespaceSerializer(),
			kvState,
			stateDesc.getDefaultValue(),
			((AggregatingStateDescriptor<T, SV, ?>) stateDesc).getAggregateFunction(),
			backend);
	}
}
