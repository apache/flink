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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.heap.KeyContextImpl;
import org.apache.flink.runtime.state.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedValueState;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * used for aggregating state.
 *
 * @param <K> The type of key the state is associated to
 * @param <N> The type of the namespace
 * @param <IN> Type of the value added to the state
 * @param <ACC> The type of elements in the state
 * @param <OUT> Type of the value extracted from the state
 */
public class ContextSubKeyedAggregatingState<K, N, IN, ACC, OUT>
	implements ContextSubKeyedAppendingState<K, N, IN, ACC, OUT>, InternalAggregatingState<K, N, IN, ACC, OUT> {

	private N namespace;

	private final KeyContextImpl<K> operator;

	private final SubKeyedValueState<Object, N, ACC> subKeyedValueState;

	private final AggregateTransformation aggregateTransformation;

	private final MergeTransformation mergeTransformation;

	public ContextSubKeyedAggregatingState(
		KeyContextImpl<K> operator,
		SubKeyedValueState<Object, N, ACC> subKeyedValueState,
		AggregateFunction<IN, ACC, OUT> aggregateFunction) {
		Preconditions.checkNotNull(operator);
		Preconditions.checkNotNull(subKeyedValueState);
		Preconditions.checkNotNull(aggregateFunction);
		this.operator = operator;
		this.subKeyedValueState = subKeyedValueState;
		this.aggregateTransformation = new AggregateTransformation(aggregateFunction);
		this.mergeTransformation = new MergeTransformation(aggregateFunction);
	}

	@Override
	public OUT get() {
		ACC accumulator = subKeyedValueState.get(operator.getCurrentKey(), namespace);
		return accumulator == null ? null : aggregateTransformation.aggregateFunction.getResult(accumulator);
	}

	@Override
	public void add(IN value) {
		subKeyedValueState.transform(operator.getCurrentKey(), namespace, value, aggregateTransformation);
	}

	@Override
	public void clear() {
		subKeyedValueState.remove(operator.getCurrentKey(), namespace);
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) {
		if (sources == null || sources.isEmpty()) {
			return; // nothing to do
		}

		Object currentKey = operator.getCurrentKey();
		ACC merged = null;

		// merge the sources
		for (N source : sources) {

			// get and remove the next source per namespace/key
			ACC sourceState = subKeyedValueState.getAndRemove(currentKey, source);

			if (merged != null && sourceState != null) {
				merged = mergeTransformation.aggregateFunction.merge(merged, sourceState);
			} else if (merged == null) {
				merged = sourceState;
			}
		}

		// merge into the target, if needed
		if (merged != null) {
			subKeyedValueState.transform(currentKey, target, merged, mergeTransformation);
		}
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
	public byte[] getSerializedValue(
		byte[] serializedKeyAndNamespace,
		TypeSerializer safeKeySerializer,
		TypeSerializer safeNamespaceSerializer,
		TypeSerializer safeValueSerializer) throws Exception {

		return subKeyedValueState.getSerializedValue(serializedKeyAndNamespace, safeKeySerializer, safeNamespaceSerializer, safeValueSerializer);
	}

	@Override
	public SubKeyedState<K, N, ACC> getSubKeyedState() {
		return (SubKeyedState<K, N, ACC>) subKeyedValueState;
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

	private class MergeTransformation implements StateTransformationFunction<ACC, ACC> {

		private final AggregateFunction<IN, ACC, OUT> aggregateFunction;

		public MergeTransformation(AggregateFunction<IN, ACC, OUT> aggregateFunction) {
			this.aggregateFunction = Preconditions.checkNotNull(aggregateFunction);
		}

		@Override
		public ACC apply(ACC v1, ACC v2) {
			return v1 == null ? v2 : aggregateFunction.merge(v1, v2);
		}
	}
}
