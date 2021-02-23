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

import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalMergingState;

import java.util.Collection;

/**
 * Base class for {@link MergingState} ({@link InternalMergingState}) that is stored on the heap.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <IN> The type of the input elements.
 * @param <SV> The type of the values in the state.
 * @param <OUT> The type of the output elements.
 */
public abstract class AbstractRemoteHeapMergingState<K, N, IN, SV, OUT>
	extends AbstractRemoteHeapAppendingState<K, N, IN, SV, OUT>
	implements InternalMergingState<K, N, IN, SV, OUT> {

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
	protected AbstractRemoteHeapMergingState(
		TypeSerializer<K> keySerializer,
		TypeSerializer<SV> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvStateInfo,
		SV defaultValue,
		RemoteHeapKeyedStateBackend backend) {

		super(
			keySerializer,
			valueSerializer,
			namespaceSerializer,
			kvStateInfo,
			defaultValue,
			backend);
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources == null || sources.isEmpty()) {
			return; // nothing to do
		}

		SV current = null;
		for (N source : sources) {

			if (source != null) {
				setCurrentNamespace(source);
				final byte[] sourceKey = serializeCurrentKeyWithGroupAndNamespace();
				final byte[] valueBytes = backend.syncRemClient.hget(
					kvStateInfo.nameBytes,
					sourceKey);

				if (valueBytes != null) {
					backend.syncRemClient.del(sourceKey);
					dataInputView.setBuffer(valueBytes);
					SV value = valueSerializer.deserialize(dataInputView);

					if (current != null) {
						current = mergeState(current, value);
					} else {
						current = value;
					}
				}
			}
		}

		if (current != null) {
			setCurrentNamespace(target);
			// create the target full-binary-key
			final byte[] targetKey = serializeCurrentKeyWithGroupAndNamespace();
			final byte[] targetValueBytes = backend.syncRemClient.hget(
				kvStateInfo.nameBytes,
				targetKey);

			if (targetValueBytes != null) {
				// target also had a value, merge
				dataInputView.setBuffer(targetValueBytes);
				SV value = valueSerializer.deserialize(dataInputView);

				current = mergeState(current, value);
			}

			// serialize the resulting value
			dataOutputView.clear();
			valueSerializer.serialize(current, dataOutputView);

			// write the resulting value
			backend.syncRemClient.hset(
				kvStateInfo.nameBytes,
				targetKey,
				dataOutputView.getCopyOfBuffer());
		}
	}

	protected abstract SV mergeState(SV a, SV b) throws Exception;

	final class MergeTransformation implements StateTransformationFunction<SV, SV> {

		@Override
		public SV apply(SV targetState, SV merged) throws Exception {
			if (targetState != null) {
				return mergeState(targetState, merged);
			} else {
				return merged;
			}
		}
	}
}
