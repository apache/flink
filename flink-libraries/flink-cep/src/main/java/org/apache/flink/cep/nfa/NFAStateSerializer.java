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

package org.apache.flink.cep.nfa;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Queue;

/**
 * A {@link TypeSerializer} for {@link NFAState} that uses Java Serialization.
 */
public class NFAStateSerializer<T> extends TypeSerializer<NFAState<T>> {

	private static final long serialVersionUID = 2098282423980597010L;

	private final TypeSerializer<SharedBuffer<String, T>> sharedBufferSerializer;

	private final TypeSerializer<T> eventSerializer;

	public NFAStateSerializer(TypeSerializer<T> typeSerializer) {
		this(typeSerializer, new SharedBuffer.SharedBufferSerializer<>(StringSerializer.INSTANCE, typeSerializer));
	}

	public NFAStateSerializer(
			TypeSerializer<T> typeSerializer,
			TypeSerializer<SharedBuffer<String, T>> sharedBufferSerializer) {
		this.eventSerializer = typeSerializer;
		this.sharedBufferSerializer = sharedBufferSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public NFAStateSerializer<T> duplicate() {
		return new NFAStateSerializer<>(eventSerializer.duplicate());
	}

	@Override
	public NFAState<T> createInstance() {
		return null;
	}

	@Override
	public NFAState<T> copy(NFAState<T> from) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			serialize(from, new DataOutputViewStreamWrapper(baos));
			baos.close();

			byte[] data = baos.toByteArray();

			ByteArrayInputStream bais = new ByteArrayInputStream(data);
			NFAState<T> copy = deserialize(new DataInputViewStreamWrapper(bais));
			bais.close();
			return copy;
		} catch (IOException e) {
			throw new RuntimeException("Could not copy NFA.", e);
		}
	}

	@Override
	public NFAState<T> copy(NFAState<T> from, NFAState<T> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(NFAState<T> record, DataOutputView target) throws IOException {
		sharedBufferSerializer.serialize(record.getEventSharedBuffer(), target);

		target.writeInt(record.getComputationStates().size());

		StringSerializer stateNameSerializer = StringSerializer.INSTANCE;
		LongSerializer timestampSerializer = LongSerializer.INSTANCE;
		DeweyNumber.DeweyNumberSerializer versionSerializer = new DeweyNumber.DeweyNumberSerializer();

		for (ComputationState<T> computationState: record.getComputationStates()) {
			stateNameSerializer.serialize(computationState.getState(), target);
			stateNameSerializer.serialize(computationState.getPreviousState(), target);

			timestampSerializer.serialize(computationState.getTimestamp(), target);
			versionSerializer.serialize(computationState.getVersion(), target);
			timestampSerializer.serialize(computationState.getStartTimestamp(), target);
			target.writeInt(computationState.getCounter());

			if (computationState.getEvent() == null) {
				target.writeBoolean(false);
			} else {
				target.writeBoolean(true);
				eventSerializer.serialize(computationState.getEvent(), target);
			}
		}
	}

	@Override
	public NFAState<T> deserialize(DataInputView source) throws IOException {
		SharedBuffer<String, T> sharedBuffer = sharedBufferSerializer.deserialize(source);
		Queue<ComputationState<T>> computationStates = NFASerializationUtils.deserializeComputationStates(
			eventSerializer, source);
		return new NFAState<>(computationStates, sharedBuffer, false);
	}

	@Override
	public NFAState<T> deserialize(NFAState<T> reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		SharedBuffer<String, T> sharedBuffer = sharedBufferSerializer.deserialize(source);
		sharedBufferSerializer.serialize(sharedBuffer, target);

		StringSerializer stateNameSerializer = StringSerializer.INSTANCE;
		LongSerializer timestampSerializer = LongSerializer.INSTANCE;
		DeweyNumber.DeweyNumberSerializer versionSerializer = new DeweyNumber.DeweyNumberSerializer();

		int computationStateNo = source.readInt();
		target.writeInt(computationStateNo);

		for (int i = 0; i < computationStateNo; i++) {
			String stateName = stateNameSerializer.deserialize(source);
			stateNameSerializer.serialize(stateName, target);

			String prevStateName = stateNameSerializer.deserialize(source);
			stateNameSerializer.serialize(prevStateName, target);

			long timestamp = timestampSerializer.deserialize(source);
			timestampSerializer.serialize(timestamp, target);

			DeweyNumber version = versionSerializer.deserialize(source);
			versionSerializer.serialize(version, target);

			long startTimestamp = timestampSerializer.deserialize(source);
			timestampSerializer.serialize(startTimestamp, target);

			int counter = source.readInt();
			target.writeInt(counter);

			boolean hasEvent = source.readBoolean();
			target.writeBoolean(hasEvent);
			if (hasEvent) {
				T event = eventSerializer.deserialize(source);
				eventSerializer.serialize(event, target);
			}
		}
	}

	@Override
	public boolean equals(Object obj) {
		return obj == this ||
				(obj != null && obj.getClass().equals(getClass()) &&
						sharedBufferSerializer.equals(((NFAStateSerializer) obj).sharedBufferSerializer) &&
						eventSerializer.equals(((NFAStateSerializer) obj).eventSerializer));
	}

	@Override
	public boolean canEqual(Object obj) {
		return true;
	}

	@Override
	public int hashCode() {
		return 37 * sharedBufferSerializer.hashCode() + eventSerializer.hashCode();
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new NFAStateSerializerConfigSnapshot<>(eventSerializer, sharedBufferSerializer);
	}

	@Override
	public CompatibilityResult<NFAState<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof NFAStateSerializerConfigSnapshot) {
			List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializersAndConfigs =
				((NFAStateSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

			CompatibilityResult<T> eventCompatResult = CompatibilityUtil.resolveCompatibilityResult(
				serializersAndConfigs.get(0).f0,
				UnloadableDummyTypeSerializer.class,
				serializersAndConfigs.get(0).f1,
				eventSerializer);

			CompatibilityResult<SharedBuffer<String, T>> sharedBufCompatResult =
				CompatibilityUtil.resolveCompatibilityResult(
					serializersAndConfigs.get(1).f0,
					UnloadableDummyTypeSerializer.class,
					serializersAndConfigs.get(1).f1,
					sharedBufferSerializer);

			if (!sharedBufCompatResult.isRequiresMigration() && !eventCompatResult.isRequiresMigration()) {
				return CompatibilityResult.compatible();
			} else {
				if (eventCompatResult.getConvertDeserializer() != null &&
					sharedBufCompatResult.getConvertDeserializer() != null) {
					return CompatibilityResult.requiresMigration(
						new NFAStateSerializer<>(
							new TypeDeserializerAdapter<>(eventCompatResult.getConvertDeserializer()),
							new TypeDeserializerAdapter<>(sharedBufCompatResult.getConvertDeserializer())));
				}
			}
		}

		return CompatibilityResult.requiresMigration();
	}
}
