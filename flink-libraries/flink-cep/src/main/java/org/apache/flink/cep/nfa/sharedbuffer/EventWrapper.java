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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * Thin wrapper around user event that adds a lock.
 *
 * @param <E> user event type
 */
public class EventWrapper<E> {

	private final E event;
	private final Lock lock;

	public EventWrapper(E event, int refCount) {
		this.event = event;
		this.lock = new Lock(refCount);
	}

	public E getEvent() {
		return event;
	}

	public Lock getLock() {
		return lock;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		EventWrapper<?> that = (EventWrapper<?>) o;
		return Objects.equals(event, that.event) &&
			Objects.equals(lock, that.lock);
	}

	@Override
	public int hashCode() {
		return Objects.hash(event, lock);
	}

	@Override
	public String toString() {
		return "EventWrapper{" +
			"event=" + event +
			", lock=" + lock +
			'}';
	}

	/** Serializer for {@link EventWrapper}. */
	public static class EventWrapperTypeSerializer<E> extends TypeSerializer<EventWrapper<E>> {
		private static final long serialVersionUID = 3298801058463337340L;
		private final TypeSerializer<E> inputSerializer;

		EventWrapperTypeSerializer(TypeSerializer<E> elementSerializer) {
			this.inputSerializer = elementSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<EventWrapper<E>> duplicate() {
			return new EventWrapperTypeSerializer<>(inputSerializer);
		}

		@Override
		public EventWrapper<E> createInstance() {
			return null;
		}

		@Override
		public EventWrapper<E> copy(EventWrapper<E> from) {
			return new EventWrapper<>(inputSerializer.copy(from.event), from.lock.getRefCounter());
		}

		@Override
		public EventWrapper<E> copy(
			EventWrapper<E> from, EventWrapper<E> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(EventWrapper<E> record, DataOutputView target) throws IOException {
			IntSerializer.INSTANCE.serialize(record.getLock().getRefCounter(), target);
			inputSerializer.serialize(record.event, target);
		}

		@Override
		public EventWrapper<E> deserialize(DataInputView source) throws IOException {
			Integer refCount = IntSerializer.INSTANCE.deserialize(source);
			E record = inputSerializer.deserialize(source);
			return new EventWrapper<>(record, refCount);
		}

		@Override
		public EventWrapper<E> deserialize(
			EventWrapper<E> reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			IntSerializer.INSTANCE.copy(source, target); // refCounter

			E record = inputSerializer.deserialize(source);
			inputSerializer.serialize(record, target);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			EventWrapperTypeSerializer<?> that = (EventWrapperTypeSerializer<?>) o;
			return Objects.equals(inputSerializer, that.inputSerializer);
		}

		@Override
		public int hashCode() {
			return Objects.hash(inputSerializer);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj.getClass().equals(EventWrapper.class);
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return inputSerializer.snapshotConfiguration();
		}

		@Override
		public CompatibilityResult<EventWrapper<E>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			CompatibilityResult<E> inputComaptibilityResult = inputSerializer.ensureCompatibility(configSnapshot);
			if (inputComaptibilityResult.isRequiresMigration()) {
				return CompatibilityResult.requiresMigration(new EventWrapperTypeSerializer<>(
					new TypeDeserializerAdapter<>(inputComaptibilityResult.getConvertDeserializer()))
				);
			} else {
				return CompatibilityResult.compatible();
			}
		}
	}

}
