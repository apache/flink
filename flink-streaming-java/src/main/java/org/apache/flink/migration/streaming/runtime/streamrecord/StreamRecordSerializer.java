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
 * WITHOUStreamRecord<?>WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.migration.streaming.runtime.streamrecord;

import java.io.IOException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

/**
 * Serializer for {@link StreamRecord}. This version ignores timestamps and only deals with
 * the element.
 *
 * <p>{@link MultiplexingStreamRecordSerializer} is a version that deals with timestamps and also
 * multiplexes {@link org.apache.flink.streaming.api.watermark.Watermark Watermarks} in the same
 * stream with {@link StreamRecord StreamRecords}.
 *
 * @see MultiplexingStreamRecordSerializer
 *
 * @param <T> The type of value in the {@link StreamRecord}
 */
@Internal
public final class StreamRecordSerializer<T> extends TypeSerializer<StreamRecord<T>> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> typeSerializer;

	public StreamRecordSerializer(TypeSerializer<T> serializer) {
		if (serializer instanceof StreamRecordSerializer) {
			throw new RuntimeException("StreamRecordSerializer given to StreamRecordSerializer as value TypeSerializer: " + serializer);
		}
		this.typeSerializer = Preconditions.checkNotNull(serializer);
	}

	public TypeSerializer<T> getContainedTypeSerializer() {
		return this.typeSerializer;
	}

	// ------------------------------------------------------------------------
	//  General serializer and type utils
	// ------------------------------------------------------------------------

	@Override
	public StreamRecordSerializer<T> duplicate() {
		TypeSerializer<T> serializerCopy = typeSerializer.duplicate();
		return serializerCopy == typeSerializer ? this : new StreamRecordSerializer<T>(serializerCopy);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public int getLength() {
		return typeSerializer.getLength();
	}

	// ------------------------------------------------------------------------
	//  Type serialization, copying, instantiation
	// ------------------------------------------------------------------------

	@Override
	public StreamRecord<T> createInstance() {
		try {
			return new StreamRecord<T>(typeSerializer.createInstance());
		} catch (Exception e) {
			throw new RuntimeException("Cannot instantiate StreamRecord.", e);
		}
	}

	@Override
	public StreamRecord<T> copy(StreamRecord<T> from) {
		return from.copy(typeSerializer.copy(from.getValue()));
	}

	@Override
	public StreamRecord<T> copy(StreamRecord<T> from, StreamRecord<T> reuse) {
		from.copyTo(typeSerializer.copy(from.getValue(), reuse.getValue()), reuse);
		return reuse;
	}

	@Override
	public void serialize(StreamRecord<T> value, DataOutputView target) throws IOException {
		typeSerializer.serialize(value.getValue(), target);
	}

	@Override
	public StreamRecord<T> deserialize(DataInputView source) throws IOException {
		return new StreamRecord<T>(typeSerializer.deserialize(source));
	}

	@Override
	public StreamRecord<T> deserialize(StreamRecord<T> reuse, DataInputView source) throws IOException {
		T element = typeSerializer.deserialize(reuse.getValue(), source);
		reuse.replace(element);
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		typeSerializer.copy(source, target);
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof StreamRecordSerializer) {
			StreamRecordSerializer<?> other = (StreamRecordSerializer<?>) obj;

			return other.canEqual(this) && typeSerializer.equals(other.typeSerializer);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof StreamRecordSerializer;
	}

	@Override
	public int hashCode() {
		return typeSerializer.hashCode();
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public StreamRecordSerializerConfigSnapshot snapshotConfiguration() {
		return new StreamRecordSerializerConfigSnapshot<>(typeSerializer);
	}

	@Override
	public CompatibilityResult<StreamRecord<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof StreamRecordSerializerConfigSnapshot) {
			Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> previousTypeSerializerAndConfig =
				((StreamRecordSerializerConfigSnapshot) configSnapshot).getSingleNestedSerializerAndConfig();

			CompatibilityResult<T> compatResult = CompatibilityUtil.resolveCompatibilityResult(
					previousTypeSerializerAndConfig.f0,
					UnloadableDummyTypeSerializer.class,
					previousTypeSerializerAndConfig.f1,
					typeSerializer);

			if (!compatResult.isRequiresMigration()) {
				return CompatibilityResult.compatible();
			} else if (compatResult.getConvertDeserializer() != null) {
				return CompatibilityResult.requiresMigration(
					new StreamRecordSerializer<>(
						new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer())));
			}
		}

		return CompatibilityResult.requiresMigration();
	}

	/**
	 * Configuration snapshot specific to the {@link StreamRecordSerializer}.
	 */
	public static final class StreamRecordSerializerConfigSnapshot<T> extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		/** This empty nullary constructor is required for deserializing the configuration. */
		public StreamRecordSerializerConfigSnapshot() {}

		public StreamRecordSerializerConfigSnapshot(TypeSerializer<T> typeSerializer) {
			super(typeSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}
}
