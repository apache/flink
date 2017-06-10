/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.migration.streaming.runtime.streamrecord;

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
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * Legacy multiplexing {@link TypeSerializer} for stream records, watermarks and other stream
 * elements.
 */
public class MultiplexingStreamRecordSerializer<T> extends TypeSerializer<StreamElement> {


	private static final long serialVersionUID = 1L;

	private static final int TAG_REC_WITH_TIMESTAMP = 0;
	private static final int TAG_REC_WITHOUT_TIMESTAMP = 1;
	private static final int TAG_WATERMARK = 2;


	private final TypeSerializer<T> typeSerializer;

	public MultiplexingStreamRecordSerializer(TypeSerializer<T> serializer) {
		if (serializer instanceof MultiplexingStreamRecordSerializer || serializer instanceof StreamRecordSerializer) {
			throw new RuntimeException("StreamRecordSerializer given to StreamRecordSerializer as value TypeSerializer: " + serializer);
		}
		this.typeSerializer = requireNonNull(serializer);
	}

	public TypeSerializer<T> getContainedTypeSerializer() {
		return this.typeSerializer;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public MultiplexingStreamRecordSerializer<T> duplicate() {
		TypeSerializer<T> copy = typeSerializer.duplicate();
		return (copy == typeSerializer) ? this : new MultiplexingStreamRecordSerializer<T>(copy);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public StreamRecord<T> createInstance() {
		return new StreamRecord<T>(typeSerializer.createInstance());
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public StreamElement copy(StreamElement from) {
		// we can reuse the timestamp since Instant is immutable
		if (from.isRecord()) {
			StreamRecord<T> fromRecord = from.asRecord();
			return fromRecord.copy(typeSerializer.copy(fromRecord.getValue()));
		}
		else if (from.isWatermark()) {
			// is immutable
			return from;
		}
		else {
			throw new RuntimeException();
		}
	}

	@Override
	public StreamElement copy(StreamElement from, StreamElement reuse) {
		if (from.isRecord() && reuse.isRecord()) {
			StreamRecord<T> fromRecord = from.asRecord();
			StreamRecord<T> reuseRecord = reuse.asRecord();

			T valueCopy = typeSerializer.copy(fromRecord.getValue(), reuseRecord.getValue());
			fromRecord.copyTo(valueCopy, reuseRecord);
			return reuse;
		}
		else if (from.isWatermark()) {
			// is immutable
			return from;
		}
		else {
			throw new RuntimeException("Cannot copy " + from + " -> " + reuse);
		}
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int tag = source.readByte();
		target.write(tag);

		if (tag == TAG_REC_WITH_TIMESTAMP) {
			// move timestamp
			target.writeLong(source.readLong());
			typeSerializer.copy(source, target);
		}
		else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
			typeSerializer.copy(source, target);
		}
		else if (tag == TAG_WATERMARK) {
			target.writeLong(source.readLong());
		}
		else {
			throw new IOException("Corrupt stream, found tag: " + tag);
		}
	}

	@Override
	public void serialize(StreamElement value, DataOutputView target) throws IOException {
		if (value.isRecord()) {
			StreamRecord<T> record = value.asRecord();

			if (record.hasTimestamp()) {
				target.write(TAG_REC_WITH_TIMESTAMP);
				target.writeLong(record.getTimestamp());
			} else {
				target.write(TAG_REC_WITHOUT_TIMESTAMP);
			}
			typeSerializer.serialize(record.getValue(), target);
		}
		else if (value.isWatermark()) {
			target.write(TAG_WATERMARK);
			target.writeLong(value.asWatermark().getTimestamp());
		}
		else {
			throw new RuntimeException();
		}
	}

	@Override
	public StreamElement deserialize(DataInputView source) throws IOException {
		int tag = source.readByte();
		if (tag == TAG_REC_WITH_TIMESTAMP) {
			long timestamp = source.readLong();
			return new StreamRecord<T>(typeSerializer.deserialize(source), timestamp);
		}
		else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
			return new StreamRecord<T>(typeSerializer.deserialize(source));
		}
		else if (tag == TAG_WATERMARK) {
			return new Watermark(source.readLong());
		}
		else {
			throw new IOException("Corrupt stream, found tag: " + tag);
		}
	}

	@Override
	public StreamElement deserialize(StreamElement reuse, DataInputView source) throws IOException {
		int tag = source.readByte();
		if (tag == TAG_REC_WITH_TIMESTAMP) {
			long timestamp = source.readLong();
			T value = typeSerializer.deserialize(source);
			StreamRecord<T> reuseRecord = reuse.asRecord();
			reuseRecord.replace(value, timestamp);
			return reuseRecord;
		}
		else if (tag == TAG_REC_WITHOUT_TIMESTAMP) {
			T value = typeSerializer.deserialize(source);
			StreamRecord<T> reuseRecord = reuse.asRecord();
			reuseRecord.replace(value);
			return reuseRecord;
		}
		else if (tag == TAG_WATERMARK) {
			return new Watermark(source.readLong());
		}
		else {
			throw new IOException("Corrupt stream, found tag: " + tag);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & compatibility
	// --------------------------------------------------------------------------------------------

	@Override
	public MultiplexingStreamRecordSerializerConfigSnapshot snapshotConfiguration() {
		return new MultiplexingStreamRecordSerializerConfigSnapshot<>(typeSerializer);
	}

	@Override
	public CompatibilityResult<StreamElement> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof MultiplexingStreamRecordSerializerConfigSnapshot) {
			Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> previousTypeSerializerAndConfig =
				((MultiplexingStreamRecordSerializerConfigSnapshot) configSnapshot).getSingleNestedSerializerAndConfig();

			CompatibilityResult<T> compatResult = CompatibilityUtil.resolveCompatibilityResult(
					previousTypeSerializerAndConfig.f0,
					UnloadableDummyTypeSerializer.class,
					previousTypeSerializerAndConfig.f1,
					typeSerializer);

			if (!compatResult.isRequiresMigration()) {
				return CompatibilityResult.compatible();
			} else if (compatResult.getConvertDeserializer() != null) {
				return CompatibilityResult.requiresMigration(
					new MultiplexingStreamRecordSerializer<>(
						new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer())));
			}
		}

		return CompatibilityResult.requiresMigration();
	}

	/**
	 * Configuration snapshot specific to the {@link MultiplexingStreamRecordSerializer}.
	 */
	public static final class MultiplexingStreamRecordSerializerConfigSnapshot<T>
			extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		/** This empty nullary constructor is required for deserializing the configuration. */
		public MultiplexingStreamRecordSerializerConfigSnapshot() {}

		public MultiplexingStreamRecordSerializerConfigSnapshot(TypeSerializer<T> typeSerializer) {
			super(typeSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof MultiplexingStreamRecordSerializer) {
			MultiplexingStreamRecordSerializer<?> other = (MultiplexingStreamRecordSerializer<?>) obj;

			return other.canEqual(this) && typeSerializer.equals(other.typeSerializer);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof MultiplexingStreamRecordSerializer;
	}

	@Override
	public int hashCode() {
		return typeSerializer.hashCode();
	}
}
