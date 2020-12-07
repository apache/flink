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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.data.TimestampData;

import java.io.IOException;

/**
 * Serializer for {@link TimestampData}.
 *
 * <p>A {@link TimestampData} instance can be compactly serialized as a long value(= millisecond) when
 * the Timestamp type is compact. Otherwise it's serialized as a long value and a int value.
 */
@Internal
public class TimestampDataSerializer extends TypeSerializer<TimestampData> {

	private static final long serialVersionUID = 1L;

	private final int precision;

	public TimestampDataSerializer(int precision) {
		this.precision = precision;
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public TypeSerializer<TimestampData> duplicate() {
		return new TimestampDataSerializer(precision);
	}

	@Override
	public TimestampData createInstance() {
		return TimestampData.fromEpochMillis(0);
	}

	@Override
	public TimestampData copy(TimestampData from) {
		return from;
	}

	@Override
	public TimestampData copy(TimestampData from, TimestampData reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return ((TimestampData.isCompact(precision)) ? 8 : 12);
	}

	@Override
	public void serialize(TimestampData record, DataOutputView target) throws IOException {
		if (TimestampData.isCompact(precision)) {
			assert record.getNanoOfMillisecond() == 0;
			target.writeLong(record.getMillisecond());
		} else {
			target.writeLong(record.getMillisecond());
			target.writeInt(record.getNanoOfMillisecond());
		}
	}

	@Override
	public TimestampData deserialize(DataInputView source) throws IOException {
		if (TimestampData.isCompact(precision)) {
			long val = source.readLong();
			return TimestampData.fromEpochMillis(val);
		} else {
			long longVal = source.readLong();
			int intVal = source.readInt();
			return TimestampData.fromEpochMillis(longVal, intVal);
		}
	}

	@Override
	public TimestampData deserialize(TimestampData reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		if (TimestampData.isCompact(precision)) {
			target.writeLong(source.readLong());
		} else {
			target.writeLong(source.readLong());
			target.writeInt(source.readInt());
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}

		TimestampDataSerializer that = (TimestampDataSerializer) obj;
		return precision == that.precision;
	}

	@Override
	public int hashCode() {
		return precision;
	}

	@Override
	public TypeSerializerSnapshot<TimestampData> snapshotConfiguration() {
		return new TimestampDataSerializerSnapshot(precision);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link TimestampDataSerializer}.
	 */
	public static final class TimestampDataSerializerSnapshot implements TypeSerializerSnapshot<TimestampData> {

		private static final int CURRENT_VERSION = 1;

		private int previousPrecision;

		public TimestampDataSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		TimestampDataSerializerSnapshot(int precision) {
			this.previousPrecision = precision;
		}

		@Override
		public int getCurrentVersion() {
			return CURRENT_VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			out.writeInt(previousPrecision);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
			this.previousPrecision = in.readInt();
		}

		@Override
		public TypeSerializer<TimestampData> restoreSerializer() {
			return new TimestampDataSerializer(previousPrecision);
		}

		@Override
		public TypeSerializerSchemaCompatibility<TimestampData> resolveSchemaCompatibility(TypeSerializer<TimestampData> newSerializer) {
			if (!(newSerializer instanceof TimestampDataSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			TimestampDataSerializer timestampDataSerializer = (TimestampDataSerializer) newSerializer;
			if (previousPrecision != timestampDataSerializer.precision) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
