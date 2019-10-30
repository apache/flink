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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformat.Timestamp;

import java.io.IOException;

/**
 * Serializer of {@link Timestamp}.
 */
public class TimestampSerializer extends TypeSerializer<Timestamp> {

	private static final long serialVersionUID = 1L;

	private final int precision;

	public TimestampSerializer(int precision) {
		this.precision = precision;
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public TypeSerializer<Timestamp> duplicate() {
		return new TimestampSerializer(precision);
	}

	@Override
	public Timestamp createInstance() {
		return Timestamp.fromLong(0, precision);
	}

	@Override
	public Timestamp copy(Timestamp from) {
		return from;
	}

	@Override
	public Timestamp copy(Timestamp from, Timestamp reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return ((precision <= 3) ? 8 : 12);
	}

	@Override
	public void serialize(Timestamp record, DataOutputView target) throws IOException {
		if (Timestamp.isCompact(precision)) {
			assert record.getNanoOfMillisecond() == 0;
			target.writeLong(record.getMillisecond());
		} else {
			target.writeLong(record.getMillisecond());
			target.writeInt(record.getNanoOfMillisecond());
		}
	}

	@Override
	public Timestamp deserialize(DataInputView source) throws IOException {
		if (Timestamp.isCompact(precision)) {
			long val = source.readLong();
			return new Timestamp(val, 0);
		} else {
			long longVal = source.readLong();
			int intVal = source.readInt();
			return new Timestamp(longVal, intVal);
		}
	}

	@Override
	public Timestamp deserialize(Timestamp reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		if (Timestamp.isCompact(precision)) {
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

		TimestampSerializer that = (TimestampSerializer) obj;
		return precision == that.precision;
	}

	@Override
	public int hashCode() {
		return precision;
	}

	@Override
	public TypeSerializerSnapshot<Timestamp> snapshotConfiguration() {
		return new PreciseTimestampSerializerSnapshot(precision);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link TimestampSerializer}.
	 */
	public static final class PreciseTimestampSerializerSnapshot implements TypeSerializerSnapshot<Timestamp> {

		private static final int CURRENT_VERSION = 1;

		private int previousPrecision;

		public PreciseTimestampSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		PreciseTimestampSerializerSnapshot(int precision) {
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
		public TypeSerializer<Timestamp> restoreSerializer() {
			return new TimestampSerializer(previousPrecision);
		}

		@Override
		public TypeSerializerSchemaCompatibility<Timestamp> resolveSchemaCompatibility(TypeSerializer<Timestamp> newSerializer) {
			if (!(newSerializer instanceof TimestampSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			TimestampSerializer precisionTimestampSerializer = (TimestampSerializer) newSerializer;
			if (previousPrecision != precisionTimestampSerializer.precision) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
