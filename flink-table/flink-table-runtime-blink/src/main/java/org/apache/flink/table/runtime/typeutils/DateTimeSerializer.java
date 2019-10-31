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
import org.apache.flink.table.dataformat.DateTime;

import java.io.IOException;

/**
 * Serializer of {@link DateTime}.
 */
public class DateTimeSerializer extends TypeSerializer<DateTime> {

	private static final long serialVersionUID = 1L;

	private final int precision;

	public DateTimeSerializer(int precision) {
		this.precision = precision;
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public TypeSerializer<DateTime> duplicate() {
		return new DateTimeSerializer(precision);
	}

	@Override
	public DateTime createInstance() {
		return DateTime.fromEpochMillis(0);
	}

	@Override
	public DateTime copy(DateTime from) {
		return from;
	}

	@Override
	public DateTime copy(DateTime from, DateTime reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return ((precision <= 3) ? 8 : 12);
	}

	@Override
	public void serialize(DateTime record, DataOutputView target) throws IOException {
		if (DateTime.isCompact(precision)) {
			assert record.getNanoOfMillisecond() == 0;
			target.writeLong(record.getMillisecond());
		} else {
			target.writeLong(record.getMillisecond());
			target.writeInt(record.getNanoOfMillisecond());
		}
	}

	@Override
	public DateTime deserialize(DataInputView source) throws IOException {
		if (DateTime.isCompact(precision)) {
			long val = source.readLong();
			return DateTime.fromEpochMillis(val);
		} else {
			long longVal = source.readLong();
			int intVal = source.readInt();
			return DateTime.fromEpochMillis(longVal, intVal);
		}
	}

	@Override
	public DateTime deserialize(DateTime reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		if (DateTime.isCompact(precision)) {
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

		DateTimeSerializer that = (DateTimeSerializer) obj;
		return precision == that.precision;
	}

	@Override
	public int hashCode() {
		return precision;
	}

	@Override
	public TypeSerializerSnapshot<DateTime> snapshotConfiguration() {
		return new DateTimeSerializerSnapshot(precision);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link DateTimeSerializer}.
	 */
	public static final class DateTimeSerializerSnapshot implements TypeSerializerSnapshot<DateTime> {

		private static final int CURRENT_VERSION = 1;

		private int previousPrecision;

		public DateTimeSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		DateTimeSerializerSnapshot(int precision) {
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
		public TypeSerializer<DateTime> restoreSerializer() {
			return new DateTimeSerializer(previousPrecision);
		}

		@Override
		public TypeSerializerSchemaCompatibility<DateTime> resolveSchemaCompatibility(TypeSerializer<DateTime> newSerializer) {
			if (!(newSerializer instanceof DateTimeSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			DateTimeSerializer dateTimeSerializer = (DateTimeSerializer) newSerializer;
			if (previousPrecision != dateTimeSerializer.precision) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
