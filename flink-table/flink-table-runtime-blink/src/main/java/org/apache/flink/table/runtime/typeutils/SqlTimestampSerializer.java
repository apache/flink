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
import org.apache.flink.table.dataformat.SqlTimestamp;

import java.io.IOException;

/**
 * Serializer of {@link SqlTimestamp}.
 *
 * <p>A {@link SqlTimestamp} instance can be compactly serialized as a long value(= millisecond) when
 * the Timestamp type is compact. Otherwise it's serialized as a long value and a int value.
 */
public class SqlTimestampSerializer extends TypeSerializer<SqlTimestamp> {

	private static final long serialVersionUID = 1L;

	private final int precision;

	public SqlTimestampSerializer(int precision) {
		this.precision = precision;
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public TypeSerializer<SqlTimestamp> duplicate() {
		return new SqlTimestampSerializer(precision);
	}

	@Override
	public SqlTimestamp createInstance() {
		return SqlTimestamp.fromEpochMillis(0);
	}

	@Override
	public SqlTimestamp copy(SqlTimestamp from) {
		return from;
	}

	@Override
	public SqlTimestamp copy(SqlTimestamp from, SqlTimestamp reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return ((SqlTimestamp.isCompact(precision)) ? 8 : 12);
	}

	@Override
	public void serialize(SqlTimestamp record, DataOutputView target) throws IOException {
		if (SqlTimestamp.isCompact(precision)) {
			assert record.getNanoOfMillisecond() == 0;
			target.writeLong(record.getMillisecond());
		} else {
			target.writeLong(record.getMillisecond());
			target.writeInt(record.getNanoOfMillisecond());
		}
	}

	@Override
	public SqlTimestamp deserialize(DataInputView source) throws IOException {
		if (SqlTimestamp.isCompact(precision)) {
			long val = source.readLong();
			return SqlTimestamp.fromEpochMillis(val);
		} else {
			long longVal = source.readLong();
			int intVal = source.readInt();
			return SqlTimestamp.fromEpochMillis(longVal, intVal);
		}
	}

	@Override
	public SqlTimestamp deserialize(SqlTimestamp reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		if (SqlTimestamp.isCompact(precision)) {
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

		SqlTimestampSerializer that = (SqlTimestampSerializer) obj;
		return precision == that.precision;
	}

	@Override
	public int hashCode() {
		return precision;
	}

	@Override
	public TypeSerializerSnapshot<SqlTimestamp> snapshotConfiguration() {
		return new SqlTimestampSerializerSnapshot(precision);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link SqlTimestampSerializer}.
	 */
	public static final class SqlTimestampSerializerSnapshot implements TypeSerializerSnapshot<SqlTimestamp> {

		private static final int CURRENT_VERSION = 1;

		private int previousPrecision;

		public SqlTimestampSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		SqlTimestampSerializerSnapshot(int precision) {
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
		public TypeSerializer<SqlTimestamp> restoreSerializer() {
			return new SqlTimestampSerializer(previousPrecision);
		}

		@Override
		public TypeSerializerSchemaCompatibility<SqlTimestamp> resolveSchemaCompatibility(TypeSerializer<SqlTimestamp> newSerializer) {
			if (!(newSerializer instanceof SqlTimestampSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			SqlTimestampSerializer sqlTimestampSerializer = (SqlTimestampSerializer) newSerializer;
			if (previousPrecision != sqlTimestampSerializer.precision) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
