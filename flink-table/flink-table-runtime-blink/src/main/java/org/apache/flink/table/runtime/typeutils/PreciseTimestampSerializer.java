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
import org.apache.flink.table.dataformat.PreciseTimestamp;

import java.io.IOException;

/**
 * Serializer of {@link PreciseTimestamp}.
 */
public class PreciseTimestampSerializer extends TypeSerializer<PreciseTimestamp> {

	private static final long serialVersionUID = 1L;

	private final int precision;

	public PreciseTimestampSerializer(int precision) {
		this.precision = precision;
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public TypeSerializer<PreciseTimestamp> duplicate() {
		return new PreciseTimestampSerializer(precision);
	}

	@Override
	public PreciseTimestamp createInstance() {
		return PreciseTimestamp.zero(precision);
	}

	@Override
	public PreciseTimestamp copy(PreciseTimestamp from) {
		return from.copy();
	}

	@Override
	public PreciseTimestamp copy(PreciseTimestamp from, PreciseTimestamp reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(PreciseTimestamp record, DataOutputView target) throws IOException {
		if (PreciseTimestamp.isCompact(precision)) {
			assert record.isCompact();
			target.writeLong(record.toLong());
		} else {
			byte[] bytes = record.toUnscaledBytes();
			target.writeInt(bytes.length);
			target.write(bytes);
		}
	}

	@Override
	public PreciseTimestamp deserialize(DataInputView source) throws IOException {
		if (PreciseTimestamp.isCompact(precision)) {
			long val = source.readLong();
			return PreciseTimestamp.fromLong(val, precision);
		} else {
			int length = source.readInt();
			byte[] bytes = new byte[length];
			source.readFully(bytes);
			return PreciseTimestamp.fromUnscaledBytes(bytes, precision);
		}
	}

	@Override
	public PreciseTimestamp deserialize(PreciseTimestamp reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		if (PreciseTimestamp.isCompact(precision)) {
			target.writeLong(source.readLong());
		} else {
			int length = source.readInt();
			target.writeInt(length);
			target.write(source, length);
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

		PreciseTimestampSerializer that = (PreciseTimestampSerializer) obj;
		return precision == that.precision;
	}

	@Override
	public int hashCode() {
		return precision;
	}

	@Override
	public TypeSerializerSnapshot<PreciseTimestamp> snapshotConfiguration() {
		return new PreciseTimestampSerializerSnapshot(precision);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link PreciseTimestampSerializer}.
	 */
	public static final class PreciseTimestampSerializerSnapshot implements TypeSerializerSnapshot<PreciseTimestamp> {

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
		public TypeSerializer<PreciseTimestamp> restoreSerializer() {
			return new PreciseTimestampSerializer(previousPrecision);
		}

		@Override
		public TypeSerializerSchemaCompatibility<PreciseTimestamp> resolveSchemaCompatibility(TypeSerializer<PreciseTimestamp> newSerializer) {
			if (!(newSerializer instanceof PreciseTimestampSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			PreciseTimestampSerializer precisionTimestampSerializer = (PreciseTimestampSerializer) newSerializer;
			if (previousPrecision != precisionTimestampSerializer.precision) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
