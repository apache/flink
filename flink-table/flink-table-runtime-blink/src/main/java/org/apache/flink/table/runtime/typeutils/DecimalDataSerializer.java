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
import org.apache.flink.table.data.DecimalData;

import java.io.IOException;

/**
 * Serializer for {@link DecimalData}.
 */
@Internal
public final class DecimalDataSerializer extends TypeSerializer<DecimalData> {

	private static final long serialVersionUID = 1L;

	private final int precision;
	private final int scale;

	public DecimalDataSerializer(int precision, int scale) {
		this.precision = precision;
		this.scale = scale;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public DecimalData createInstance() {
		return DecimalData.zero(precision, scale);
	}

	@Override
	public DecimalData copy(DecimalData from) {
		return from.copy();
	}

	@Override
	public DecimalData copy(DecimalData from, DecimalData reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(DecimalData record, DataOutputView target) throws IOException {
		if (DecimalData.isCompact(precision)) {
			assert record.isCompact();
			target.writeLong(record.toUnscaledLong());
		} else {
			byte[] bytes = record.toUnscaledBytes();
			target.writeInt(bytes.length);
			target.write(bytes);
		}
	}

	@Override
	public DecimalData deserialize(DataInputView source) throws IOException {
		if (DecimalData.isCompact(precision)) {
			long longVal = source.readLong();
			return DecimalData.fromUnscaledLong(longVal, precision, scale);
		} else {
			int length = source.readInt();
			byte[] bytes = new byte[length];
			source.readFully(bytes);
			return DecimalData.fromUnscaledBytes(bytes, precision, scale);
		}
	}

	@Override
	public DecimalData deserialize(DecimalData record, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		if (DecimalData.isCompact(precision)) {
			target.writeLong(source.readLong());
		} else {
			int len = source.readInt();
			target.writeInt(len);
			target.write(source, len);
		}
	}

	@Override
	public DecimalDataSerializer duplicate() {
		return new DecimalDataSerializer(precision, scale);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		DecimalDataSerializer that = (DecimalDataSerializer) o;

		return precision == that.precision && scale == that.scale;
	}

	@Override
	public int hashCode() {
		int result = precision;
		result = 31 * result + scale;
		return result;
	}

	@Override
	public TypeSerializerSnapshot<DecimalData> snapshotConfiguration() {
		return new DecimalSerializerSnapshot(precision, scale);
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link DecimalDataSerializer}.
	 */
	public static final class DecimalSerializerSnapshot implements TypeSerializerSnapshot<DecimalData> {

		private static final int CURRENT_VERSION = 3;

		private int previousPrecision;
		private int previousScale;

		@SuppressWarnings("unused")
		public DecimalSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		DecimalSerializerSnapshot(int precision, int scale) {
			this.previousPrecision = precision;
			this.previousScale = scale;
		}

		@Override
		public int getCurrentVersion() {
			return CURRENT_VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			out.writeInt(previousPrecision);
			out.writeInt(previousScale);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
			this.previousPrecision = in.readInt();
			this.previousScale = in.readInt();
		}

		@Override
		public TypeSerializer<DecimalData> restoreSerializer() {
			return new DecimalDataSerializer(previousPrecision, previousScale);
		}

		@Override
		public TypeSerializerSchemaCompatibility<DecimalData> resolveSchemaCompatibility(TypeSerializer<DecimalData> newSerializer) {
			if (!(newSerializer instanceof DecimalDataSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			DecimalDataSerializer newDecimalDataSerializer = (DecimalDataSerializer) newSerializer;
			if (previousPrecision != newDecimalDataSerializer.precision ||
				previousScale != newDecimalDataSerializer.scale) {
				return TypeSerializerSchemaCompatibility.incompatible();
			} else {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
		}
	}
}
