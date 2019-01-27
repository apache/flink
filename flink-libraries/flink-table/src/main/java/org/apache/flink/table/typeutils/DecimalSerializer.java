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

package org.apache.flink.table.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.ParameterlessTypeSerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.dataformat.Decimal;

import java.io.IOException;

/**
 * Serializer for {@link Decimal}.
 */
@Internal
public final class DecimalSerializer extends TypeSerializer<Decimal> {

	private static final long serialVersionUID = 1L;

	public static DecimalSerializer of(int precision, int scale) {
		return new DecimalSerializer(precision, scale);
	}

	final int precision;
	final int scale;

	public DecimalSerializer(int precision, int scale) {
		this.precision = precision;
		this.scale = scale;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public Decimal createInstance() {
		return Decimal.zero(precision, scale);
	}

	@Override
	public Decimal copy(Decimal from) {
		return from.copy();
	}

	@Override
	public Decimal copy(Decimal from, Decimal reuse) {
		return copy(from); // TODO
	}

	@Override
	public int getLength() {
		return -1; // TODO
	}

	@Override
	public void serialize(Decimal record, DataOutputView target) throws IOException {
		if (Decimal.isCompact(precision)) {
			assert record.isCompact();
			target.writeLong(record.toUnscaledLong());
		} else {
			byte[] bytes = record.toUnscaledBytes();
			target.writeInt(bytes.length);
			target.write(bytes);
		}
	}

	@Override
	public Decimal deserialize(DataInputView source) throws IOException {
		if (Decimal.isCompact(precision)) {
			long longVal = source.readLong();
			return Decimal.fromUnscaledLong(precision, scale, longVal);
		} else {
			int length = source.readInt();
			byte[] bytes = new byte[length];
			source.readFully(bytes);
			return Decimal.fromUnscaledBytes(precision, scale, bytes);
		}
	}

	@Override
	public Decimal deserialize(Decimal record, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int length = source.readInt();
		byte[] bytes = new byte[length];
		source.readFully(bytes);
		target.write(bytes);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof DecimalSerializer;
	}

	@Override
	public DecimalSerializer duplicate() {
		return this;
	}

	@Override
	public int hashCode() {
		return getSerializationFormatIdentifier().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof DecimalSerializer)) {
			return false;
		}
		DecimalSerializer that = (DecimalSerializer) obj;
		return this.precision == that.precision && this.scale == that.scale;
	}

	private String serializationFormatIdentifier; // lazy
	private String getSerializationFormatIdentifier() {
		String id = serializationFormatIdentifier;
		if (id == null) {
			id = getClass().getCanonicalName() + "," + precision + "," + scale;
			serializationFormatIdentifier = id;
		}
		return id;
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new ParameterlessTypeSerializerConfig(getSerializationFormatIdentifier());
	}

	@Override
	public CompatibilityResult<Decimal> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof ParameterlessTypeSerializerConfig
			&& isCompatibleSerializationFormatIdentifier(
			((ParameterlessTypeSerializerConfig) configSnapshot).getSerializationFormatIdentifier())) {

			return CompatibilityResult.compatible();
		} else {
			return CompatibilityResult.requiresMigration();
		}
	}

	private boolean isCompatibleSerializationFormatIdentifier(String identifier) {
		return identifier.equals(getSerializationFormatIdentifier());
	}

}
