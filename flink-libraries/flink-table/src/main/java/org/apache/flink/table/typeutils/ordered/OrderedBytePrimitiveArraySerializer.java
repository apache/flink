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

package org.apache.flink.table.typeutils.ordered;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.ParameterlessTypeSerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A serializer for byte arrays. The serialized value maintains the sort order of the original value.
 */
@Internal
public final class OrderedBytePrimitiveArraySerializer extends TypeSerializer<byte[]> {

	private static final long serialVersionUID = 1L;

	private static final byte[] EMPTY = new byte[0];

	public static final OrderedBytePrimitiveArraySerializer ASC_INSTANCE =
		new OrderedBytePrimitiveArraySerializer(OrderedBytes.Order.ASCENDING);

	public static final OrderedBytePrimitiveArraySerializer DESC_INSTANCE =
		new OrderedBytePrimitiveArraySerializer(OrderedBytes.Order.DESCENDING);

	private final OrderedBytes orderedBytes;

	private final OrderedBytes.Order ord;

	private OrderedBytePrimitiveArraySerializer(OrderedBytes.Order ord) {
		this.ord = ord;
		this.orderedBytes = new OrderedBytes();
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<byte[]> duplicate() {
		return new OrderedBytePrimitiveArraySerializer(this.ord);
	}

	@Override
	public byte[] createInstance() {
		return EMPTY;
	}

	@Override
	public byte[] copy(byte[] from) {
		byte[] copy = new byte[from.length];
		System.arraycopy(from, 0, copy, 0, from.length);
		return copy;
	}

	@Override
	public byte[] copy(byte[] from, byte[] reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(byte[] record, DataOutputView target) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("The record must not be null.");
		}

		orderedBytes.encodeByteArray(target, record, ord);
	}

	@Override
	public byte[] deserialize(DataInputView source) throws IOException {
		return orderedBytes.decodeByteArray(source, ord);
	}

	@Override
	public byte[] deserialize(byte[] reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		orderedBytes.encodeByteArray(target, orderedBytes.decodeByteArray(source, ord), ord);
	}

	@Override
	public boolean equals(Object obj) {
		if (canEqual(obj)) {
			OrderedBytePrimitiveArraySerializer other = (OrderedBytePrimitiveArraySerializer) obj;
			return ord.equals(other.ord);
		}

		return false;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof OrderedBytePrimitiveArraySerializer;
	}

	@Override
	public int hashCode() {
		return getSerializationFormatIdentifier().hashCode();
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		return new ParameterlessTypeSerializerConfig(getSerializationFormatIdentifier());
	}

	@Override
	public CompatibilityResult<byte[]> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof ParameterlessTypeSerializerConfig
			&& isCompatibleSerializationFormatIdentifier(
			((ParameterlessTypeSerializerConfig) configSnapshot).getSerializationFormatIdentifier())) {

			return CompatibilityResult.compatible();
		} else {
			return CompatibilityResult.requiresMigration();
		}
	}

	private String serializationFormatIdentifier; // lazy
	private String getSerializationFormatIdentifier() {
		String id = serializationFormatIdentifier;
		if (id == null) {
			id = getClass().getCanonicalName() + "," + ord;
			serializationFormatIdentifier = id;
		}
		return id;
	}

	private boolean isCompatibleSerializationFormatIdentifier(String identifier) {
		return identifier.equals(getSerializationFormatIdentifier());
	}
}
