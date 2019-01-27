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
import org.apache.flink.table.dataformat.BinaryString;

import java.io.IOException;

/**
 * Serializer for {@link BinaryString}.
 */
@Internal
public final class OrderedBinaryStringSerializer extends TypeSerializer<BinaryString> {

	private static final long serialVersionUID = 1L;

	public static final OrderedBinaryStringSerializer ASC_INSTANCE =
		new OrderedBinaryStringSerializer(OrderedBytes.Order.ASCENDING);

	public static final OrderedBinaryStringSerializer DESC_INSTANCE =
		new OrderedBinaryStringSerializer(OrderedBytes.Order.DESCENDING);

	private final OrderedBytes orderedBytes;

	private final OrderedBytes.Order ord;

	private OrderedBinaryStringSerializer(OrderedBytes.Order ord) {
		this.ord = ord;
		this.orderedBytes = new OrderedBytes();
	}

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public TypeSerializer<BinaryString> duplicate() {
		return new OrderedBinaryStringSerializer(this.ord);
	}

	@Override
	public BinaryString createInstance() {
		return BinaryString.EMPTY_UTF8;
	}

	@Override
	public BinaryString copy(BinaryString from) {
		return BinaryString.fromBytes(from.getBytes());
	}

	@Override
	public BinaryString copy(BinaryString from, BinaryString reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(BinaryString record, DataOutputView target) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("The record must not be null.");
		}
		orderedBytes.encodeBinaryString(target, record, ord);
	}

	@Override
	public BinaryString deserialize(DataInputView source) throws IOException {
		return orderedBytes.decodeBinaryString(source, ord);
	}

	@Override
	public BinaryString deserialize(BinaryString record, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		orderedBytes.encodeBinaryString(target, orderedBytes.decodeBinaryString(source, ord), ord);
	}

	@Override
	public boolean equals(Object obj) {
		if (canEqual(obj)) {
			OrderedBinaryStringSerializer other = (OrderedBinaryStringSerializer) obj;
			return ord.equals(other.ord);
		}

		return false;
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof OrderedBinaryStringSerializer;
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
	public CompatibilityResult<BinaryString> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
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
