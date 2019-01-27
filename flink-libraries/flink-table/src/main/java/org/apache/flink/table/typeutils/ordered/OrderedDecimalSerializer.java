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
import org.apache.flink.table.dataformat.Decimal;

import java.io.IOException;

/**
 * Serializer for {@link Decimal}. The serialized value maintains the sort order of the original value.
 */
@Internal
public final class OrderedDecimalSerializer extends TypeSerializer<Decimal> {

	private static final long serialVersionUID = 1L;

	public static OrderedDecimalSerializer of(int precision, int scale, OrderedBytes.Order ord) {
		return new OrderedDecimalSerializer(precision, scale, ord);
	}

	private final int precision;
	private final int scale;
	private final OrderedBytes orderedBytes;
	private final OrderedBytes.Order ord;

	public OrderedDecimalSerializer(int precision, int scale, OrderedBytes.Order ord) {
		this.precision = precision;
		this.scale = scale;
		this.orderedBytes = new OrderedBytes();
		this.ord = ord;
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
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(Decimal record, DataOutputView target) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("The record must not be null.");
		}
		orderedBytes.encodeDecimal(target, record, ord);
	}

	@Override
	public Decimal deserialize(DataInputView source) throws IOException {
		return orderedBytes.decodeDecimal(source, precision, scale, ord);
	}

	@Override
	public Decimal deserialize(Decimal record, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		orderedBytes.encodeDecimal(target, orderedBytes.decodeDecimal(source, precision, scale, ord), ord);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof OrderedDecimalSerializer;
	}

	@Override
	public OrderedDecimalSerializer duplicate() {
		return new OrderedDecimalSerializer(precision, scale, ord);
	}

	@Override
	public int hashCode() {
		return getSerializationFormatIdentifier().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof OrderedDecimalSerializer)) {
			return false;
		}
		OrderedDecimalSerializer that = (OrderedDecimalSerializer) obj;
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
