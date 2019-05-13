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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Logical type of an arbitrary serialized type. This type is a black box within the table ecosystem
 * and is only deserialized at the edges. The any type is an extension to the SQL standard.
 *
 * <p>The serialized string representation is {@code ANY(c, s)} where {@code c} is the originating
 * class and {@code s} is the serialized {@link TypeSerializerSnapshot} in Base64 encoding.
 *
 * @param <T> originating class for this type
 */
@PublicEvolving
public final class AnyType<T> extends LogicalType {

	private static final String FORMAT = "ANY(%s, %s)";

	private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
		byte[].class.getName(),
		"org.apache.flink.table.dataformat.BinaryGeneric");

	private final Class<T> clazz;

	private final TypeSerializer<T> serializer;

	private transient String serializerString;

	public AnyType(boolean isNullable, Class<T> clazz, TypeSerializer<T> serializer) {
		super(isNullable, LogicalTypeRoot.ANY);
		this.clazz = Preconditions.checkNotNull(clazz, "Class must not be null.");
		this.serializer = Preconditions.checkNotNull(serializer, "Serializer must not be null.");
	}

	public AnyType(Class<T> clazz, TypeSerializer<T> serializer) {
		this(true, clazz, serializer);
	}

	public Class<T> getOriginatingClass() {
		return clazz;
	}

	public TypeSerializer<T> getTypeSerializer() {
		return serializer;
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new AnyType<>(isNullable, clazz, serializer.duplicate());
	}

	@Override
	public String asSummaryString() {
		return withNullability(FORMAT, clazz.getName(), "...");
	}

	@Override
	public String asSerializableString() {
		return withNullability(FORMAT, clazz.getName(), getOrCreateSerializerString());
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		return this.clazz.isAssignableFrom(clazz) ||
			INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		return clazz.isAssignableFrom(this.clazz) ||
			INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public Class<?> getDefaultConversion() {
		return clazz;
	}

	@Override
	public List<LogicalType> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(LogicalTypeVisitor<R> visitor) {
		return visitor.visit(this);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		AnyType<?> anyType = (AnyType<?>) o;
		return clazz.equals(anyType.clazz) && serializer.equals(anyType.serializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), clazz, serializer);
	}

	// --------------------------------------------------------------------------------------------

	private String getOrCreateSerializerString() {
		if (serializerString == null) {
			final DataOutputSerializer outputSerializer = new DataOutputSerializer(128);
			try {
				serializer.snapshotConfiguration().writeSnapshot(outputSerializer);
				serializerString = EncodingUtils.encodeBytesToBase64(outputSerializer.getCopyOfBuffer());
				return serializerString;
			} catch (Exception e) {
				throw new TableException(String.format(
					"Unable to generate a string representation of the serializer snapshot of '%s' " +
						"describing the class '%s' for the ANY type.",
					serializer.getClass().getName(),
					clazz.toString()));
			}
		}
		return serializerString;
	}
}
