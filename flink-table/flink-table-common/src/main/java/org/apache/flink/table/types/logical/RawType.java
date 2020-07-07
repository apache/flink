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
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Logical type of an arbitrary serialized type. This type is a black box within the table ecosystem
 * and is only deserialized at the edges. The raw type is an extension to the SQL standard.
 *
 * <p>The serialized string representation is {@code RAW('c', 's')} where {@code c} is the originating
 * class and {@code s} is the serialized {@link TypeSerializerSnapshot} in Base64 encoding.
 *
 * @param <T> originating class for this type
 */
@PublicEvolving
public final class RawType<T> extends LogicalType {

	public static final String FORMAT = "RAW('%s', '%s')";

	private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
		byte[].class.getName(),
		RawValueData.class.getName());

	private final Class<T> clazz;

	private final TypeSerializer<T> serializer;

	private transient String serializerString;

	public RawType(boolean isNullable, Class<T> clazz, TypeSerializer<T> serializer) {
		super(isNullable, LogicalTypeRoot.RAW);
		this.clazz = Preconditions.checkNotNull(clazz, "Class must not be null.");
		this.serializer = Preconditions.checkNotNull(serializer, "Serializer must not be null.");
	}

	public RawType(Class<T> clazz, TypeSerializer<T> serializer) {
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
		return new RawType<>(isNullable, clazz, serializer.duplicate());
	}

	@Override
	public String asSummaryString() {
		return withNullability(FORMAT, clazz.getName(), "...");
	}

	@Override
	public String asSerializableString() {
		return withNullability(FORMAT, clazz.getName(), getSerializerString());
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
		RawType<?> rawType = (RawType<?>) o;
		return clazz.equals(rawType.clazz) && serializer.equals(rawType.serializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), clazz, serializer);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Restores a raw type from the components of a serialized string representation.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static RawType<?> restore(
			ClassLoader classLoader,
			String className,
			String serializerString) {
		try {
			final Class<?> clazz = Class.forName(className, true, classLoader);
			final byte[] bytes = EncodingUtils.decodeBase64ToBytes(serializerString);
			final DataInputDeserializer inputDeserializer = new DataInputDeserializer(bytes);
			final TypeSerializerSnapshot<?> snapshot = TypeSerializerSnapshot.readVersionedSnapshot(
				inputDeserializer,
				classLoader);
			return (RawType<?>) new RawType(clazz, snapshot.restoreSerializer());
		} catch (Throwable t) {
			throw new ValidationException(
				String.format(
					"Unable to restore the RAW type of class '%s' with serializer snapshot '%s'.",
					className,
					serializerString),
				t);
		}
	}

	/**
	 * Returns the serialized {@link TypeSerializerSnapshot} in Base64 encoding of this raw type.
	 */
	public String getSerializerString() {
		if (serializerString == null) {
			final DataOutputSerializer outputSerializer = new DataOutputSerializer(128);
			try {
				TypeSerializerSnapshot.writeVersionedSnapshot(outputSerializer, serializer.snapshotConfiguration());
				serializerString = EncodingUtils.encodeBytesToBase64(outputSerializer.getCopyOfBuffer());
				return serializerString;
			} catch (Exception e) {
				throw new TableException(String.format(
					"Unable to generate a string representation of the serializer snapshot of '%s' " +
						"describing the class '%s' for the RAW type.",
					serializer.getClass().getName(),
					clazz.toString()), e);
			}
		}
		return serializerString;
	}
}
