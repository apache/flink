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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TypeInformation} for arrays of primitive types (int, long, double, ...).
 * Supports the creation of dedicated efficient serializers for these types.
 *
 * @param <T> The type represented by this type information, e.g., int[], double[], long[]
 */
@Internal
public class OrderedPrimitiveArrayTypeInfo<T> extends TypeInformation<T> {

	private static final long serialVersionUID = 1L;

	public static final OrderedPrimitiveArrayTypeInfo<byte[]>
		ASC_BYTE_PRIMITIVE_ARRAY_TYPE_INFO = new OrderedPrimitiveArrayTypeInfo<>(
		byte[].class, OrderedBytePrimitiveArraySerializer.ASC_INSTANCE, BytePrimitiveArraySerializer.INSTANCE);

	public static final OrderedPrimitiveArrayTypeInfo<byte[]>
		DESC_BYTE_PRIMITIVE_ARRAY_TYPE_INFO = new OrderedPrimitiveArrayTypeInfo<>(
		byte[].class, OrderedBytePrimitiveArraySerializer.DESC_INSTANCE, BytePrimitiveArraySerializer.INSTANCE);

	// --------------------------------------------------------------------------------------------

	/** The class of the array (such as int[].class). */
	private final Class<T> arrayClass;

	/** The serializer for the array. */
	private final TypeSerializer<T> serializer;

	/** The serializer used in heap backend,
	 * because the key&value is serialized together in heap backend. */
	private final TypeSerializer<T> heapSerializer;

	/**
	 * Creates a new type info.
	 * @param arrayClass The class of the array (such as int[].class)
	 * @param serializer The serializer for the array.
	 */
	private OrderedPrimitiveArrayTypeInfo(Class<T> arrayClass, TypeSerializer<T> serializer, TypeSerializer<T> heapSerializer) {
		this.arrayClass = checkNotNull(arrayClass);
		this.serializer = checkNotNull(serializer);
		this.heapSerializer = checkNotNull(heapSerializer);

		checkArgument(
			arrayClass.isArray() && arrayClass.getComponentType().isPrimitive(),
			"Class must represent an array of primitives");
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public boolean isBasicType() {
		return false;
	}

	@Override
	public boolean isTupleType() {
		return false;
	}

	@Override
	public int getArity() {
		return 1;
	}

	@Override
	public int getTotalFields() {
		return 1;
	}

	@Override
	public Class<T> getTypeClass() {
		return this.arrayClass;
	}

	@Override
	public boolean isKeyType() {
		return true;
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		return this.serializer;
	}

	@Override
	public String toString() {
		return arrayClass.getComponentType().getName() + "[]";
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof OrderedPrimitiveArrayTypeInfo) {
			@SuppressWarnings("unchecked")
			OrderedPrimitiveArrayTypeInfo<T>
				otherArray = (OrderedPrimitiveArrayTypeInfo<T>) other;

			return otherArray.canEqual(this) &&
				arrayClass == otherArray.arrayClass &&
				serializer.equals(otherArray.serializer);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(arrayClass, serializer);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof OrderedPrimitiveArrayTypeInfo;
	}
}
