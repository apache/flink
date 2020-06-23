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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Type information that wraps a serializer that originated from a {@link LogicalType}.
 *
 * <p>Use {@link #of(LogicalType)} for type information of internal data structures.
 */
@Internal
public final class WrapperTypeInfo<T> extends TypeInformation<T> {

	private static final String FORMAT = "%s(%s, %s)";

	private final LogicalType type;

	private final Class<T> typeClass;

	private final TypeSerializer<T> typeSerializer;

	public WrapperTypeInfo(LogicalType type, Class<T> typeClass, TypeSerializer<T> typeSerializer) {
		this.type = Preconditions.checkNotNull(type);
		this.typeClass = Preconditions.checkNotNull(typeClass);
		this.typeSerializer = Preconditions.checkNotNull(typeSerializer);
	}

	/**
	 * Creates type information for a logical type represented by internal data structures.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public static <T> WrapperTypeInfo<T> of(LogicalType type) {
		final Class<?> typeClass = LogicalTypeUtils.toInternalConversionClass(type);
		final TypeSerializer<?> serializer = InternalSerializers.create(type);
		return (WrapperTypeInfo<T>) new WrapperTypeInfo(type, typeClass, serializer);
	}

	public LogicalType toLogicalType() {
		return type;
	}

	public RowType toRowType() {
		return (RowType) type;
	}

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
		return typeClass;
	}

	@Override
	public boolean isKeyType() {
		return false;
	}

	@Override
	public TypeSerializer<T> createSerializer(ExecutionConfig config) {
		return typeSerializer;
	}

	@Override
	public String toString() {
		return String.format(
			FORMAT,
			type.asSummaryString(),
			typeClass.getName(),
			typeSerializer.getClass().getName());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final WrapperTypeInfo<?> that = (WrapperTypeInfo<?>) o;
		return type.equals(that.type) &&
			typeClass.equals(that.typeClass) &&
			typeSerializer.equals(that.typeSerializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(type, typeClass, typeSerializer);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof WrapperTypeInfo;
	}
}
