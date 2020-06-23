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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Logical type of an array of elements with same subtype. Compared to the SQL standard, the maximum
 * cardinality of an array cannot be specified but is fixed at {@link Integer#MAX_VALUE}. Also, any
 * valid type is supported as a subtype.
 *
 * <p>The serialized string representation is {@code ARRAY<t>} where {@code t} is the logical type of
 * the contained elements. {@code t ARRAY} is a synonym for being closer to the SQL standard.
 */
@PublicEvolving
public final class ArrayType extends LogicalType {

	public static final String FORMAT = "ARRAY<%s>";

	private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
		List.class.getName(),
		ArrayData.class.getName());

	private final LogicalType elementType;

	public ArrayType(boolean isNullable, LogicalType elementType) {
		super(isNullable, LogicalTypeRoot.ARRAY);
		this.elementType = Preconditions.checkNotNull(elementType, "Element type must not be null.");
	}

	public ArrayType(LogicalType elementType) {
		this(true, elementType);
	}

	public LogicalType getElementType() {
		return elementType;
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new ArrayType(isNullable, elementType.copy());
	}

	@Override
	public String asSummaryString() {
		return withNullability(FORMAT, elementType.asSummaryString());
	}

	@Override
	public String asSerializableString() {
		return withNullability(FORMAT, elementType.asSerializableString());
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		if (List.class.isAssignableFrom(clazz)) {
			return true;
		}
		if (INPUT_OUTPUT_CONVERSION.contains(clazz.getName())) {
			return true;
		}
		if (!clazz.isArray()) {
			return false;
		}
		return elementType.supportsInputConversion(clazz.getComponentType());
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		if (INPUT_OUTPUT_CONVERSION.contains(clazz.getName())) {
			return true;
		}
		if (!clazz.isArray()) {
			return false;
		}
		return elementType.supportsOutputConversion(clazz.getComponentType());
	}

	@Override
	public Class<?> getDefaultConversion() {
		return Array.newInstance(elementType.getDefaultConversion(), 0).getClass();
	}

	@Override
	public List<LogicalType> getChildren() {
		return Collections.singletonList(elementType);
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
		ArrayType arrayType = (ArrayType) o;
		return elementType.equals(arrayType.elementType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), elementType);
	}
}
