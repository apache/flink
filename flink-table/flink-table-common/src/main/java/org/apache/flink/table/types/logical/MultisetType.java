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
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Logical type of a multiset (=bag). Unlike a set, it allows for multiple instances for each of its
 * elements with a common subtype. Each unique value (including {@code NULL}) is mapped to some
 * multiplicity. There is no restriction of element types; it is the responsibility of the user to
 * ensure uniqueness.
 *
 * <p>The serialized string representation is {@code MULTISET<t>} where {@code t} is the logical type
 * of the contained elements. {@code t MULTISET} is a synonym for being closer to the SQL standard.
 *
 * <p>A conversion is possible through a map that assigns each value to an integer multiplicity
 * ({@code Map<t, Integer>}).
 */
@PublicEvolving
public final class MultisetType extends LogicalType {

	private static final String FORMAT = "MULTISET<%s>";

	private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
		Map.class.getName(),
		"org.apache.flink.table.dataformat.BinaryMap");

	private static final Class<?> DEFAULT_CONVERSION = Map.class;

	private final LogicalType elementType;

	public MultisetType(boolean isNullable, LogicalType elementType) {
		super(isNullable, LogicalTypeRoot.MULTISET);
		this.elementType = Preconditions.checkNotNull(elementType, "Element type must not be null.");
	}

	public MultisetType(LogicalType elementType) {
		this(true, elementType);
	}

	public LogicalType getElementType() {
		return elementType;
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new MultisetType(isNullable, elementType.copy());
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
		return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public Class<?> getDefaultConversion() {
		return DEFAULT_CONVERSION;
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
		MultisetType that = (MultisetType) o;
		return elementType.equals(that.elementType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), elementType);
	}
}
