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
import org.apache.flink.table.data.MapData;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Logical type of an associative array that maps keys (including {@code NULL}) to values (including
 * {@code NULL}). A map cannot contain duplicate keys; each key can map to at most one value. There
 * is no restriction of key types; it is the responsibility of the user to ensure uniqueness. The map
 * type is an extension to the SQL standard.
 *
 * <p>The serialized string representation is {@code MAP<kt, vt>} where {@code kt} is the logical type
 * of the key elements and {@code vt} is the logical type of the value elements.
 */
@PublicEvolving
public final class MapType extends LogicalType {

	public static final String FORMAT = "MAP<%s, %s>";

	private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
		Map.class.getName(),
		MapData.class.getName());

	private static final Class<?> DEFAULT_CONVERSION = Map.class;

	private final LogicalType keyType;

	private final LogicalType valueType;

	public MapType(boolean isNullable, LogicalType keyType, LogicalType valueType) {
		super(isNullable, LogicalTypeRoot.MAP);
		this.keyType = Preconditions.checkNotNull(keyType, "Key type must not be null.");
		this.valueType = Preconditions.checkNotNull(valueType, "Value type must not be null.");
	}

	public MapType(LogicalType keyType, LogicalType valueType) {
		this(true, keyType, valueType);
	}

	public LogicalType getKeyType() {
		return keyType;
	}

	public LogicalType getValueType() {
		return valueType;
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new MapType(isNullable, keyType.copy(), valueType.copy());
	}

	@Override
	public String asSummaryString() {
		return withNullability(FORMAT,
			keyType.asSummaryString(),
			valueType.asSummaryString());
	}

	@Override
	public String asSerializableString() {
		return withNullability(FORMAT,
			keyType.asSerializableString(),
			valueType.asSerializableString());
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		if (Map.class.isAssignableFrom(clazz)) {
			return true;
		}
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
		return Collections.unmodifiableList(Arrays.asList(keyType, valueType));
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
		MapType mapType = (MapType) o;
		return keyType.equals(mapType.keyType) && valueType.equals(mapType.valueType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), keyType, valueType);
	}
}
