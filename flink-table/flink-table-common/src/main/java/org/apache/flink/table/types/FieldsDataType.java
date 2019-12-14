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

package org.apache.flink.table.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A data type that contains field data types (e.g. {@code ROW} or structured types).
 *
 * @see DataTypes for a list of supported data types
 */
@PublicEvolving
public final class FieldsDataType extends DataType {

	private final Map<String, DataType> fieldDataTypes;

	public FieldsDataType(
			LogicalType logicalType,
			@Nullable Class<?> conversionClass,
			Map<String, DataType> fieldDataTypes) {
		super(logicalType, conversionClass);
		this.fieldDataTypes = Collections.unmodifiableMap(
			new HashMap<>(
				Preconditions.checkNotNull(fieldDataTypes, "Field data types must not be null.")));
	}

	public FieldsDataType(
			LogicalType logicalType,
			Map<String, DataType> fieldDataTypes) {
		this(logicalType, null, fieldDataTypes);
	}

	public Map<String, DataType> getFieldDataTypes() {
		return fieldDataTypes;
	}

	@Override
	public DataType notNull() {
		return new FieldsDataType(
			logicalType.copy(false),
			conversionClass,
			fieldDataTypes);
	}

	@Override
	public DataType nullable() {
		return new FieldsDataType(
			logicalType.copy(true),
			conversionClass,
			fieldDataTypes);
	}

	@Override
	public DataType bridgedTo(Class<?> newConversionClass) {
		return new FieldsDataType(
			logicalType,
			Preconditions.checkNotNull(newConversionClass, "New conversion class must not be null."),
			fieldDataTypes);
	}

	@Override
	public <R> R accept(DataTypeVisitor<R> visitor) {
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
		FieldsDataType that = (FieldsDataType) o;
		return fieldDataTypes.equals(that.fieldDataTypes);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), fieldDataTypes);
	}
}
