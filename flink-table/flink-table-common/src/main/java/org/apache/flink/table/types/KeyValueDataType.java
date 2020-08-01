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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A data type that contains a key and value data type (e.g. {@code MAP}).
 *
 * @see DataTypes for a list of supported data types
 */
@PublicEvolving
public final class KeyValueDataType extends DataType {

	private final DataType keyDataType;

	private final DataType valueDataType;

	public KeyValueDataType(
			LogicalType logicalType,
			@Nullable Class<?> conversionClass,
			DataType keyDataType,
			DataType valueDataType) {
		super(logicalType, conversionClass);
		this.keyDataType = Preconditions.checkNotNull(keyDataType, "Key data type must not be null.");
		this.valueDataType = Preconditions.checkNotNull(valueDataType, "Value data type must not be null.");
	}

	public KeyValueDataType(
			LogicalType logicalType,
			DataType keyDataType,
			DataType valueDataType) {
		this(logicalType, null, keyDataType, valueDataType);
	}

	public DataType getKeyDataType() {
		return keyDataType;
	}

	public DataType getValueDataType() {
		return valueDataType;
	}

	@Override
	public DataType notNull() {
		return new KeyValueDataType(
			logicalType.copy(false),
			conversionClass,
			keyDataType,
			valueDataType);
	}

	@Override
	public DataType nullable() {
		return new KeyValueDataType(
			logicalType.copy(true),
			conversionClass,
			keyDataType,
			valueDataType);
	}

	@Override
	public DataType bridgedTo(Class<?> newConversionClass) {
		return new KeyValueDataType(
			logicalType,
			Preconditions.checkNotNull(newConversionClass, "New conversion class must not be null."),
			keyDataType,
			valueDataType);
	}

	@Override
	public List<DataType> getChildren() {
		return Arrays.asList(keyDataType, valueDataType);
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
		KeyValueDataType that = (KeyValueDataType) o;
		return keyDataType.equals(that.keyDataType) && valueDataType.equals(that.valueDataType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), keyDataType, valueDataType);
	}
}
