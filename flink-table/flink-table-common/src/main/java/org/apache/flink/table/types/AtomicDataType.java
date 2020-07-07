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
import java.util.List;

/**
 * A data type that does not contain further data types (e.g. {@code INT} or {@code BOOLEAN}).
 *
 * @see DataTypes for a list of supported data types
 */
@PublicEvolving
public final class AtomicDataType extends DataType {

	public AtomicDataType(LogicalType logicalType, @Nullable Class<?> conversionClass) {
		super(logicalType, conversionClass);
	}

	public AtomicDataType(LogicalType logicalType) {
		super(logicalType, null);
	}

	@Override
	public DataType notNull() {
		return new AtomicDataType(
			logicalType.copy(false),
			conversionClass);
	}

	@Override
	public DataType nullable() {
		return new AtomicDataType(
			logicalType.copy(true),
			conversionClass);
	}

	@Override
	public DataType bridgedTo(Class<?> newConversionClass) {
		return new AtomicDataType(
			logicalType,
			Preconditions.checkNotNull(newConversionClass, "New conversion class must not be null."));
	}

	@Override
	public List<DataType> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(DataTypeVisitor<R> visitor) {
		return visitor.visit(this);
	}
}
