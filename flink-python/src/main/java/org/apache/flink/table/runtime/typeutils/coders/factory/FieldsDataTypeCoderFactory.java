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

package org.apache.flink.table.runtime.typeutils.coders.factory;

import org.apache.flink.table.runtime.typeutils.coders.BaseRowCoder;
import org.apache.flink.table.runtime.typeutils.coders.RowCoder;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.beam.sdk.coders.Coder;

/**
 * The coder factory for {@link FieldsDataType}.
 */
public class FieldsDataTypeCoderFactory implements DataTypeCoderFactory<FieldsDataType> {
	public static FieldsDataTypeCoderFactory of(DataTypeVisitor<Coder> dataTypeVisitor, boolean isBlinkPlanner) {
		return new FieldsDataTypeCoderFactory(dataTypeVisitor, isBlinkPlanner);
	}

	private final DataTypeVisitor<Coder> dataTypeVisitor;

	private final boolean isBlinkPlanner;

	private FieldsDataTypeCoderFactory(DataTypeVisitor<Coder> dataTypeVisitor, boolean isBlinkPlanner) {
		this.dataTypeVisitor = dataTypeVisitor;
		this.isBlinkPlanner = isBlinkPlanner;
	}

	@Override
	public Coder findCoder(FieldsDataType dataType) {
		LogicalType logicalType = dataType.getLogicalType();
		LogicalType[] fieldTypes = dataType.getLogicalType().getChildren().toArray(new LogicalType[0]);
		if (logicalType instanceof RowType) {
			final Coder[] fieldCoders = ((RowType) logicalType).getFieldNames()
				.stream()
				.map(f -> dataType.getFieldDataTypes().get(f).accept(dataTypeVisitor))
				.toArray(Coder[]::new);
			if (isBlinkPlanner) {
				return new BaseRowCoder(fieldCoders, fieldTypes);
			}
			return new RowCoder(fieldCoders);
		}
		throw new IllegalArgumentException("No matched FieldsDataType Coder for " + logicalType);
	}
}
