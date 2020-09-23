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

package org.apache.flink.table.formats;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Arrays;

/**
 * RowDataTypeInfo for test.
 */
public class MockRowDataTypeInfo extends TupleTypeInfoBase<RowData> {

	private static final long serialVersionUID = 1L;

	private final String[] fieldNames;
	private final LogicalType[] logicalTypes;

	public MockRowDataTypeInfo(RowType rowType) {
		this(
			rowType.getFields().stream().map(RowType.RowField::getType).toArray(LogicalType[]::new),
			rowType.getFieldNames().toArray(new String[0]));
	}

	public MockRowDataTypeInfo(LogicalType[] logicalTypes, String[] fieldNames) {
		super(RowData.class, Arrays.stream(logicalTypes)
			.map(MockRowDataTypeInfo::fromLogicalTypeToTypeInfo)
			.toArray(TypeInformation[]::new));
		this.logicalTypes = logicalTypes;
		this.fieldNames = Arrays.copyOf(fieldNames, fieldNames.length);
	}

	@Override
	protected TypeComparatorBuilder<RowData> createTypeComparatorBuilder() {
		return null;
	}

	@Override
	public String[] getFieldNames() {
		return new String[0];
	}

	@Override
	public int getFieldIndex(String fieldName) {
		return 0;
	}

	@Override
	public TypeSerializer<RowData> createSerializer(
		ExecutionConfig config) {
		return null;
	}

	private static TypeInformation<?> fromLogicalTypeToTypeInfo(LogicalType logicalType) {
		switch (logicalType.getTypeRoot()) {
			case VARCHAR:
				return PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO;
			case BINARY:
			case VARBINARY:
				return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
			default:
				return TypeConversions.fromDataTypeToLegacyInfo(TypeConversions.fromLogicalToDataType(logicalType));
		}
	}
}
