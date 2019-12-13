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

package org.apache.flink.table.types.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Conversion hub for interoperability of {@link Class}, {@link TypeInformation}, {@link DataType},
 * and {@link LogicalType}.
 *
 * <p>See the corresponding converter classes for more information about how the conversion is performed.
 */
@Internal
public final class TypeConversions {

	public static DataType fromLegacyInfoToDataType(TypeInformation<?> typeInfo) {
		return LegacyTypeInfoDataTypeConverter.toDataType(typeInfo);
	}

	public static DataType[] fromLegacyInfoToDataType(TypeInformation<?>[] typeInfo) {
		return Stream.of(typeInfo)
			.map(TypeConversions::fromLegacyInfoToDataType)
			.toArray(DataType[]::new);
	}

	public static TypeInformation<?> fromDataTypeToLegacyInfo(DataType dataType) {
		return LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(dataType);
	}

	public static TypeInformation<?>[] fromDataTypeToLegacyInfo(DataType[] dataType) {
		return Stream.of(dataType)
			.map(TypeConversions::fromDataTypeToLegacyInfo)
			.toArray(TypeInformation[]::new);
	}

	public static Optional<DataType> fromClassToDataType(Class<?> clazz) {
		return ClassDataTypeConverter.extractDataType(clazz);
	}

	public static DataType fromLogicalToDataType(LogicalType logicalType) {
		return LogicalTypeDataTypeConverter.toDataType(logicalType);
	}

	public static DataType[] fromLogicalToDataType(LogicalType[] logicalTypes) {
		return Stream.of(logicalTypes)
			.map(LogicalTypeDataTypeConverter::toDataType)
			.toArray(DataType[]::new);
	}

	public static LogicalType fromDataToLogicalType(DataType dataType) {
		return dataType.getLogicalType();
	}

	public static LogicalType[] fromDataToLogicalType(DataType[] dataTypes) {
		return Stream.of(dataTypes)
			.map(TypeConversions::fromDataToLogicalType)
			.toArray(LogicalType[]::new);
	}

	/**
	 * Expands a composite {@link DataType} to a corresponding {@link TableSchema}. Useful for
	 * flattening a column or mapping a physical to logical type of a table source
	 *
	 * <p>Throws an exception for a non composite type. You can use
	 * {@link DataTypeUtils#isCompositeType(DataType)} to check that.
	 *
	 * @param dataType Data type to expand. Must be a composite type.
	 * @return A corresponding table schema.
	 */
	public static TableSchema expandCompositeTypeToSchema(DataType dataType) {
		if (dataType instanceof FieldsDataType) {
			return expandCompositeType((FieldsDataType) dataType);
		} else if (dataType.getLogicalType() instanceof LegacyTypeInformationType &&
			dataType.getLogicalType().getTypeRoot() == LogicalTypeRoot.STRUCTURED_TYPE) {
			return expandLegacyCompositeType(dataType);
		}

		throw new IllegalArgumentException("Expected a composite type");
	}

	private static TableSchema expandCompositeType(FieldsDataType dataType) {
		Map<String, DataType> fieldDataTypes = dataType.getFieldDataTypes();
		return dataType.getLogicalType().accept(new LogicalTypeDefaultVisitor<TableSchema>() {
			@Override
			public TableSchema visit(RowType rowType) {
				return expandRowType(rowType, fieldDataTypes);
			}

			@Override
			public TableSchema visit(StructuredType structuredType) {
				return expandStructuredType(structuredType, fieldDataTypes);
			}

			@Override
			protected TableSchema defaultMethod(LogicalType logicalType) {
				throw new IllegalArgumentException("Expected a composite type");
			}
		});
	}

	private static TableSchema expandLegacyCompositeType(DataType dataType) {
		// legacy composite type
		CompositeType<?> compositeType = (CompositeType<?>) ((LegacyTypeInformationType<?>) dataType.getLogicalType())
			.getTypeInformation();

		String[] fieldNames = compositeType.getFieldNames();
		TypeInformation<?>[] fieldTypes = Arrays.stream(fieldNames)
			.map(compositeType::getTypeAt)
			.toArray(TypeInformation[]::new);

		return new TableSchema(fieldNames, fieldTypes);
	}

	private static TableSchema expandStructuredType(
			StructuredType structuredType,
			Map<String, DataType> fieldDataTypes) {
		String[] fieldNames = structuredType.getAttributes()
			.stream()
			.map(StructuredType.StructuredAttribute::getName)
			.toArray(String[]::new);
		DataType[] dataTypes = structuredType.getAttributes()
			.stream()
			.map(attr -> fieldDataTypes.get(attr.getName()))
			.toArray(DataType[]::new);
		return TableSchema.builder()
			.fields(fieldNames, dataTypes)
			.build();
	}

	private static TableSchema expandRowType(
			RowType rowType,
			Map<String, DataType> fieldDataTypes) {
		String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
		DataType[] dataTypes = rowType.getFields()
			.stream()
			.map(field -> fieldDataTypes.get(field.getName()))
			.toArray(DataType[]::new);
		return TableSchema.builder()
			.fields(fieldNames, dataTypes)
			.build();
	}

	private TypeConversions() {
		// no instance
	}
}
