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
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities for handling {@link DataType}s.
 */
@Internal
public final class DataTypeUtils {

	/**
	 * Replaces the {@link LogicalType} of a {@link DataType}, i.e., it keeps the bridging class.
	 */
	public static DataType replaceLogicalType(DataType dataType, LogicalType replacement) {
		return LogicalTypeDataTypeConverter.toDataType(replacement)
			.bridgedTo(dataType.getConversionClass());
	}

	/**
	 * Transforms the given data type (can be nested) to a different data type using the given
	 * transformations. The given transformations will be called in order.
	 *
	 * @param typeToTransform data type to be transformed.
	 * @param transformations the transformations to transform data type to another type.
	 * @return the new data type,
	 */
	public static DataType transform(DataType typeToTransform, TypeTransformation... transformations) {
		Preconditions.checkArgument(transformations.length > 0, "transformations should not be empty.");
		DataType newType = typeToTransform;
		for (TypeTransformation transformation : transformations) {
			newType = newType.accept(new DataTypeTransformer(transformation));
		}
		return newType;
	}

	/**
	 * Expands a composite {@link DataType} to a corresponding {@link TableSchema}. Useful for
	 * flattening a column or mapping a physical to logical type of a table source
	 *
	 * <p>Throws an exception for a non composite type. You can use
	 * {@link LogicalTypeChecks#isCompositeType(LogicalType)} to check that.
	 *
	 * <p>It does not expand an atomic type on purpose, because that operation depends on the
	 * context. E.g. in case of a {@code FLATTEN} function such operation is not allowed, whereas
	 * when mapping a physical type to logical the field name should be derived from the logical schema.
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

	private DataTypeUtils() {
		// no instantiation
	}

	// ------------------------------------------------------------------------------------------

	private static class DataTypeTransformer implements DataTypeVisitor<DataType> {

		private final TypeTransformation transformation;

		private DataTypeTransformer(TypeTransformation transformation) {
			this.transformation = transformation;
		}

		@Override
		public DataType visit(AtomicDataType atomicDataType) {
			return transformation.transform(atomicDataType);
		}

		@Override
		public DataType visit(CollectionDataType collectionDataType) {
			DataType newElementType = collectionDataType.getElementDataType().accept(this);
			LogicalType logicalType = collectionDataType.getLogicalType();
			LogicalType newLogicalType;
			if (logicalType instanceof ArrayType) {
				newLogicalType = new ArrayType(
					logicalType.isNullable(),
					newElementType.getLogicalType());
			} else if (logicalType instanceof MultisetType){
				newLogicalType = new MultisetType(
					logicalType.isNullable(),
					newElementType.getLogicalType());
			} else {
				throw new UnsupportedOperationException("Unsupported logical type : " + logicalType);
			}
			return transformation.transform(new CollectionDataType(newLogicalType, newElementType));
		}

		@Override
		public DataType visit(FieldsDataType fieldsDataType) {
			Map<String, DataType> newFields = new HashMap<>();
			fieldsDataType.getFieldDataTypes().forEach((name, type) ->
				newFields.put(name, type.accept(this)));
			LogicalType logicalType = fieldsDataType.getLogicalType();
			LogicalType newLogicalType;
			if (logicalType instanceof RowType) {
				List<RowType.RowField> rowFields = ((RowType) logicalType).getFields();
				List<RowType.RowField> newRowFields = rowFields.stream()
					.map(f -> new RowType.RowField(
						f.getName(),
						newFields.get(f.getName()).getLogicalType(),
						f.getDescription().orElse(null)))
					.collect(Collectors.toList());
				newLogicalType = new RowType(
					logicalType.isNullable(),
					newRowFields);
			} else {
				throw new UnsupportedOperationException("Unsupported logical type : " + logicalType);
			}
			return transformation.transform(new FieldsDataType(newLogicalType, newFields));
		}

		@Override
		public DataType visit(KeyValueDataType keyValueDataType) {
			DataType newKeyType = keyValueDataType.getKeyDataType().accept(this);
			DataType newValueType = keyValueDataType.getValueDataType().accept(this);
			LogicalType logicalType = keyValueDataType.getLogicalType();
			LogicalType newLogicalType;
			if (logicalType instanceof MapType) {
				newLogicalType = new MapType(
					logicalType.isNullable(),
					newKeyType.getLogicalType(),
					newValueType.getLogicalType());
			} else {
				throw new UnsupportedOperationException("Unsupported logical type : " + logicalType);
			}
			return transformation.transform(new KeyValueDataType(newLogicalType, newKeyType, newValueType));
		}
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
			public TableSchema visit(DistinctType distinctType) {
				return distinctType.getSourceType().accept(this);
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
}
