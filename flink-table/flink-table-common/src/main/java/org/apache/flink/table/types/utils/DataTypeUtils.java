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
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

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
}
