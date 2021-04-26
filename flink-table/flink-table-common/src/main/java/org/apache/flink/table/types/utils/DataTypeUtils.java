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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.inference.TypeTransformations;
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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.primitiveToWrapper;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldNames;
import static org.apache.flink.table.types.logical.utils.LogicalTypeUtils.toInternalConversionClass;

/**
 * Utilities for handling {@link DataType}s.
 */
@Internal
public final class DataTypeUtils {

	/**
	 * Creates a {@link DataType} from the given {@link LogicalType} with internal data structures.
	 */
	public static DataType toInternalDataType(LogicalType logicalType) {
		final DataType defaultDataType = TypeConversions.fromLogicalToDataType(logicalType);
		return transform(defaultDataType, TypeTransformations.TO_INTERNAL_CLASS);
	}

	/**
	 * Checks whether a given data type is an internal data structure.
	 */
	public static boolean isInternal(DataType dataType) {
		final Class<?> clazz = primitiveToWrapper(dataType.getConversionClass());
		return clazz == toInternalConversionClass(dataType.getLogicalType());
	}

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

	/**
	 * The {@link DataType} class can only partially verify the conversion class. This method can perform
	 * the final check when we know if the data type should be used for input.
	 */
	public static void validateInputDataType(DataType dataType) {
		dataType.accept(DataTypeInputClassValidator.INSTANCE);
	}

	/**
	 * The {@link DataType} class can only partially verify the conversion class. This method can perform
	 * the final check when we know if the data type should be used for output.
	 */
	public static void validateOutputDataType(DataType dataType) {
		dataType.accept(DataTypeOutputClassValidator.INSTANCE);
	}

	private DataTypeUtils() {
		// no instantiation
	}

	// ------------------------------------------------------------------------------------------

	private static class DataTypeInputClassValidator extends DataTypeDefaultVisitor<Void> {

		private static final DataTypeInputClassValidator INSTANCE = new DataTypeInputClassValidator();

		@Override
		protected Void defaultMethod(DataType dataType) {
			if (!dataType.getLogicalType().supportsInputConversion(dataType.getConversionClass())) {
				throw new ValidationException(
					String.format(
						"Data type '%s' does not support an input conversion from class '%s'.",
						dataType,
						dataType.getConversionClass().getName()));
			}
			dataType.getChildren().forEach(child -> child.accept(this));
			return null;
		}
	}

	private static class DataTypeOutputClassValidator extends DataTypeDefaultVisitor<Void> {

		private static final DataTypeOutputClassValidator INSTANCE = new DataTypeOutputClassValidator();

		@Override
		protected Void defaultMethod(DataType dataType) {
			if (!dataType.getLogicalType().supportsOutputConversion(dataType.getConversionClass())) {
				throw new ValidationException(
					String.format(
						"Data type '%s' does not support an output conversion to class '%s'.",
						dataType,
						dataType.getConversionClass().getName()));
			}
			dataType.getChildren().forEach(child -> child.accept(this));
			return null;
		}
	}

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
			final List<DataType> newFields = fieldsDataType.getChildren().stream()
				.map(dt -> dt.accept(this))
				.collect(Collectors.toList());

			final LogicalType logicalType = fieldsDataType.getLogicalType();
			final LogicalType newLogicalType;
			if (logicalType instanceof RowType) {
				final List<RowType.RowField> oldFields = ((RowType) logicalType).getFields();
				final List<RowType.RowField> newRowFields = IntStream.range(0, oldFields.size())
					.mapToObj(i ->
						new RowType.RowField(
							oldFields.get(i).getName(),
							newFields.get(i).getLogicalType(),
							oldFields.get(i).getDescription().orElse(null)))
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
		DataType[] fieldDataTypes = dataType.getChildren().toArray(new DataType[0]);
		return dataType.getLogicalType().accept(new LogicalTypeDefaultVisitor<TableSchema>() {
			@Override
			public TableSchema visit(RowType rowType) {
				return expandCompositeType(rowType, fieldDataTypes);
			}

			@Override
			public TableSchema visit(StructuredType structuredType) {
				return expandCompositeType(structuredType, fieldDataTypes);
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

	private static TableSchema expandCompositeType(
			LogicalType compositeType,
			DataType[] fieldDataTypes) {
		final String[] fieldNames = getFieldNames(compositeType).toArray(new String[0]);
		return TableSchema.builder()
			.fields(fieldNames, fieldDataTypes)
			.build();
	}
}
