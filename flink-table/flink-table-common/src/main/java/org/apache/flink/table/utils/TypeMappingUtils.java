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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;

/**
 * Utility methods for dealing with {@link org.apache.flink.table.sources.TableSource}.
 */
@Internal
public final class TypeMappingUtils {

	/**
	 * Computes indices of physical fields corresponding to the selected logical fields of a {@link TableSchema}.
	 *
	 * @param logicalColumns         Logical columns that describe the physical type.
	 * @param physicalType           Physical type to retrieve indices from.
	 * @param nameRemapping          Additional remapping of a logical to a physical field name.
	 *                               TimestampExtractor works with logical names, but accesses physical
	 *                               fields
	 * @return Physical indices of logical fields selected with {@code projectedLogicalFields} mask.
	 */
	public static int[] computePhysicalIndices(
			List<TableColumn> logicalColumns,
			DataType physicalType,
			Function<String, String> nameRemapping) {

		Map<TableColumn, Integer> physicalIndexLookup = computePhysicalIndices(
			logicalColumns.stream(),
			physicalType,
			nameRemapping);

		return logicalColumns.stream().mapToInt(physicalIndexLookup::get).toArray();
	}

	/**
	 * Computes indices of physical fields corresponding to the selected logical fields of a {@link TableSchema}.
	 *
	 * <p>It puts markers (idx < 0) for time attributes extracted from {@link DefinedProctimeAttribute}
	 * and {@link DefinedRowtimeAttributes}
	 *
	 * <p>{@link TypeMappingUtils#computePhysicalIndices(List, DataType, Function)} should be preferred. The
	 * time attribute markers should not be used anymore.
	 *
	 * @param tableSource     Used to extract {@link DefinedRowtimeAttributes}, {@link DefinedProctimeAttribute}
	 *                        and {@link TableSource#getProducedDataType()}.
	 * @param logicalColumns  Logical columns that describe the physical type.
	 * @param streamMarkers   If true puts stream markers otherwise puts batch markers.
	 * @param nameRemapping   Additional remapping of a logical to a physical field name.
	 *                        TimestampExtractor works with logical names, but accesses physical
	 *                        fields
	 * @return Physical indices of logical fields selected with {@code projectedLogicalFields} mask.
	 */
	public static int[] computePhysicalIndicesOrTimeAttributeMarkers(
			TableSource<?> tableSource,
			List<TableColumn> logicalColumns,
			boolean streamMarkers,
			Function<String, String> nameRemapping) {
		Optional<String> proctimeAttribute = getProctimeAttribute(tableSource);
		List<String> rowtimeAttributes = getRowtimeAttributes(tableSource);

		List<TableColumn> columnsWithoutTimeAttributes = logicalColumns.stream().filter(col ->
			!rowtimeAttributes.contains(col.getName())
				&& proctimeAttribute.map(attr -> !attr.equals(col.getName())).orElse(true))
			.collect(Collectors.toList());

		Map<TableColumn, Integer> columnsToPhysicalIndices = TypeMappingUtils.computePhysicalIndices(
			columnsWithoutTimeAttributes.stream(),
			tableSource.getProducedDataType(),
			nameRemapping
		);

		return logicalColumns.stream().mapToInt(logicalColumn -> {
			if (proctimeAttribute.map(attr -> attr.equals(logicalColumn.getName())).orElse(false)) {
				verifyTimeAttributeType(logicalColumn, "Proctime");

				if (streamMarkers) {
					return TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER;
				} else {
					return TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER;
				}
			} else if (rowtimeAttributes.contains(logicalColumn.getName())) {
				verifyTimeAttributeType(logicalColumn, "Rowtime");

				if (streamMarkers) {
					return TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER;
				} else {
					return TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER;
				}
			} else {
				return columnsToPhysicalIndices.get(logicalColumn);
			}
		}).toArray();
	}

	private static void verifyTimeAttributeType(TableColumn logicalColumn, String rowtimeOrProctime) {
		if (!hasFamily(logicalColumn.getType().getLogicalType(), LogicalTypeFamily.TIMESTAMP)) {
			throw new ValidationException(String.format(
				"%s field '%s' has invalid type %s. %s attributes must be of a Timestamp family.",
				rowtimeOrProctime,
				logicalColumn.getName(),
				logicalColumn.getType(),
				rowtimeOrProctime));
		}
	}

	private static Map<TableColumn, Integer> computePhysicalIndices(
			Stream<TableColumn> columns,
			DataType physicalType,
			Function<String, String> nameRemappingFunction) {
		if (LogicalTypeChecks.isCompositeType(physicalType.getLogicalType())) {
			TableSchema physicalSchema = DataTypeUtils.expandCompositeTypeToSchema(physicalType);
			return computeInCompositeType(columns, physicalSchema, wrapWithNotNullCheck(nameRemappingFunction));
		} else {
			return computeInSimpleType(columns, physicalType);
		}
	}

	private static Function<String, String> wrapWithNotNullCheck(Function<String, String> nameRemapping) {
		return name -> {
			String resolvedFieldName = nameRemapping.apply(name);
			if (resolvedFieldName == null) {
				throw new ValidationException(String.format(
					"Field '%s' could not be resolved by the field mapping.",
					name));
			}
			return resolvedFieldName;
		};
	}

	private static Map<TableColumn, Integer> computeInCompositeType(
			Stream<TableColumn> columns,
			TableSchema physicalSchema,
			Function<String, String> nameRemappingFunction) {
		return columns.collect(
			Collectors.toMap(
				Function.identity(),
				column -> {
					String remappedName = nameRemappingFunction.apply(column.getName());

					int idx = IntStream.range(0, physicalSchema.getFieldCount())
						.filter(i -> physicalSchema.getFieldName(i).get().equals(remappedName))
						.findFirst()
						.orElseThrow(() -> new ValidationException(String.format(
							"Could not map %s column to the underlying physical type %s. No such field.",
							column.getName(),
							physicalSchema
						)));

					LogicalType physicalFieldType = physicalSchema.getFieldDataType(idx).get().getLogicalType();
					LogicalType logicalFieldType = column.getType().getLogicalType();

					checkIfCompatible(
						physicalFieldType,
						logicalFieldType,
						(cause) -> new ValidationException(
							String.format(
								"Type %s of table field '%s' does not match with " +
									"the physical type %s of the '%s' field of the TableSource return type.",
								logicalFieldType,
								column.getName(),
								physicalFieldType,
								remappedName),
							cause));

					return idx;
				}
			)
		);
	}

	private static void checkIfCompatible(
			LogicalType physicalFieldType,
			LogicalType logicalFieldType,
			Function<Throwable, ValidationException> exceptionSupplier) {
		if (LogicalTypeChecks.areTypesCompatible(physicalFieldType, logicalFieldType)) {
			return;
		}

		physicalFieldType.accept(new LogicalTypeDefaultVisitor<Void>() {
			@Override
			public Void visit(LogicalType other) {
				if (other instanceof LegacyTypeInformationType && other.getTypeRoot() == LogicalTypeRoot.DECIMAL) {
					if (!(logicalFieldType instanceof DecimalType)) {
						throw exceptionSupplier.apply(null);
					}

					DecimalType logicalDecimalType = (DecimalType) logicalFieldType;
					if (logicalDecimalType.getPrecision() != DecimalType.MAX_PRECISION ||
							logicalDecimalType.getScale() != 18) {
						throw exceptionSupplier.apply(new ValidationException(
							"Legacy decimal type can only be mapped to DECIMAL(38, 18)."));
					}

					return null;
				}

				return defaultMethod(other);
			}

			@Override
			protected Void defaultMethod(LogicalType logicalType) {
				throw exceptionSupplier.apply(null);
			}
		});
	}

	private static Map<TableColumn, Integer> computeInSimpleType(
			Stream<TableColumn> columns,
			DataType physicalType) {

		Map<TableColumn, Integer> indices = columns.collect(
			Collectors.toMap(
				Function.identity(),
				col -> 0
			)
		);

		if (indices.keySet().size() > 1) {
			throw new ValidationException(String.format(
				"More than one table field matched to atomic input type %s.)",
				physicalType));
		}

		return indices;
	}

	/** Returns a list with all rowtime attribute names of the [[TableSource]]. */
	private static List<String> getRowtimeAttributes(TableSource<?> tableSource){
		if (tableSource instanceof DefinedRowtimeAttributes) {
			return ((DefinedRowtimeAttributes) tableSource).getRowtimeAttributeDescriptors()
				.stream()
				.map(RowtimeAttributeDescriptor::getAttributeName)
				.collect(Collectors.toList());
		} else {
			return Collections.emptyList();
		}
	}

	/** Returns the proctime attribute of the [[TableSource]] if it is defined. */
	private static Optional<String> getProctimeAttribute(TableSource<?> tableSource) {
		if (tableSource instanceof DefinedProctimeAttribute) {
			return Optional.ofNullable(((DefinedProctimeAttribute) tableSource).getProctimeAttribute());
		} else {
			return Optional.empty();
		}
	}

	private TypeMappingUtils() {
	}
}
