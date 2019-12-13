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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Utility methods for dealing with {@link org.apache.flink.table.sources.TableSource}.
 */
@Internal
public final class TableSourceUtils {
	/**
	 * Computes indices of physical fields corresponding to the fields of a {@link TableSchema}.
	 *
	 * <p>It puts markers (idx < 0) for time attributes.
	 *
	 * @param logicalSchema          Logical schema that describes the physical type.
	 * @param physicalType           Physical type to retrieve indices from.
	 * @param streamMarkers          Flag to tell, what kind of time attribute markers should be used.
	 * @param nameRemappingFunction  Additional remapping of a logical to a physical field name.
	 *                               TimestampExtractor works with logical names, but accesses physical
	 *                               fields
	 * @return Physical indices of logical fields of given {@code TableSchema}.
	 */
	public static int[] computePhysicalIndices(
			TableSchema logicalSchema,
			DataType physicalType,
			boolean streamMarkers,
			Function<String, String> nameRemappingFunction) {
		return computePhysicalIndices(
			logicalSchema,
			IntStream.range(0, logicalSchema.getFieldCount()).toArray(),
			physicalType,
			streamMarkers,
			nameRemappingFunction);
	}

	/**
	 * Computes indices of physical fields corresponding to the selected logical fields of a {@link TableSchema}.
	 *
	 * <p>It puts markers (idx < 0) for time attributes.
	 *
	 * @param logicalSchema          Logical schema that describes the physical type.
	 * @param projectedLogicalFields Selected logical fields. The returned array will
	 *                               describe physical indices of logical fields selected with this mask.
	 * @param physicalType           Physical type to retrieve indices from.
	 * @param streamMarkers          Flag to tell, what kind of time attribute markers should be used.
	 * @param nameRemappingFunction  Additional remapping of a logical to a physical field name.
	 *                               TimestampExtractor works with logical names, but accesses physical
	 *                               fields
	 * @return Physical indices of logical fields selected with {@code projectedLogicalFields} mask.
	 */
	public static int[] computePhysicalIndices(
			TableSchema logicalSchema,
			int[] projectedLogicalFields,
			DataType physicalType,
			boolean streamMarkers,
			Function<String, String> nameRemappingFunction) {

		Stream<TableColumn> columns = Arrays.stream(projectedLogicalFields)
			.mapToObj(logicalSchema::getTableColumn)
			.filter(Optional::isPresent)
			.map(Optional::get);

		return computePhysicalIndices(columns, physicalType, streamMarkers, nameRemappingFunction);
	}

	private static int[] computePhysicalIndices(
			Stream<TableColumn> columns,
			DataType physicalType,
			boolean isStreamTable,
			Function<String, String> nameRemappingFunction) {
		if (DataTypeUtils.isCompositeType(physicalType)) {
			TableSchema physicalSchema = TypeConversions.expandCompositeTypeToSchema(physicalType);
			return computeInCompositeType(columns, physicalSchema, isStreamTable, nameRemappingFunction);
		} else {
			return computeInSimpleType(columns, physicalType, isStreamTable);
		}
	}

	private static int[] computeInCompositeType(
			Stream<TableColumn> columns,
			TableSchema physicalSchema,
			boolean isStreamTable,
			Function<String, String> nameRemappingFunction) {

		return mapToIndex(
			columns,
			isStreamTable,
			column -> {
				String remappedName = nameRemappingFunction.apply(column.getName());

				int idx = IntStream.range(0, physicalSchema.getFieldCount())
					.filter(i -> physicalSchema.getFieldName(i).get().equals(remappedName))
					.findFirst()
					.orElseThrow(() -> new TableException(String.format(
						"Could not map %s column to the underlying physical type %s. No such field.",
						column.getName(),
						physicalSchema
					)));

				LogicalType physicalFieldType = physicalSchema.getFieldDataType(idx).get().getLogicalType();
				LogicalType logicalFieldType = column.getType().getLogicalType();

				if (!physicalFieldType.equals(logicalFieldType)) {
					throw new ValidationException(String.format(
						"Type %s of table field '%s' does not match with type %s of the field of the " +
							"TableSource return type.",
						physicalFieldType,
						column.getName(),
						logicalFieldType));
				}

				return idx;
			});
	}

	private static int[] computeInSimpleType(
			Stream<TableColumn> columns,
			DataType physicalType,
			boolean isStreamTable) {
		int[] indices = mapToIndex(
			columns,
			isStreamTable,
			column -> 0);

		if (Arrays.stream(indices).filter(i -> i == 0).count() > 1) {
			throw new ValidationException(String.format(
				"More than one table field matched to atomic input type %s.)",
				physicalType));
		}

		return indices;
	}

	private static int[] mapToIndex(
			Stream<TableColumn> columns,
			boolean isStreamTable,
			Function<TableColumn, Integer> nonTimeAttributeMapper) {
		return columns.mapToInt(tableColumn -> {
				LogicalType logicalType = tableColumn.getType().getLogicalType();
				if (isTimeAttribute(logicalType)) {
					return timeAttributeToIdxMarker(isStreamTable, logicalType);
				} else {
					return nonTimeAttributeMapper.apply(tableColumn);
				}
			}
		).toArray();
	}

	private static boolean isTimeAttribute(LogicalType logicalType) {
		return LogicalTypeChecks.hasFamily(logicalType, LogicalTypeFamily.TIMESTAMP) &&
			LogicalTypeChecks.isTimeAttribute(logicalType);
	}

	private static int timeAttributeToIdxMarker(boolean isStreamTable, LogicalType logicalType) {
		if (LogicalTypeChecks.isRowtimeAttribute(logicalType)) {
			if (isStreamTable) {
				return TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER;
			} else {
				return TimeIndicatorTypeInfo.ROWTIME_BATCH_MARKER;
			}
		} else {
			if (isStreamTable) {
				return TimeIndicatorTypeInfo.PROCTIME_STREAM_MARKER;
			} else {
				return TimeIndicatorTypeInfo.PROCTIME_BATCH_MARKER;
			}
		}
	}

	private TableSourceUtils() {
	}
}
