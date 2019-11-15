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

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;

/**
 * Utilities for {@link org.apache.flink.table.api.TableSchema} validation.
 */
public final class TableSchemaValidation {

	/**
	 * Validate the field names {@code fieldNames} and field types {@code fieldTypes}
	 * have equal number.
	 *
	 * @param fieldNames Field names
	 * @param fieldTypes Field data types
	 */
	public static void validateNameTypeNumberEqual(String[] fieldNames, DataType[] fieldTypes) {
		if (fieldNames.length != fieldTypes.length) {
			throw new ValidationException(
				"Number of field names and field data types must be equal.\n" +
					"Number of names is " + fieldNames.length +
					", number of data types is " + fieldTypes.length + ".\n" +
					"List of field names: " + Arrays.toString(fieldNames) + "\n" +
					"List of field data types: " + Arrays.toString(fieldTypes));
		}
	}

	/**
	 * Validate table columns, watermark specification and primary key.
	 */
	public static void validateSchema(
			List<TableColumn> columns,
			List<WatermarkSpec> watermarkSpecs,
			List<String> primaryKey) {
		// validate and create nested field name to type mapping.
		// we need nested field name to type mapping because the row time attribute
		// field can be nested.

		// this also check duplicate fields.
		final Map<String, DataType> fieldNameToType = new HashMap<>();
		for (TableColumn column : columns) {
			validateAndBuildNameToTypeMapping(fieldNameToType,
				column.getName(),
				column.getType(),
				"");
		}

		// primary key requires the key field is top-level (non-nested)
		Map<String, TableColumn> nameToColumn = columns.stream()
			.collect(Collectors.toMap(TableColumn::getName, Function.identity()));

		// validate primary key
		for (String key : primaryKey) {
			TableColumn column = nameToColumn.get(key);
			if (column == null) {
				throw new ValidationException("The primary key field '" + key +
					"' is not existed in the schema.");
			} else if (column.isGenerated()) {
				throw new ValidationException("The primary key field '" + key +
					"' should not be a generated column.");
			}
		}

		// validate watermark and rowtime attribute.
		for (WatermarkSpec watermark : watermarkSpecs) {
			String rowtimeAttribute = watermark.getRowtimeAttribute();
			DataType rowtimeType = Optional.ofNullable(fieldNameToType.get(rowtimeAttribute))
				.orElseThrow(() -> new ValidationException(String.format(
					"Rowtime attribute '%s' is not defined in schema.", rowtimeAttribute)));
			if (rowtimeType.getLogicalType().getTypeRoot() != TIMESTAMP_WITHOUT_TIME_ZONE) {
				throw new ValidationException(String.format(
					"Rowtime attribute '%s' must be of type TIMESTAMP but is of type '%s'.",
					rowtimeAttribute, rowtimeType));
			}
			LogicalType watermarkOutputType = watermark.getWatermarkExprOutputType().getLogicalType();
			if (watermarkOutputType.getTypeRoot() != TIMESTAMP_WITHOUT_TIME_ZONE) {
				throw new ValidationException(String.format(
					"Watermark strategy '%s' must be of type TIMESTAMP but is of type '%s'.",
					watermark.getWatermarkExpressionString(),
					watermarkOutputType.asSerializableString()));
			}
		}
	}

	/**
	 * Builds a mapping from field name to data type, the field name can be a nested field.
	 * This is mainly used for validating whether the rowtime attribute (might be nested) exists
	 * in the schema. During creating, it also validates whether there is duplicate field names.
	 *
	 * <p>For example, a "f0" field of ROW type has two nested fields "q1" and "q2". Then the
	 * mapping will be ["f0" -> ROW, "f0.q1" -> INT, "f0.q2" -> STRING].
	 *
	 * <pre>
	 * {@code
	 *     f0 ROW<q1 INT, q2 STRING>
	 * }
	 * </pre>
	 *
	 * @param fieldNameToType Field name to type mapping that to update
	 * @param fieldName       Name of this field, e.g. "q1" or "q2" in the above example
	 * @param fieldType       Data type of this field
	 * @param parentFieldName Field name of parent type, e.g. "f0" in the above example
	 */
	private static void validateAndBuildNameToTypeMapping(
		Map<String, DataType> fieldNameToType,
		String fieldName,
		DataType fieldType,
		String parentFieldName) {
		String fullFieldName = parentFieldName.isEmpty() ? fieldName : parentFieldName + "." + fieldName;
		DataType oldType = fieldNameToType.put(fullFieldName, fieldType);
		if (oldType != null) {
			throw new ValidationException("Field names must be unique. Duplicate field: '" + fullFieldName + "'");
		}
		if (fieldType instanceof FieldsDataType) {
			Map<String, DataType> fieldDataTypes = ((FieldsDataType) fieldType).getFieldDataTypes();
			fieldDataTypes.forEach((key, value) ->
				validateAndBuildNameToTypeMapping(fieldNameToType, key, value, fullFieldName));
		}
	}
}
