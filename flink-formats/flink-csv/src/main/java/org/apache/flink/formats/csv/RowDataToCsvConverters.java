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

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ContainerNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

/** Tool class used to convert from {@link RowData} to CSV-format {@link JsonNode}. **/
@Internal
public class RowDataToCsvConverters implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Runtime converter that converts objects of Flink Table & SQL internal data structures
	 * to corresponding {@link JsonNode}s.
	 */
	public interface RowDataToCsvConverter extends Serializable {
		JsonNode convert(CsvMapper csvMapper, ContainerNode<?> container, RowData row);
	}

	private interface RowFieldConverter extends Serializable {
		JsonNode convert(CsvMapper csvMapper, ContainerNode<?> container, RowData row, int pos);
	}

	private interface ArrayElementConverter extends Serializable {
		JsonNode convert(CsvMapper csvMapper, ContainerNode<?> container, ArrayData array, int pos);
	}

	public static RowDataToCsvConverter createRowConverter(RowType type) {
		LogicalType[] fieldTypes = type.getFields().stream()
			.map(RowType.RowField::getType)
			.toArray(LogicalType[]::new);
		final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
		final RowFieldConverter[] fieldConverters = Arrays.stream(fieldTypes)
			.map(RowDataToCsvConverters::createNullableRowFieldConverter)
			.toArray(RowFieldConverter[]::new);
		final int rowArity = type.getFieldCount();
		return (csvMapper, container, row) -> {
			// top level reuses the object node container
			final ObjectNode objectNode = (ObjectNode) container;
			for (int i = 0; i < rowArity; i++) {
				objectNode.set(
					fieldNames[i],
					fieldConverters[i].convert(csvMapper, container, row, i));
			}
			return objectNode;
		};
	}

	private static RowFieldConverter createNullableRowFieldConverter(LogicalType fieldType) {
		final RowFieldConverter fieldConverter = createRowFieldConverter(fieldType);
		return (csvMapper, container, row, pos) -> {
			if (row.isNullAt(pos)) {
				return container.nullNode();
			}
			return fieldConverter.convert(csvMapper, container, row, pos);
		};
	}

	private static RowFieldConverter createRowFieldConverter(LogicalType fieldType) {
		switch (fieldType.getTypeRoot()) {
			case NULL:
				return (csvMapper, container, row, pos) -> container.nullNode();
			case BOOLEAN:
				return (csvMapper, container, row, pos) -> container.booleanNode(row.getBoolean(pos));
			case TINYINT:
				return (csvMapper, container, row, pos) -> container.numberNode(row.getByte(pos));
			case SMALLINT:
				return (csvMapper, container, row, pos) -> container.numberNode(row.getShort(pos));
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return (csvMapper, container, row, pos) -> container.numberNode(row.getInt(pos));
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return (csvMapper, container, row, pos) -> container.numberNode(row.getLong(pos));
			case FLOAT:
				return (csvMapper, container, row, pos) -> container.numberNode(row.getFloat(pos));
			case DOUBLE:
				return (csvMapper, container, row, pos) -> container.numberNode(row.getDouble(pos));
			case CHAR:
			case VARCHAR:
				return (csvMapper, container, row, pos) -> container.textNode(row.getString(pos).toString());
			case BINARY:
			case VARBINARY:
				return (csvMapper, container, row, pos) -> container.binaryNode(row.getBinary(pos));
			case DATE:
				return (csvMapper, container, row, pos) -> convertDate(row.getInt(pos), container);
			case TIME_WITHOUT_TIME_ZONE:
				return (csvMapper, container, row, pos) -> convertTime(row.getInt(pos), container);
			case TIMESTAMP_WITH_TIME_ZONE:
				final int zonedTimestampPrecision = ((LocalZonedTimestampType) fieldType).getPrecision();
				return (csvMapper, container, row, pos) ->
					convertTimestamp(row.getTimestamp(pos, zonedTimestampPrecision), container);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				final int timestampPrecision = ((TimestampType) fieldType).getPrecision();
				return (csvMapper, container, row, pos) ->
					convertTimestamp(row.getTimestamp(pos, timestampPrecision), container);
			case DECIMAL:
				return createDecimalRowFieldConverter((DecimalType) fieldType);
			case ARRAY:
				return createArrayRowFieldConverter((ArrayType) fieldType);
			case ROW:
				return createRowRowFieldConverter((RowType) fieldType);
			case MAP:
			case MULTISET:
			case RAW:
			default:
				throw new UnsupportedOperationException("Unsupported type: " + fieldType);
		}
	}

	private static ArrayElementConverter createNullableArrayElementConverter(LogicalType fieldType) {
		final ArrayElementConverter elementConverter = createArrayElementConverter(fieldType);
		return (csvMapper, container, array, pos) -> {
			if (array.isNullAt(pos)) {
				return container.nullNode();
			}
			return elementConverter.convert(csvMapper, container, array, pos);
		};
	}

	private static ArrayElementConverter createArrayElementConverter(LogicalType fieldType) {
		switch (fieldType.getTypeRoot()) {
			case NULL:
				return (csvMapper, container, array, pos) -> container.nullNode();
			case BOOLEAN:
				return (csvMapper, container, array, pos) -> container.booleanNode(array.getBoolean(pos));
			case TINYINT:
				return (csvMapper, container, array, pos) -> container.numberNode(array.getByte(pos));
			case SMALLINT:
				return (csvMapper, container, array, pos) -> container.numberNode(array.getShort(pos));
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return (csvMapper, container, array, pos) -> container.numberNode(array.getInt(pos));
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return (csvMapper, container, array, pos) -> container.numberNode(array.getLong(pos));
			case FLOAT:
				return (csvMapper, container, array, pos) -> container.numberNode(array.getFloat(pos));
			case DOUBLE:
				return (csvMapper, container, array, pos) -> container.numberNode(array.getDouble(pos));
			case CHAR:
			case VARCHAR:
				return (csvMapper, container, array, pos) -> container.textNode(array.getString(pos).toString());
			case BINARY:
			case VARBINARY:
				return (csvMapper, container, array, pos) -> container.binaryNode(array.getBinary(pos));
			case DATE:
				return (csvMapper, container, array, pos) -> convertDate(array.getInt(pos), container);
			case TIME_WITHOUT_TIME_ZONE:
				return (csvMapper, container, array, pos) -> convertTime(array.getInt(pos), container);
			case TIMESTAMP_WITH_TIME_ZONE:
				final int zonedTimestampPrecision = ((LocalZonedTimestampType) fieldType).getPrecision();
				return (csvMapper, container, array, pos) ->
					convertTimestamp(array.getTimestamp(pos, zonedTimestampPrecision), container);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				final int timestampPrecision = ((TimestampType) fieldType).getPrecision();
				return (csvMapper, container, array, pos) ->
					convertTimestamp(array.getTimestamp(pos, timestampPrecision), container);
			case DECIMAL:
				return createDecimalArrayElementConverter((DecimalType) fieldType);
			// we don't support ARRAY and ROW in an ARRAY, see CsvRowSchemaConverter#validateNestedField
			case ARRAY:
			case ROW:
			case MAP:
			case MULTISET:
			case RAW:
			default:
				throw new UnsupportedOperationException("Unsupported type: " + fieldType);
		}
	}

	// ------------------------------------------------------------------------------------------
	// Field/Element Converters
	// ------------------------------------------------------------------------------------------

	private static RowFieldConverter createDecimalRowFieldConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return (csvMapper, container, row, pos) -> {
			DecimalData decimal = row.getDecimal(pos, precision, scale);
			return convertDecimal(decimal, container);
		};
	}

	private static ArrayElementConverter createDecimalArrayElementConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return (csvMapper, container, array, pos) -> {
			DecimalData decimal = array.getDecimal(pos, precision, scale);
			return convertDecimal(decimal, container);
		};
	}

	private static JsonNode convertDecimal(DecimalData decimal, ContainerNode<?> container) {
		return container.numberNode(decimal.toBigDecimal());
	}

	private static JsonNode convertDate(int days, ContainerNode<?> container) {
		LocalDate date = LocalDate.ofEpochDay(days);
		return container.textNode(ISO_LOCAL_DATE.format(date));
	}

	private static JsonNode convertTime(int millisecond, ContainerNode<?> container) {
		LocalTime time = LocalTime.ofNanoOfDay(millisecond * 1000_000L);
		return container.textNode(ISO_LOCAL_TIME.format(time));
	}

	private static JsonNode convertTimestamp(TimestampData timestamp, ContainerNode<?> container) {
		return container
			.textNode(DATE_TIME_FORMATTER.format(timestamp.toLocalDateTime()));
	}

	private static RowFieldConverter createArrayRowFieldConverter(ArrayType type) {
		LogicalType elementType = type.getElementType();
		final ArrayElementConverter elementConverter = createNullableArrayElementConverter(elementType);
		return (csvMapper, container, row, pos) -> {
			ArrayNode arrayNode = csvMapper.createArrayNode();
			ArrayData arrayData = row.getArray(pos);
			int numElements = arrayData.size();
			for (int i = 0; i < numElements; i++) {
				arrayNode.add(elementConverter.convert(csvMapper, arrayNode, arrayData, i));
			}
			return arrayNode;
		};
	}

	private static RowFieldConverter createRowRowFieldConverter(RowType type) {
		LogicalType[] fieldTypes = type.getFields().stream()
			.map(RowType.RowField::getType)
			.toArray(LogicalType[]::new);
		final RowFieldConverter[] fieldConverters = Arrays.stream(fieldTypes)
			.map(RowDataToCsvConverters::createNullableRowFieldConverter)
			.toArray(RowFieldConverter[]::new);
		final int rowArity = type.getFieldCount();
		return (csvMapper, container, row, pos) -> {
			final RowData value = row.getRow(pos, rowArity);
			// nested rows use array node container
			final ArrayNode arrayNode = csvMapper.createArrayNode();
			for (int i = 0; i < rowArity; i++) {
				arrayNode.add(fieldConverters[i].convert(csvMapper, arrayNode, value, i));
			}
			return arrayNode;
		};
	}

	private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
		.parseCaseInsensitive()
		.append(ISO_LOCAL_DATE)
		.appendLiteral(' ')
		.append(ISO_LOCAL_TIME)
		.toFormatter();
}
