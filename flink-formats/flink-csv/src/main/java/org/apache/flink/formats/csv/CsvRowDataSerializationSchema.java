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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
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
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ContainerNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

/**
 * Serialization schema that serializes an object of Flink Table & SQL internal data structure
 * into a CSV bytes.
 *
 * <p>Serializes the input row into a {@link JsonNode} and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link CsvRowDataDeserializationSchema}.
 */
@PublicEvolving
public final class CsvRowDataSerializationSchema implements SerializationSchema<RowData> {

	private static final long serialVersionUID = 1L;

	/** Logical row type describing the input CSV data. */
	private final RowType rowType;

	/** Runtime instance that performs the actual work. */
	private final SerializationRuntimeConverter runtimeConverter;

	/** CsvMapper used to write {@link JsonNode} into bytes. */
	private final CsvMapper csvMapper;

	/** Schema describing the input CSV data. */
	private final CsvSchema csvSchema;

	/** Object writer used to write rows. It is configured by {@link CsvSchema}. */
	private final ObjectWriter objectWriter;

	/** Reusable object node. */
	private transient ObjectNode root;

	private CsvRowDataSerializationSchema(
			RowType rowType,
			CsvSchema csvSchema) {
		this.rowType = rowType;
		this.runtimeConverter = createRowConverter(rowType);
		this.csvMapper = new CsvMapper();
		this.csvSchema = csvSchema;
		this.objectWriter = csvMapper.writer(csvSchema);
	}

	/**
	 * A builder for creating a {@link CsvRowDataSerializationSchema}.
	 */
	@PublicEvolving
	public static class Builder {

		private final RowType rowType;
		private CsvSchema csvSchema;

		/**
		 * Creates a {@link CsvRowDataSerializationSchema} expecting the given {@link RowType}.
		 *
		 * @param rowType logical row type used to create schema.
		 */
		public Builder(RowType rowType) {
			Preconditions.checkNotNull(rowType, "Row type must not be null.");

			this.rowType = rowType;
			this.csvSchema = CsvRowSchemaConverter.convert(rowType);
		}

		public Builder setFieldDelimiter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(c).build();
			return this;
		}

		public Builder setLineDelimiter(String delimiter) {
			Preconditions.checkNotNull(delimiter, "Delimiter must not be null.");
			if (!delimiter.equals("\n") && !delimiter.equals("\r") && !delimiter.equals("\r\n") && !delimiter.equals("")) {
				throw new IllegalArgumentException(
					"Unsupported new line delimiter. Only \\n, \\r, \\r\\n, or empty string are supported.");
			}
			this.csvSchema = this.csvSchema.rebuild().setLineSeparator(delimiter).build();
			return this;
		}

		public Builder setArrayElementDelimiter(String delimiter) {
			Preconditions.checkNotNull(delimiter, "Delimiter must not be null.");
			this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
			return this;
		}

		public Builder disableQuoteCharacter() {
			this.csvSchema = this.csvSchema.rebuild().disableQuoteChar().build();
			return this;
		}

		public Builder setQuoteCharacter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
			return this;
		}

		public Builder setEscapeCharacter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
			return this;
		}

		public Builder setNullLiteral(String s) {
			this.csvSchema = this.csvSchema.rebuild().setNullValue(s).build();
			return this;
		}

		public CsvRowDataSerializationSchema build() {
			return new CsvRowDataSerializationSchema(
				rowType,
				csvSchema);
		}
	}

	@Override
	public byte[] serialize(RowData row) {
		if (root == null) {
			root = csvMapper.createObjectNode();
		}
		try {
			runtimeConverter.convert(csvMapper, root, row);
			return objectWriter.writeValueAsBytes(root);
		} catch (Throwable t) {
			throw new RuntimeException("Could not serialize row '" + row + "'.", t);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		if (this == o) {
			return true;
		}
		final CsvRowDataSerializationSchema that = (CsvRowDataSerializationSchema) o;
		final CsvSchema otherSchema = that.csvSchema;

		return rowType.equals(that.rowType) &&
			csvSchema.getColumnSeparator() == otherSchema.getColumnSeparator() &&
			Arrays.equals(csvSchema.getLineSeparator(), otherSchema.getLineSeparator()) &&
			csvSchema.getArrayElementSeparator().equals(otherSchema.getArrayElementSeparator()) &&
			csvSchema.getQuoteChar() == otherSchema.getQuoteChar() &&
			csvSchema.getEscapeChar() == otherSchema.getEscapeChar() &&
			Arrays.equals(csvSchema.getNullValue(), otherSchema.getNullValue());
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			rowType,
			csvSchema.getColumnSeparator(),
			csvSchema.getLineSeparator(),
			csvSchema.getArrayElementSeparator(),
			csvSchema.getQuoteChar(),
			csvSchema.getEscapeChar(),
			csvSchema.getNullValue());
	}

	// --------------------------------------------------------------------------------
	// Runtime Converters
	// --------------------------------------------------------------------------------

	/**
	 * Runtime converter that converts objects of Flink Table & SQL internal data structures
	 * to corresponding {@link JsonNode}s.
	 */
	private interface SerializationRuntimeConverter extends Serializable {
		JsonNode convert(CsvMapper csvMapper, ContainerNode<?> container, RowData row);
	}

	private interface RowFieldConverter extends Serializable {
		JsonNode convert(CsvMapper csvMapper, ContainerNode<?> container, RowData row, int pos);
	}

	private interface ArrayElementConverter extends Serializable {
		JsonNode convert(CsvMapper csvMapper, ContainerNode<?> container, ArrayData array, int pos);
	}

	private SerializationRuntimeConverter createRowConverter(RowType type) {
		LogicalType[] fieldTypes = type.getFields().stream()
			.map(RowType.RowField::getType)
			.toArray(LogicalType[]::new);
		final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
		final RowFieldConverter[] fieldConverters = Arrays.stream(fieldTypes)
			.map(this::createNullableRowFieldConverter)
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

	private RowFieldConverter createNullableRowFieldConverter(LogicalType fieldType) {
		final RowFieldConverter fieldConverter = createRowFieldConverter(fieldType);
		return (csvMapper, container, row, pos) -> {
			if (row.isNullAt(pos)) {
				return container.nullNode();
			}
			return fieldConverter.convert(csvMapper, container, row, pos);
		};
	}

	private RowFieldConverter createRowFieldConverter(LogicalType fieldType) {
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

	private ArrayElementConverter createNullableArrayElementConverter(LogicalType fieldType) {
		final ArrayElementConverter elementConverter = createArrayElementConverter(fieldType);
		return (csvMapper, container, array, pos) -> {
			if (array.isNullAt(pos)) {
				return container.nullNode();
			}
			return elementConverter.convert(csvMapper, container, array, pos);
		};
	}

	private ArrayElementConverter createArrayElementConverter(LogicalType fieldType) {
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

	private RowFieldConverter createDecimalRowFieldConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return (csvMapper, container, row, pos) -> {
			DecimalData decimal = row.getDecimal(pos, precision, scale);
			return convertDecimal(decimal, container);
		};
	}

	private ArrayElementConverter createDecimalArrayElementConverter(DecimalType decimalType) {
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
		LocalTime time = LocalTime.ofSecondOfDay(millisecond / 1000L);
		return container.textNode(ISO_LOCAL_TIME.format(time));
	}

	private static JsonNode convertTimestamp(TimestampData timestamp, ContainerNode<?> container) {
		return container
			.textNode(DATE_TIME_FORMATTER.format(timestamp.toLocalDateTime()));
	}

	private RowFieldConverter createArrayRowFieldConverter(ArrayType type) {
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

	private RowFieldConverter createRowRowFieldConverter(RowType type) {
		LogicalType[] fieldTypes = type.getFields().stream()
			.map(RowType.RowField::getType)
			.toArray(LogicalType[]::new);
		final RowFieldConverter[] fieldConverters = Arrays.stream(fieldTypes)
			.map(this::createNullableRowFieldConverter)
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
