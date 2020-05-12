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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Objects;

/**
 * Deserialization schema from CSV to Flink Table & SQL internal data structures.
 *
 * <p>Deserializes a <code>byte[]</code> message as a {@link JsonNode} and
 * converts it to {@link RowData}.
 *
 * <p>Failure during deserialization are forwarded as wrapped {@link IOException}s.
 */
@Internal
public final class CsvRowDataDeserializationSchema implements DeserializationSchema<RowData> {

	private static final long serialVersionUID = 1L;

	/** Type information describing the result type. */
	private final TypeInformation<RowData> resultTypeInfo;

	/** Runtime instance that performs the actual work. */
	private final DeserializationRuntimeConverter runtimeConverter;

	/** Schema describing the input CSV data. */
	private final CsvSchema csvSchema;

	/** Object reader used to read rows. It is configured by {@link CsvSchema}. */
	private final ObjectReader objectReader;

	/** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
	private final boolean ignoreParseErrors;

	private CsvRowDataDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			CsvSchema csvSchema,
			boolean ignoreParseErrors) {
		this.resultTypeInfo = resultTypeInfo;
		this.runtimeConverter = createRowConverter(rowType, true);
		this.csvSchema = CsvRowSchemaConverter.convert(rowType);
		this.objectReader = new CsvMapper().readerFor(JsonNode.class).with(csvSchema);
		this.ignoreParseErrors = ignoreParseErrors;
	}

	/**
	 * A builder for creating a {@link CsvRowDataDeserializationSchema}.
	 */
	@Internal
	public static class Builder {

		private final RowType rowType;
		private final TypeInformation<RowData> resultTypeInfo;
		private CsvSchema csvSchema;
		private boolean ignoreParseErrors;

		/**
		 * Creates a CSV deserialization schema for the given {@link TypeInformation} with
		 * optional parameters.
		 */
		public Builder(RowType rowType, TypeInformation<RowData> resultTypeInfo) {
			Preconditions.checkNotNull(rowType, "RowType must not be null.");
			Preconditions.checkNotNull(resultTypeInfo, "Result type information must not be null.");
			this.rowType = rowType;
			this.resultTypeInfo = resultTypeInfo;
			this.csvSchema = CsvRowSchemaConverter.convert(rowType);
		}

		public Builder setFieldDelimiter(char delimiter) {
			this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(delimiter).build();
			return this;
		}

		public Builder setAllowComments(boolean allowComments) {
			this.csvSchema = this.csvSchema.rebuild().setAllowComments(allowComments).build();
			return this;
		}

		public Builder setArrayElementDelimiter(String delimiter) {
			Preconditions.checkNotNull(delimiter, "Array element delimiter must not be null.");
			this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
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

		public Builder setNullLiteral(String nullLiteral) {
			Preconditions.checkNotNull(nullLiteral, "Null literal must not be null.");
			this.csvSchema = this.csvSchema.rebuild().setNullValue(nullLiteral).build();
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public CsvRowDataDeserializationSchema build() {
			return new CsvRowDataDeserializationSchema(
				rowType,
				resultTypeInfo,
				csvSchema,
				ignoreParseErrors);
		}
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		try {
			final JsonNode root = objectReader.readValue(message);
			return (RowData) runtimeConverter.convert(root);
		} catch (Throwable t) {
			if (ignoreParseErrors) {
				return null;
			}
			throw new IOException("Failed to deserialize CSV row '" + new String(message) + "'.", t);
		}
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return resultTypeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		final CsvRowDataDeserializationSchema that = (CsvRowDataDeserializationSchema) o;
		final CsvSchema otherSchema = that.csvSchema;

		return resultTypeInfo.equals(that.resultTypeInfo) &&
			ignoreParseErrors == that.ignoreParseErrors &&
			csvSchema.getColumnSeparator() == otherSchema.getColumnSeparator() &&
			csvSchema.allowsComments() == otherSchema.allowsComments() &&
			csvSchema.getArrayElementSeparator().equals(otherSchema.getArrayElementSeparator()) &&
			csvSchema.getQuoteChar() == otherSchema.getQuoteChar() &&
			csvSchema.getEscapeChar() == otherSchema.getEscapeChar() &&
			Arrays.equals(csvSchema.getNullValue(), otherSchema.getNullValue());
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			resultTypeInfo,
			ignoreParseErrors,
			csvSchema.getColumnSeparator(),
			csvSchema.allowsComments(),
			csvSchema.getArrayElementSeparator(),
			csvSchema.getQuoteChar(),
			csvSchema.getEscapeChar(),
			csvSchema.getNullValue());
	}

	// -------------------------------------------------------------------------------------
	// Runtime Converters
	// -------------------------------------------------------------------------------------

	// -------------------------------------------------------------------------------------
	// Runtime Converters
	// -------------------------------------------------------------------------------------

	/**
	 * Runtime converter that converts {@link JsonNode}s into objects of Flink Table & SQL
	 * internal data structures.
	 */
	@FunctionalInterface
	interface DeserializationRuntimeConverter extends Serializable {
		Object convert(JsonNode jsonNode);
	}

	DeserializationRuntimeConverter createRowConverter(RowType rowType, boolean isTopLevel) {
		final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(this::createNullableConverter)
			.toArray(DeserializationRuntimeConverter[]::new);
		final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
		final int arity = fieldNames.length;

		return jsonNode -> {
			int nodeSize = jsonNode.size();

			validateArity(arity, nodeSize, ignoreParseErrors);

			GenericRowData row = new GenericRowData(arity);
			for (int i = 0; i < arity; i++) {
				JsonNode field;
				// Jackson only supports mapping by name in the first level
				if (isTopLevel) {
					field = jsonNode.get(fieldNames[i]);
				} else {
					field = jsonNode.get(i);
				}
				if (field == null) {
					row.setField(i, null);
				} else {
					row.setField(i, fieldConverters[i].convert(field));
				}
			}
			return row;
		};
	}

	/**
	 * Creates a runtime converter which is null safe.
	 */
	private DeserializationRuntimeConverter createNullableConverter(LogicalType type) {
		final DeserializationRuntimeConverter converter = createConverter(type);
		return jsonNode -> {
			if (jsonNode == null || jsonNode.isNull()) {
				return null;
			}
			try {
				return converter.convert(jsonNode);
			} catch (Throwable t) {
				if (!ignoreParseErrors) {
					throw t;
				}
				return null;
			}
		};
	}

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	private DeserializationRuntimeConverter createConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return jsonNode -> null;
			case BOOLEAN:
				return this::convertToBoolean;
			case TINYINT:
				return jsonNode -> Byte.parseByte(jsonNode.asText().trim());
			case SMALLINT:
				return jsonNode -> Short.parseShort(jsonNode.asText().trim());
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return this::convertToInt;
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return this::convertToLong;
			case DATE:
				return this::convertToDate;
			case TIME_WITHOUT_TIME_ZONE:
				return this::convertToTime;
			case TIMESTAMP_WITH_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return this::convertToTimestamp;
			case FLOAT:
				return this::convertToFloat;
			case DOUBLE:
				return this::convertToDouble;
			case CHAR:
			case VARCHAR:
				return this::convertToString;
			case BINARY:
			case VARBINARY:
				return this::convertToBytes;
			case DECIMAL:
				return createDecimalConverter((DecimalType) type);
			case ARRAY:
				return createArrayConverter((ArrayType) type);
			case ROW:
				return createRowConverter((RowType) type, false);
			case MAP:
			case MULTISET:
			case RAW:
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

	private boolean convertToBoolean(JsonNode jsonNode) {
		if (jsonNode.isBoolean()) {
			// avoid redundant toString and parseBoolean, for better performance
			return jsonNode.asBoolean();
		} else {
			return Boolean.parseBoolean(jsonNode.asText().trim());
		}
	}

	private int convertToInt(JsonNode jsonNode) {
		if (jsonNode.canConvertToInt()) {
			// avoid redundant toString and parseInt, for better performance
			return jsonNode.asInt();
		} else {
			return Integer.parseInt(jsonNode.asText().trim());
		}
	}

	private long convertToLong(JsonNode jsonNode) {
		if (jsonNode.canConvertToLong()) {
			// avoid redundant toString and parseLong, for better performance
			return jsonNode.asLong();
		} else {
			return Long.parseLong(jsonNode.asText().trim());
		}
	}

	private double convertToDouble(JsonNode jsonNode) {
		if (jsonNode.isDouble()) {
			// avoid redundant toString and parseDouble, for better performance
			return jsonNode.asDouble();
		} else {
			return Double.parseDouble(jsonNode.asText().trim());
		}
	}

	private float convertToFloat(JsonNode jsonNode) {
		if (jsonNode.isDouble()) {
			// avoid redundant toString and parseDouble, for better performance
			return (float) jsonNode.asDouble();
		} else {
			return Float.parseFloat(jsonNode.asText().trim());
		}
	}

	private int convertToDate(JsonNode jsonNode) {
		// csv currently is using Date.valueOf() to parse date string
		return (int) Date.valueOf(jsonNode.asText()).toLocalDate().toEpochDay();
	}

	private int convertToTime(JsonNode jsonNode) {
		// csv currently is using Time.valueOf() to parse time string
		LocalTime localTime = Time.valueOf(jsonNode.asText()).toLocalTime();
		// TODO: FLINK-17525 support millisecond and nanosecond
		// get number of milliseconds of the day
		return localTime.toSecondOfDay() * 1000;
	}

	private TimestampData convertToTimestamp(JsonNode jsonNode) {
		// csv currently is using Timestamp.valueOf() to parse timestamp string
		Timestamp timestamp = Timestamp.valueOf(jsonNode.asText());
		return TimestampData.fromTimestamp(timestamp);
	}

	private StringData convertToString(JsonNode jsonNode) {
		return StringData.fromString(jsonNode.asText());
	}

	private byte[] convertToBytes(JsonNode jsonNode) {
		try {
			return jsonNode.binaryValue();
		} catch (IOException e) {
			throw new CsvParseException("Unable to deserialize byte array.", e);
		}
	}

	private DeserializationRuntimeConverter createDecimalConverter(DecimalType decimalType) {
		final int precision = decimalType.getPrecision();
		final int scale = decimalType.getScale();
		return jsonNode -> {
			BigDecimal bigDecimal;
			if (jsonNode.isBigDecimal()) {
				bigDecimal = jsonNode.decimalValue();
			} else {
				bigDecimal = new BigDecimal(jsonNode.asText());
			}
			return DecimalData.fromBigDecimal(bigDecimal, precision, scale);
		};
	}

	private DeserializationRuntimeConverter createArrayConverter(ArrayType arrayType) {
		final DeserializationRuntimeConverter elementConverter = createNullableConverter(arrayType.getElementType());
		final Class<?> elementClass = LogicalTypeUtils.toInternalConversionClass(arrayType.getElementType());
		return jsonNode -> {
			final ArrayNode node = (ArrayNode) jsonNode;
			final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
			for (int i = 0; i < node.size(); i++) {
				final JsonNode innerNode = node.get(i);
				array[i] = elementConverter.convert(innerNode);
			}
			return new GenericArrayData(array);
		};
	}

	private static void validateArity(int expected, int actual, boolean ignoreParseErrors) {
		if (expected != actual && !ignoreParseErrors) {
			throw new RuntimeException("Row length mismatch. " + expected +
				" fields expected but was " + actual + ".");
		}
	}

	/**
	 * Exception which refers to parse errors in converters.
	 * */
	private static final class CsvParseException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		public CsvParseException(String message, Throwable cause) {
			super(message, cause);
		}
	}
}
