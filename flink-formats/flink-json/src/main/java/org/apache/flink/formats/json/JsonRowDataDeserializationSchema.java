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

package org.apache.flink.formats.json;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.json.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.TimeFormats.SQL_TIME_FORMAT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from JSON to Flink Table/SQL internal data structure {@link RowData}.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads
 * the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
@Internal
public class JsonRowDataDeserializationSchema implements DeserializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	/** Flag indicating whether to fail if a field is missing. */
	private final boolean failOnMissingField;

	/** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
	private final boolean ignoreParseErrors;

	/** TypeInformation of the produced {@link RowData}. **/
	private final TypeInformation<RowData> resultTypeInfo;

	/**
	 * Runtime converter that converts {@link JsonNode}s into
	 * objects of Flink SQL internal data structures. **/
	private final DeserializationRuntimeConverter runtimeConverter;

	/** Object mapper for parsing the JSON. */
	private final ObjectMapper objectMapper = new ObjectMapper();

	/** Timestamp format specification which is used to parse timestamp. */
	private final TimestampFormat timestampFormat;

	public JsonRowDataDeserializationSchema(
			RowType rowType,
			TypeInformation<RowData> resultTypeInfo,
			boolean failOnMissingField,
			boolean ignoreParseErrors,
			TimestampFormat timestampFormat) {
		if (ignoreParseErrors && failOnMissingField) {
			throw new IllegalArgumentException(
				"JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
		}
		this.resultTypeInfo = checkNotNull(resultTypeInfo);
		this.failOnMissingField = failOnMissingField;
		this.ignoreParseErrors = ignoreParseErrors;
		this.runtimeConverter = createRowConverter(checkNotNull(rowType));
		this.timestampFormat = timestampFormat;
	}

	@Override
	public RowData deserialize(byte[] message) throws IOException {
		try {
			final JsonNode root = objectMapper.readTree(message);
			return (RowData) runtimeConverter.convert(root);
		} catch (Throwable t) {
			if (ignoreParseErrors) {
				return null;
			}
			throw new IOException(format("Failed to deserialize JSON '%s'.", new String(message)), t);
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
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JsonRowDataDeserializationSchema that = (JsonRowDataDeserializationSchema) o;
		return failOnMissingField == that.failOnMissingField &&
				ignoreParseErrors == that.ignoreParseErrors &&
				resultTypeInfo.equals(that.resultTypeInfo) &&
				timestampFormat.equals(that.timestampFormat);
	}

	@Override
	public int hashCode() {
		return Objects.hash(failOnMissingField, ignoreParseErrors, resultTypeInfo, timestampFormat);
	}

	// -------------------------------------------------------------------------------------
	// Runtime Converters
	// -------------------------------------------------------------------------------------

	/**
	 * Runtime converter that converts {@link JsonNode}s into objects of Flink Table & SQL
	 * internal data structures.
	 */
	@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(JsonNode jsonNode);
	}

	/**
	 * Creates a runtime converter which is null safe.
	 */
	private DeserializationRuntimeConverter createConverter(LogicalType type) {
		return wrapIntoNullableConverter(createNotNullConverter(type));
	}

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	private DeserializationRuntimeConverter createNotNullConverter(LogicalType type) {
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
			case MAP:
			case MULTISET:
				return createMapConverter((MapType) type);
			case ROW:
				return createRowConverter((RowType) type);
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
		LocalDate date = ISO_LOCAL_DATE.parse(jsonNode.asText()).query(TemporalQueries.localDate());
		return (int) date.toEpochDay();
	}

	private int convertToTime(JsonNode jsonNode) {
		TemporalAccessor parsedTime = SQL_TIME_FORMAT.parse(jsonNode.asText());
		LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

		// get number of milliseconds of the day
		return localTime.toSecondOfDay() * 1000;
	}

	private TimestampData convertToTimestamp(JsonNode jsonNode) {
		TemporalAccessor parsedTimestamp;
		switch (timestampFormat){
			case SQL:
				parsedTimestamp = SQL_TIMESTAMP_FORMAT.parse(jsonNode.asText());
				break;
			case ISO_8601:
				parsedTimestamp = ISO8601_TIMESTAMP_FORMAT.parse(jsonNode.asText());
				break;
			default:
				throw new TableException(String.format("Unsupported timestamp format '%s'. Validator should have checked that.", timestampFormat));
		}
		LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
		LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

		return TimestampData.fromLocalDateTime(LocalDateTime.of(localDate, localTime));
	}

	private StringData convertToString(JsonNode jsonNode) {
		return StringData.fromString(jsonNode.asText());
	}

	private byte[] convertToBytes(JsonNode jsonNode) {
		try {
			return jsonNode.binaryValue();
		} catch (IOException e) {
			throw new JsonParseException("Unable to deserialize byte array.", e);
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
		DeserializationRuntimeConverter elementConverter = createConverter(arrayType.getElementType());
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

	private DeserializationRuntimeConverter createMapConverter(MapType mapType) {
		LogicalType keyType = mapType.getKeyType();
		if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
			throw new UnsupportedOperationException(
				"JSON format doesn't support non-string as key type of map. " +
				"The map type is: " + mapType.asSummaryString());
		}
		final DeserializationRuntimeConverter keyConverter = createConverter(keyType);
		final DeserializationRuntimeConverter valueConverter = createConverter(mapType.getValueType());

		return jsonNode -> {
			Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
			Map<Object, Object> result = new HashMap<>();
			while (fields.hasNext()) {
				Map.Entry<String, JsonNode> entry = fields.next();
				Object key = keyConverter.convert(TextNode.valueOf(entry.getKey()));
				Object value = valueConverter.convert(entry.getValue());
				result.put(key, value);
			}
			return new GenericMapData(result);
		};
	}

	private DeserializationRuntimeConverter createRowConverter(RowType rowType) {
		final DeserializationRuntimeConverter[] fieldConverters = rowType.getFields().stream()
			.map(RowType.RowField::getType)
			.map(this::createConverter)
			.toArray(DeserializationRuntimeConverter[]::new);
		final String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

		return jsonNode -> {
			ObjectNode node = (ObjectNode) jsonNode;
			int arity = fieldNames.length;
			GenericRowData row = new GenericRowData(arity);
			for (int i = 0; i < arity; i++) {
				String fieldName = fieldNames[i];
				JsonNode field = node.get(fieldName);
				Object convertedField = convertField(fieldConverters[i], fieldName, field);
				row.setField(i, convertedField);
			}
			return row;
		};
	}

	private Object convertField(
			DeserializationRuntimeConverter fieldConverter,
			String fieldName,
			JsonNode field) {
		if (field == null) {
			if (failOnMissingField) {
				throw new JsonParseException(
					"Could not find field with name '" + fieldName + "'.");
			} else {
				return null;
			}
		} else {
			return fieldConverter.convert(field);
		}
	}

	private DeserializationRuntimeConverter wrapIntoNullableConverter(
			DeserializationRuntimeConverter converter) {
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
	 * Exception which refers to parse errors in converters.
	 * */
	private static final class JsonParseException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public JsonParseException(String message) {
			super(message);
		}

		public JsonParseException(String message, Throwable cause) {
			super(message, cause);
		}
	}
}
