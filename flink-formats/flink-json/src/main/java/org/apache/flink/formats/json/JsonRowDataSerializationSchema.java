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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.json.TimeFormats.ISO8601_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.TimeFormats.SQL_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.TimeFormats.SQL_TIME_FORMAT;

/**
 * Serialization schema that serializes an object of Flink internal data structure into a JSON bytes.
 *
 * <p>Serializes the input Flink object into a JSON string and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link JsonRowDataDeserializationSchema}.
 */
@Internal
public class JsonRowDataSerializationSchema implements SerializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	/** RowType to generate the runtime converter. */
	private final RowType rowType;

	/** The converter that converts internal data formats to JsonNode. */
	private final SerializationRuntimeConverter runtimeConverter;

	/** Object mapper that is used to create output JSON objects. */
	private final ObjectMapper mapper = new ObjectMapper();

	/** Reusable object node. */
	private transient ObjectNode node;

	/** Timestamp format specification which is used to parse timestamp. */
	private final TimestampFormat timestampFormat;

	public JsonRowDataSerializationSchema(RowType rowType, TimestampFormat timestampFormat) {
		this.rowType = rowType;
		this.timestampFormat = timestampFormat;
		this.runtimeConverter = createConverter(rowType);
	}

	@Override
	public byte[] serialize(RowData row) {
		if (node == null) {
			node = mapper.createObjectNode();
		}

		try {
			runtimeConverter.convert(mapper, node, row);
			return mapper.writeValueAsBytes(node);
		} catch (Throwable t) {
			throw new RuntimeException("Could not serialize row '" + row + "'. " +
				"Make sure that the schema matches the input.", t);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JsonRowDataSerializationSchema that = (JsonRowDataSerializationSchema) o;
		return rowType.equals(that.rowType) && timestampFormat.equals(timestampFormat);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rowType, timestampFormat);
	}

	// --------------------------------------------------------------------------------
	// Runtime Converters
	// --------------------------------------------------------------------------------

	/**
	 * Runtime converter that converts objects of Flink Table & SQL internal data structures
	 * to corresponding {@link JsonNode}s.
	 */
	private interface SerializationRuntimeConverter extends Serializable {
		JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object value);
	}

	/**
	 * Creates a runtime converter which is null safe.
	 */
	private SerializationRuntimeConverter createConverter(LogicalType type) {
		return wrapIntoNullableConverter(createNotNullConverter(type));
	}

	/**
	 * Creates a runtime converter which assuming input object is not null.
	 */
	private SerializationRuntimeConverter createNotNullConverter(LogicalType type) {
		switch (type.getTypeRoot()) {
			case NULL:
				return (mapper, reuse, value) -> mapper.getNodeFactory().nullNode();
			case BOOLEAN:
				return (mapper, reuse, value) -> mapper.getNodeFactory().booleanNode((boolean) value);
			case TINYINT:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((byte) value);
			case SMALLINT:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((short) value);
			case INTEGER:
			case INTERVAL_YEAR_MONTH:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((int) value);
			case BIGINT:
			case INTERVAL_DAY_TIME:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((long) value);
			case FLOAT:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((float) value);
			case DOUBLE:
				return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((double) value);
			case CHAR:
			case VARCHAR:
				// value is BinaryString
				return (mapper, reuse, value) -> mapper.getNodeFactory().textNode(value.toString());
			case BINARY:
			case VARBINARY:
				return (mapper, reuse, value) -> mapper.getNodeFactory().binaryNode((byte[]) value);
			case DATE:
				return createDateConverter();
			case TIME_WITHOUT_TIME_ZONE:
				return createTimeConverter();
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return createTimestampConverter();
			case DECIMAL:
				return createDecimalConverter();
			case ARRAY:
				return createArrayConverter((ArrayType) type);
			case MAP:
			case MULTISET:
				return createMapConverter((MapType) type);
			case ROW:
				return createRowConverter((RowType) type);
			case RAW:
			default:
				throw new UnsupportedOperationException("Not support to parse type: " + type);
		}
	}

	private SerializationRuntimeConverter createDecimalConverter() {
		return (mapper, reuse, value) -> {
			BigDecimal bd = ((DecimalData) value).toBigDecimal();
			return mapper.getNodeFactory().numberNode(bd);
		};
	}

	private SerializationRuntimeConverter createDateConverter() {
		return (mapper, reuse, value) -> {
			int days = (int) value;
			LocalDate date = LocalDate.ofEpochDay(days);
			return mapper.getNodeFactory().textNode(ISO_LOCAL_DATE.format(date));
		};
	}

	private SerializationRuntimeConverter createTimeConverter() {
		return (mapper, reuse, value) -> {
			int millisecond = (int) value;
			LocalTime time = LocalTime.ofSecondOfDay(millisecond / 1000L);
			return mapper.getNodeFactory().textNode(SQL_TIME_FORMAT.format(time));
		};
	}

	private SerializationRuntimeConverter createTimestampConverter() {
		switch (timestampFormat){
			case ISO_8601:
				return (mapper, reuse, value) -> {
					TimestampData timestamp = (TimestampData) value;
					return mapper.getNodeFactory()
						.textNode(ISO8601_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
				};
			case SQL:
				return (mapper, reuse, value) -> {
					TimestampData timestamp = (TimestampData) value;
					return mapper.getNodeFactory()
						.textNode(SQL_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
				};
			default:
				throw new TableException("Unsupported timestamp format. Validator should have checked that.");
		}
	}

	private SerializationRuntimeConverter createArrayConverter(ArrayType type) {
		final LogicalType elementType = type.getElementType();
		final SerializationRuntimeConverter elementConverter = createConverter(elementType);
		return (mapper, reuse, value) -> {
			ArrayNode node;

			// reuse could be a NullNode if last record is null.
			if (reuse == null || reuse.isNull()) {
				node = mapper.createArrayNode();
			} else {
				node = (ArrayNode) reuse;
				node.removeAll();
			}

			ArrayData array = (ArrayData) value;
			int numElements = array.size();
			for (int i = 0; i < numElements; i++) {
				Object element = ArrayData.get(array, i, elementType);
				node.add(elementConverter.convert(mapper, null, element));
			}

			return node;
		};
	}

	private SerializationRuntimeConverter createMapConverter(MapType type) {
		LogicalType keyType = type.getKeyType();
		if (!LogicalTypeChecks.hasFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
			throw new UnsupportedOperationException(
				"JSON format doesn't support non-string as key type of map. " +
					"The map type is: " + type.asSummaryString());
		}
		final LogicalType valueType = type.getValueType();
		final SerializationRuntimeConverter valueConverter = createConverter(valueType);
		return (mapper, reuse, object) -> {
			ObjectNode node;
			// reuse could be a NullNode if last record is null.
			if (reuse == null || reuse.isNull()) {
				node = mapper.createObjectNode();
			} else {
				node = (ObjectNode) reuse;
				node.removeAll();
			}

			MapData map = (MapData) object;
			ArrayData keyArray = map.keyArray();
			ArrayData valueArray = map.valueArray();
			int numElements = map.size();
			for (int i = 0; i < numElements; i++) {
				String fieldName = keyArray.getString(i).toString(); // key must be string
				Object value = ArrayData.get(valueArray, i, valueType);
				node.set(fieldName, valueConverter.convert(mapper, node.get(fieldName), value));
			}

			return node;
		};
	}

	private SerializationRuntimeConverter createRowConverter(RowType type) {
		final String[] fieldNames = type.getFieldNames().toArray(new String[0]);
		final LogicalType[] fieldTypes = type.getFields().stream()
			.map(RowType.RowField::getType)
			.toArray(LogicalType[]::new);
		final SerializationRuntimeConverter[] fieldConverters = Arrays.stream(fieldTypes)
			.map(this::createConverter)
			.toArray(SerializationRuntimeConverter[]::new);
		final int fieldCount = type.getFieldCount();

		return (mapper, reuse, value) -> {
			ObjectNode node;
			// reuse could be a NullNode if last record is null.
			if (reuse == null || reuse.isNull()) {
				node = mapper.createObjectNode();
			} else {
				node = (ObjectNode) reuse;
			}
			RowData row = (RowData) value;
			for (int i = 0; i < fieldCount; i++) {
				String fieldName = fieldNames[i];
				Object field = RowData.get(row, i, fieldTypes[i]);
				node.set(fieldName, fieldConverters[i].convert(mapper, node.get(fieldName), field));
			}
			return node;
		};
	}

	private SerializationRuntimeConverter wrapIntoNullableConverter(
			SerializationRuntimeConverter converter) {
		return (mapper, reuse, object) -> {
			if (object == null) {
				return mapper.getNodeFactory().nullNode();
			}

			return converter.convert(mapper, reuse, object);
		};
	}
}
