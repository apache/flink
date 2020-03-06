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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.apache.flink.formats.json.TimeFormats.RFC3339_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.TimeFormats.RFC3339_TIME_FORMAT;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Deserialization schema from JSON to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads
 * the specified fields.
 *
 * <p>Failures during deserialization are forwarded as wrapped IOExceptions.
 */
@PublicEvolving
public class JsonRowDeserializationSchema implements DeserializationSchema<Row> {

	private static final long serialVersionUID = -228294330688809195L;

	/** Type information describing the result type. */
	private final RowTypeInfo typeInfo;

	private boolean failOnMissingField;

	/** Object mapper for parsing the JSON. */
	private final ObjectMapper objectMapper = new ObjectMapper();

	private DeserializationRuntimeConverter runtimeConverter;

	/** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
	private final boolean ignoreParseErrors;

	private JsonRowDeserializationSchema(
			TypeInformation<Row> typeInfo,
			boolean failOnMissingField,
			boolean ignoreParseErrors) {
		checkNotNull(typeInfo, "Type information");
		checkArgument(typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
		this.typeInfo = (RowTypeInfo) typeInfo;
		this.failOnMissingField = failOnMissingField;
		this.runtimeConverter = createConverter(this.typeInfo);
		this.ignoreParseErrors = ignoreParseErrors;
	}

	/**
	 * @deprecated Use the provided {@link Builder} instead.
	 */
	@Deprecated
	public JsonRowDeserializationSchema(TypeInformation<Row> typeInfo) {
		this(typeInfo, false, false);
	}

	/**
	 * @deprecated Use the provided {@link Builder} instead.
	 */
	@Deprecated
	public JsonRowDeserializationSchema(String jsonSchema) {
		this(JsonRowSchemaConverter.convert(checkNotNull(jsonSchema)), false, false);
	}

	/**
	 * @deprecated Use the provided {@link Builder} instead.
	 */
	@Deprecated
	public void setFailOnMissingField(boolean failOnMissingField) {
		// TODO make this class immutable once we drop this method
		this.failOnMissingField = failOnMissingField;
		this.runtimeConverter = createConverter(this.typeInfo);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		try {
			final JsonNode root;
			try {
				root = objectMapper.readTree(message);
			} catch (IOException e) {
				return (Row) nullOrThrowParseException("Unable to parse JsonNode from message.", e);
			}
			return (Row) runtimeConverter.convert(objectMapper, root);
		} catch (Throwable t) {
			throw new IOException("Failed to deserialize JSON object.", t);
		}
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	/**
	 * Builder for {@link JsonRowDeserializationSchema}.
	 */
	public static class Builder {

		private final RowTypeInfo typeInfo;
		private boolean failOnMissingField = false;
		private boolean ignoreParseErrors = false;

		/**
		 * Creates a JSON deserialization schema for the given type information.
		 *
		 * @param typeInfo Type information describing the result type. The field names of {@link Row}
		 *                 are used to parse the JSON properties.
		 */
		public Builder(TypeInformation<Row> typeInfo) {
			checkArgument(typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
			this.typeInfo = (RowTypeInfo) typeInfo;
		}

		/**
		 * Creates a JSON deserialization schema for the given JSON schema.
		 *
		 * @param jsonSchema JSON schema describing the result type
		 *
		 * @see <a href="http://json-schema.org/">http://json-schema.org/</a>
		 */
		public Builder(String jsonSchema) {
			this(JsonRowSchemaConverter.convert(checkNotNull(jsonSchema)));
		}

		/**
		 * Configures schema to fail if a JSON field is missing.
		 *
		 * <p>By default, a missing field is ignored and the field is set to null.
		 */
		public Builder failOnMissingField() {
			this.failOnMissingField = true;
			return this;
		}

		/**
		 * Configures schema to fail when parsing json failed.
		 *
		 * <p>By default, an exception will be thrown when parsing json fails.
		 */
		public Builder ignoreParseErrors() {
			this.ignoreParseErrors = true;
			return this;
		}

		public JsonRowDeserializationSchema build() {
			return new JsonRowDeserializationSchema(typeInfo, failOnMissingField, ignoreParseErrors);
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
		final JsonRowDeserializationSchema that = (JsonRowDeserializationSchema) o;
		return Objects.equals(typeInfo, that.typeInfo) &&
			Objects.equals(failOnMissingField, that.failOnMissingField);
	}

	@Override
	public int hashCode() {
		return Objects.hash(typeInfo, failOnMissingField);
	}

	/*
		Runtime converter
	 */

	/**
	 * Runtime converter that maps between {@link JsonNode}s and Java objects.
	 */
	@FunctionalInterface
	private interface DeserializationRuntimeConverter extends Serializable {
		Object convert(ObjectMapper mapper, JsonNode jsonNode);
	}

	private DeserializationRuntimeConverter createConverter(TypeInformation<?> typeInfo) {
		DeserializationRuntimeConverter baseConverter = createConverterForSimpleType(typeInfo)
			.orElseGet(() ->
				createContainerConverter(typeInfo)
					.orElseGet(() -> createFallbackConverter(typeInfo.getTypeClass())));
		return wrapIntoNullableConverter(baseConverter);
	}

	private DeserializationRuntimeConverter wrapIntoNullableConverter(DeserializationRuntimeConverter converter) {
		return (mapper, jsonNode) -> {
			if (jsonNode.isNull()) {
				return null;
			}
			return converter.convert(mapper, jsonNode);
		};
	}

	private Optional<DeserializationRuntimeConverter> createContainerConverter(TypeInformation<?> typeInfo) {
		if (typeInfo instanceof RowTypeInfo) {
			return Optional.of(createRowConverter((RowTypeInfo) typeInfo));
		} else if (typeInfo instanceof ObjectArrayTypeInfo) {
			return Optional.of(createObjectArrayConverter(((ObjectArrayTypeInfo) typeInfo).getComponentInfo()));
		} else if (typeInfo instanceof BasicArrayTypeInfo) {
			return Optional.of(createObjectArrayConverter(((BasicArrayTypeInfo) typeInfo).getComponentInfo()));
		} else if (isPrimitiveByteArray(typeInfo)) {
			return Optional.of(createByteArrayConverter());
		} else if (typeInfo instanceof MapTypeInfo) {
			MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) typeInfo;
			return Optional.of(createMapConverter(mapTypeInfo.getKeyTypeInfo(), mapTypeInfo.getValueTypeInfo()));
		} else {
			return Optional.empty();
		}
	}

	private DeserializationRuntimeConverter createMapConverter(TypeInformation keyType, TypeInformation valueType) {
		DeserializationRuntimeConverter valueConverter = createConverter(valueType);
		DeserializationRuntimeConverter keyConverter = createConverter(keyType);
		return (mapper, jsonNode) -> {
			try {
				Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
				Map<Object, Object> result = new HashMap<>();
				while (fields.hasNext()) {
					Map.Entry<String, JsonNode> entry = fields.next();
					Object key = keyConverter.convert(mapper, TextNode.valueOf(entry.getKey()));
					Object value = valueConverter.convert(mapper, entry.getValue());
					result.put(key, value);
				}
				return result;
			}  catch (RuntimeException e) {
				return nullOrThrowParseException("Unable to deserialize map.", e);
			}
		};
	}

	private DeserializationRuntimeConverter createByteArrayConverter() {
		return (mapper, jsonNode) -> {
			try {
				return jsonNode.binaryValue();
			} catch (IOException e) {
				return nullOrThrowParseException("Unable to deserialize byte array.", e);
			}
		};
	}

	private boolean isPrimitiveByteArray(TypeInformation<?> typeInfo) {
		return typeInfo instanceof PrimitiveArrayTypeInfo &&
			((PrimitiveArrayTypeInfo) typeInfo).getComponentType() == Types.BYTE;
	}

	private DeserializationRuntimeConverter createObjectArrayConverter(TypeInformation elementTypeInfo) {
		DeserializationRuntimeConverter elementConverter = createConverter(elementTypeInfo);
		return assembleArrayConverter(elementTypeInfo, elementConverter);
	}

	private DeserializationRuntimeConverter createRowConverter(RowTypeInfo typeInfo) {
		List<DeserializationRuntimeConverter> fieldConverters = Arrays.stream(typeInfo.getFieldTypes())
			.map(this::createConverter)
			.collect(Collectors.toList());

		return assembleRowConverter(typeInfo.getFieldNames(), fieldConverters);
	}

	private DeserializationRuntimeConverter createFallbackConverter(Class<?> valueType) {
		return (mapper, jsonNode) -> {
			// for types that were specified without JSON schema
			// e.g. POJOs
			try {
				return mapper.treeToValue(jsonNode, valueType);
			} catch (JsonProcessingException e) {
				return nullOrThrowParseException(format("Could not convert node: %s", jsonNode), e);
			}
		};
	}

	private Optional<DeserializationRuntimeConverter> createConverterForSimpleType(TypeInformation<?> simpleTypeInfo) {
		if (simpleTypeInfo == Types.VOID) {
			return Optional.of((mapper, jsonNode) -> null);
		} else if (simpleTypeInfo == Types.BOOLEAN) {
			return Optional.of((mapper, jsonNode) -> jsonNode.asBoolean());
		} else if (simpleTypeInfo == Types.STRING) {
			return Optional.of((mapper, jsonNode) -> jsonNode.asText());
		} else if (simpleTypeInfo == Types.INT) {
			return Optional.of((mapper, jsonNode) -> jsonNode.asInt());
		} else if (simpleTypeInfo == Types.LONG) {
			return Optional.of((mapper, jsonNode) -> jsonNode.asLong());
		} else if (simpleTypeInfo == Types.DOUBLE) {
			return Optional.of((mapper, jsonNode) -> jsonNode.asDouble());
		} else if (simpleTypeInfo == Types.FLOAT) {
			return Optional.of(this::convertToFloat);
		} else if (simpleTypeInfo == Types.SHORT) {
			return Optional.of(this::convertToShot);
		} else if (simpleTypeInfo == Types.BYTE) {
			return Optional.of(this::convertToByte);
		} else if (simpleTypeInfo == Types.BIG_DEC) {
			return Optional.of((mapper, jsonNode) -> jsonNode.decimalValue());
		} else if (simpleTypeInfo == Types.BIG_INT) {
			return Optional.of((mapper, jsonNode) -> jsonNode.bigIntegerValue());
		} else if (simpleTypeInfo == Types.SQL_DATE) {
			return Optional.of(this::convertToDate);
		} else if (simpleTypeInfo == Types.SQL_TIME) {
			return Optional.of(this::convertToTime);
		} else if (simpleTypeInfo == Types.SQL_TIMESTAMP) {
			return Optional.of(this::convertToTimestamp);
		} else if (simpleTypeInfo == Types.LOCAL_DATE) {
			return Optional.of(this::convertToLocalDate);
		} else if (simpleTypeInfo == Types.LOCAL_TIME) {
			return Optional.of(this::convertToLocalTime);
		} else if (simpleTypeInfo == Types.LOCAL_DATE_TIME) {
			return Optional.of(this::convertToLocalDateTime);
		} else {
			return Optional.empty();
		}
	}

	private Float convertToFloat(ObjectMapper mapper, JsonNode jsonNode) {
		try {
			return Float.parseFloat(jsonNode.asText().trim());
		} catch (NumberFormatException e) {
			return (Float) nullOrThrowParseException("Unable to deserialize float.", e);
		}
	}

	private Short convertToShot(ObjectMapper mapper, JsonNode jsonNode) {
		try {
			return Short.parseShort(jsonNode.asText().trim());
		} catch (NumberFormatException e) {
			return (Short) nullOrThrowParseException("Unable to deserialize short.", e);
		}
	}

	private Byte convertToByte(ObjectMapper mapper, JsonNode jsonNode) {
		try {
			return Byte.parseByte(jsonNode.asText().trim());
		} catch (NumberFormatException e) {
			return (Byte) nullOrThrowParseException("Unable to deserialize byte.", e);
		}
	}

	private LocalDate convertToLocalDate(ObjectMapper mapper, JsonNode jsonNode) {
		try {
			return ISO_LOCAL_DATE.parse(jsonNode.asText()).query(TemporalQueries.localDate());
		} catch (RuntimeException e) {
			return (LocalDate) nullOrThrowParseException("Unable to deserialize local date.", e);
		}
	}

	private Date convertToDate(ObjectMapper mapper, JsonNode jsonNode) {
		try {
			return Date.valueOf(convertToLocalDate(mapper, jsonNode));
		} catch (RuntimeException e) {
			return (Date) nullOrThrowParseException("Unable to deserialize date.", e);
		}
	}

	private LocalDateTime convertToLocalDateTime(ObjectMapper mapper, JsonNode jsonNode) {
		// according to RFC 3339 every date-time must have a timezone;
		// until we have full timezone support, we only support UTC;
		// users can parse their time as string as a workaround
		try {
			TemporalAccessor parsedTimestamp = RFC3339_TIMESTAMP_FORMAT.parse(jsonNode.asText());

			ZoneOffset zoneOffset = parsedTimestamp.query(TemporalQueries.offset());

			if (zoneOffset != null && zoneOffset.getTotalSeconds() != 0) {
				throw new IllegalStateException(
					"Invalid timestamp format. Only a timestamp in UTC timezone is supported yet. " +
						"Format: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			}

			LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
			LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

			return LocalDateTime.of(localDate, localTime);
		} catch (RuntimeException e) {
			return (LocalDateTime) nullOrThrowParseException("Unable to deserialize local date time.", e);
		}
	}

	private Timestamp convertToTimestamp(ObjectMapper mapper, JsonNode jsonNode) {
		try {
			return Timestamp.valueOf(convertToLocalDateTime(mapper, jsonNode));
		} catch (RuntimeException e) {
			return (Timestamp) nullOrThrowParseException("Unable to deserialize timestamp.", e);
		}
	}

	private LocalTime convertToLocalTime(ObjectMapper mapper, JsonNode jsonNode) {
		// according to RFC 3339 every full-time must have a timezone;
		// until we have full timezone support, we only support UTC;
		// users can parse their time as string as a workaround

		try {
			TemporalAccessor parsedTime = RFC3339_TIME_FORMAT.parse(jsonNode.asText());

			ZoneOffset zoneOffset = parsedTime.query(TemporalQueries.offset());
			LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

			if (zoneOffset != null && zoneOffset.getTotalSeconds() != 0 || localTime.getNano() != 0) {
				throw new IllegalStateException(
					"Invalid time format. Only a time in UTC timezone without milliseconds is supported yet.");
			}

			return localTime;
		} catch (RuntimeException e) {
			return (LocalTime) nullOrThrowParseException("Unable to deserialize local time.", e);
		}
	}

	private Time convertToTime(ObjectMapper mapper, JsonNode jsonNode) {
		try {
			return Time.valueOf(convertToLocalTime(mapper, jsonNode));
		} catch (RuntimeException e) {
			return (Time) nullOrThrowParseException("Unable to deserialize time.", e);
		}
	}

	private DeserializationRuntimeConverter assembleRowConverter(
		String[] fieldNames,
		List<DeserializationRuntimeConverter> fieldConverters) {
		return (mapper, jsonNode) -> {
			try {
				ObjectNode node = (ObjectNode) jsonNode;
				int arity = fieldNames.length;
				Row row = new Row(arity);
				for (int i = 0; i < arity; i++) {
					String fieldName = fieldNames[i];
					JsonNode field = node.get(fieldName);
					Object convertField = convertField(mapper, fieldConverters.get(i), fieldName, field);
					row.setField(i, convertField);
				}

				return row;
			} catch (RuntimeException e) {
				if (e instanceof IllegalStateException) {
					throw e;
				}
				return nullOrThrowParseException("Unable to deserialize row.", e);
			}
		};
	}

	private Object convertField(
		ObjectMapper mapper,
		DeserializationRuntimeConverter fieldConverter,
		String fieldName,
		JsonNode field) {
		if (field == null) {
			if (failOnMissingField) {
				throw new IllegalStateException(
					"Could not find field with name '" + fieldName + "'.");
			} else {
				return null;
			}
		} else {
			try {
				return fieldConverter.convert(mapper, field);
			} catch (RuntimeException e) {
				return nullOrThrowParseException("Unable to deserialize field.", e);
			}
		}
	}

	private DeserializationRuntimeConverter assembleArrayConverter(
		TypeInformation<?> elementType,
		DeserializationRuntimeConverter elementConverter) {
		final Class<?> elementClass = elementType.getTypeClass();

		return (mapper, jsonNode) -> {
			try {
				final ArrayNode node = (ArrayNode) jsonNode;
				final Object[] array = (Object[]) Array.newInstance(elementClass, node.size());
				for (int i = 0; i < node.size(); i++) {
					final JsonNode innerNode = node.get(i);
					array[i] = elementConverter.convert(mapper, innerNode);
				}

				return array;
			} catch (RuntimeException e) {
				return nullOrThrowParseException("Unable to deserialize array.", e);
			}
		};
	}

	private Object nullOrThrowParseException(String msg, Exception e) {
		if (ignoreParseErrors) {
			return null;
		}
		if (e instanceof ParseErrorException) {
			throw (ParseErrorException) e;
		}
		throw new ParseErrorException(msg, e);
	}

	/**
	 * Exception which refers to parse errors in converters.
	 * */
	class ParseErrorException extends FlinkRuntimeException {
		ParseErrorException(String message, Throwable cause) {
			super(message, cause);
		}
	}
}
