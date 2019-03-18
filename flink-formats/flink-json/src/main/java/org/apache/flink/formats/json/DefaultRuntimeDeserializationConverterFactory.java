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
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.WrappingRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.Serializable;
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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.util.Spliterators.spliterator;
import static java.util.stream.StreamSupport.stream;
import static org.apache.flink.formats.json.TimeFormats.RFC3339_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.json.TimeFormats.RFC3339_TIME_FORMAT;

/**
 * Default implementation of {@link RuntimeDeserializationConverterFactory} that follows
 * <a href="http://json-schema.org/">http://json-schema.org/</a> format.
 */
@Internal
class DefaultRuntimeDeserializationConverterFactory implements RuntimeDeserializationConverterFactory {

	public DeserializationRuntimeConverter getDeserializationRuntimeConverter(
			RowTypeInfo typeInfo,
			boolean failOnMissingField) {
		return new ConfiguredConverter(typeInfo, failOnMissingField);
	}

	/**
	 * Simple wrapper around factory methods for configuration sharing via function/lambda closure.
	 */
	private static class ConfiguredConverter implements DeserializationRuntimeConverter, Serializable {

		private final boolean failOnMissingField;
		private final DeserializationRuntimeConverter wrappedConverter;

		private ConfiguredConverter(RowTypeInfo typeInfo, boolean failOnMissingField) {
			this.failOnMissingField = failOnMissingField;
			this.wrappedConverter = createConverter(typeInfo);
		}

		@Override
		public Object convert(ObjectMapper mapper, JsonNode jsonNode) {
			return wrappedConverter.convert(mapper, jsonNode);
		}

		// We compare just configuration equality, because the schema equality is verified in the
		// JsonRowDeserializationSchema.
		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ConfiguredConverter that = (ConfiguredConverter) o;
			return failOnMissingField == that.failOnMissingField;
		}

		@Override
		public int hashCode() {
			return Objects.hash(failOnMissingField);
		}

		private DeserializationRuntimeConverter createConverter(TypeInformation<?> typeInfo) {
			DeserializationRuntimeConverter baseConverter = createConverterForSimpleType(typeInfo)
				.orElseGet(() ->
					createContainerConverter(typeInfo)
						.orElseGet(() -> createFallbackConverter(typeInfo.getTypeClass())));
			return wrapIntoNullableConverter(baseConverter);
		}

		private DeserializationRuntimeConverter wrapIntoNullableConverter(DeserializationRuntimeConverter converter) {
			return (DeserializationRuntimeConverter) (mapper, jsonNode) -> {
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
			} else {
				return Optional.empty();
			}
		}

		private DeserializationRuntimeConverter createByteArrayConverter() {
			return (mapper, jsonNode) -> {
				try {
					return jsonNode.binaryValue();
				} catch (IOException e) {
					throw new WrappingRuntimeException("Unable to deserialize byte array.", e);
				}
			};
		}

		private boolean isPrimitiveByteArray(TypeInformation<?> typeInfo) {
			return typeInfo instanceof PrimitiveArrayTypeInfo &&
				((PrimitiveArrayTypeInfo) typeInfo).getComponentType() == Types.BYTE;
		}

		private DeserializationRuntimeConverter createObjectArrayConverter(TypeInformation elementTypeInfo) {
			DeserializationRuntimeConverter elementConverter = createConverter(elementTypeInfo);
			return assembleArrayConverter(elementConverter);
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
					throw new WrappingRuntimeException(format("Could not convert node: %s", jsonNode), e);
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
				return Optional.of((mapper, jsonNode) -> Float.parseFloat(jsonNode.asText().trim()));
			} else if (simpleTypeInfo == Types.SHORT) {
				return Optional.of((mapper, jsonNode) -> Short.parseShort(jsonNode.asText().trim()));
			} else if (simpleTypeInfo == Types.BYTE) {
				return Optional.of((mapper, jsonNode) -> Byte.parseByte(jsonNode.asText().trim()));
			} else if (simpleTypeInfo == Types.BIG_DEC) {
				return Optional.of((mapper, jsonNode) -> jsonNode.decimalValue());
			} else if (simpleTypeInfo == Types.BIG_INT) {
				return Optional.of((mapper, jsonNode) -> jsonNode.bigIntegerValue());
			} else if (simpleTypeInfo == Types.SQL_DATE) {
				return Optional.of(createDateConverter());
			} else if (simpleTypeInfo == Types.SQL_TIME) {
				return Optional.of(createTimeConverter());
			} else if (simpleTypeInfo == Types.SQL_TIMESTAMP) {
				return Optional.of(createTimestampConverter());
			} else {
				return Optional.empty();
			}
		}

		private DeserializationRuntimeConverter createDateConverter() {
			return (mapper, jsonNode) -> Date.valueOf(ISO_LOCAL_DATE.parse(jsonNode.asText())
				.query(TemporalQueries.localDate()));
		}

		private DeserializationRuntimeConverter createTimestampConverter() {
			return (mapper, jsonNode) -> {
				TemporalAccessor parsedTimestamp = RFC3339_TIMESTAMP_FORMAT.parse(jsonNode.asText());

				ZoneOffset zoneOffset = parsedTimestamp.query(TemporalQueries.offset());

				if (zoneOffset != null && zoneOffset.getTotalSeconds() != 0) {
					throw new IllegalStateException(
						"Invalid timestamp format. Only a timestamp in UTC timezone is supported yet. " +
							"Format: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
				}

				LocalTime localTime = parsedTimestamp.query(TemporalQueries.localTime());
				LocalDate localDate = parsedTimestamp.query(TemporalQueries.localDate());

				return Timestamp.valueOf(LocalDateTime.of(localDate, localTime));
			};
		}

		private DeserializationRuntimeConverter createTimeConverter() {
			return (mapper, jsonNode) -> {

				TemporalAccessor parsedTime = RFC3339_TIME_FORMAT.parse(jsonNode.asText());

				ZoneOffset zoneOffset = parsedTime.query(TemporalQueries.offset());
				LocalTime localTime = parsedTime.query(TemporalQueries.localTime());

				if (zoneOffset != null && zoneOffset.getTotalSeconds() != 0 || localTime.getNano() != 0) {
					throw new IllegalStateException(
						"Invalid time format. Only a time in UTC timezone without milliseconds is supported yet.");
				}

				return Time.valueOf(localTime);
			};
		}

		private DeserializationRuntimeConverter assembleRowConverter(
				String[] fieldNames,
				List<DeserializationRuntimeConverter> fieldConverters) {
			return (DeserializationRuntimeConverter) (mapper, jsonNode) -> {
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
				return fieldConverter.convert(mapper, field);
			}
		}

		private DeserializationRuntimeConverter assembleArrayConverter(DeserializationRuntimeConverter elementConverter) {
			return (DeserializationRuntimeConverter) (mapper, jsonNode) -> {
				ArrayNode node = (ArrayNode) jsonNode;

				return stream(spliterator(node.elements(), node.size(), 0), false)
					.map(innerNode -> elementConverter.convert(mapper, innerNode))
					.toArray();
			};
		}
	}
}
