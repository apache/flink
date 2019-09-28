/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.json;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingRuntimeException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
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
 * Serialization schema that serializes an object of Flink types into a JSON bytes.
 *
 * <p>Serializes the input Flink object into a JSON string and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link JsonRowDeserializationSchema}.
 */
@PublicEvolving
public class JsonRowSerializationSchema implements SerializationSchema<Row> {

	private static final long serialVersionUID = -2885556750743978636L;

	/** Type information describing the input type. */
	private final RowTypeInfo typeInfo;

	/** Object mapper that is used to create output JSON objects. */
	private final ObjectMapper mapper = new ObjectMapper();

	private final SerializationRuntimeConverter runtimeConverter;

	/** Reusable object node. */
	private transient ObjectNode node;

	/**
	 * @deprecated Use the provided {@link Builder} instead.
	 */
	@Deprecated
	public JsonRowSerializationSchema(TypeInformation<Row> typeInfo) {
		// TODO make this constructor private in the future
		Preconditions.checkNotNull(typeInfo, "Type information");
		Preconditions.checkArgument(typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
		this.typeInfo = (RowTypeInfo) typeInfo;
		this.runtimeConverter = createConverter(typeInfo);
	}

	/**
	 * Builder for {@link JsonRowSerializationSchema}.
	 */
	@PublicEvolving
	public static class Builder {

		private final RowTypeInfo typeInfo;

		/**
		 * Creates a JSON serialization schema for the given type information.
		 *
		 * @param typeInfo Type information describing the result type. The field names of {@link Row}
		 *                 are used to parse the JSON properties.
		 */
		public Builder(TypeInformation<Row> typeInfo) {
			checkArgument(typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
			this.typeInfo = (RowTypeInfo) typeInfo;
		}

		/**
		 * Creates a JSON serialization schema for the given JSON schema.
		 *
		 * @param jsonSchema JSON schema describing the result type
		 *
		 * @see <a href="http://json-schema.org/">http://json-schema.org/</a>
		 */
		public Builder(String jsonSchema) {
			this(JsonRowSchemaConverter.convert(checkNotNull(jsonSchema)));
		}

		public JsonRowSerializationSchema build() {
			return new JsonRowSerializationSchema(typeInfo);
		}
	}

	@Override
	public byte[] serialize(Row row) {
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
		final JsonRowSerializationSchema that = (JsonRowSerializationSchema) o;
		return Objects.equals(typeInfo, that.typeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(typeInfo);
	}

	/*
		Runtime converters
	 */

	/**
	 * Runtime converter that maps between Java objects and corresponding {@link JsonNode}s.
	 */
	@FunctionalInterface
	private interface SerializationRuntimeConverter extends Serializable {
		JsonNode convert(ObjectMapper mapper, JsonNode reuse, Object object);
	}

	private SerializationRuntimeConverter createConverter(TypeInformation<?> typeInfo) {
		SerializationRuntimeConverter baseConverter = createConverterForSimpleType(typeInfo)
			.orElseGet(() ->
				createContainerConverter(typeInfo)
					.orElseGet(this::createFallbackConverter));
		return wrapIntoNullableConverter(baseConverter);
	}

	private SerializationRuntimeConverter wrapIntoNullableConverter(SerializationRuntimeConverter converter) {
		return (mapper, reuse, object) -> {
			if (object == null) {
				return mapper.getNodeFactory().nullNode();
			}

			return converter.convert(mapper, reuse, object);
		};
	}

	private Optional<SerializationRuntimeConverter> createContainerConverter(TypeInformation<?> typeInfo) {
		if (typeInfo instanceof RowTypeInfo) {
			return Optional.of(createRowConverter((RowTypeInfo) typeInfo));
		} else if (typeInfo instanceof ObjectArrayTypeInfo) {
			return Optional.of(createObjectArrayConverter(((ObjectArrayTypeInfo) typeInfo).getComponentInfo()));
		} else if (typeInfo instanceof BasicArrayTypeInfo) {
			return Optional.of(createObjectArrayConverter(((BasicArrayTypeInfo) typeInfo).getComponentInfo()));
		} else if (isPrimitiveByteArray(typeInfo)) {
			return Optional.of((mapper, reuse, object) -> mapper.getNodeFactory().binaryNode((byte[]) object));
		} else {
			return Optional.empty();
		}
	}

	private boolean isPrimitiveByteArray(TypeInformation<?> typeInfo) {
		return typeInfo instanceof PrimitiveArrayTypeInfo &&
			((PrimitiveArrayTypeInfo) typeInfo).getComponentType() == Types.BYTE;
	}

	private SerializationRuntimeConverter createObjectArrayConverter(TypeInformation elementTypeInfo) {
		SerializationRuntimeConverter elementConverter = createConverter(elementTypeInfo);
		return assembleArrayConverter(elementConverter);
	}

	private SerializationRuntimeConverter createRowConverter(RowTypeInfo typeInfo) {
		List<SerializationRuntimeConverter> fieldConverters = Arrays.stream(typeInfo.getFieldTypes())
			.map(this::createConverter)
			.collect(Collectors.toList());

		return assembleRowConverter(typeInfo.getFieldNames(), fieldConverters);
	}

	private SerializationRuntimeConverter createFallbackConverter() {
		return (mapper, reuse, object) -> {
			// for types that were specified without JSON schema
			// e.g. POJOs
			try {
				return mapper.valueToTree(object);
			} catch (IllegalArgumentException e) {
				throw new WrappingRuntimeException(format("Could not convert object: %s", object), e);
			}
		};
	}

	private Optional<SerializationRuntimeConverter> createConverterForSimpleType(TypeInformation<?> simpleTypeInfo) {
		if (simpleTypeInfo == Types.VOID) {
			return Optional.of((mapper, reuse, object) -> mapper.getNodeFactory().nullNode());
		} else if (simpleTypeInfo == Types.BOOLEAN) {
			return Optional.of((mapper, reuse, object) -> mapper.getNodeFactory().booleanNode((Boolean) object));
		} else if (simpleTypeInfo == Types.STRING) {
			return Optional.of((mapper, reuse, object) -> mapper.getNodeFactory().textNode((String) object));
		} else if (simpleTypeInfo == Types.INT) {
			return Optional.of((mapper, reuse, object) -> mapper.getNodeFactory().numberNode((Integer) object));
		} else if (simpleTypeInfo == Types.LONG) {
			return Optional.of((mapper, reuse, object) -> mapper.getNodeFactory().numberNode((Long) object));
		} else if (simpleTypeInfo == Types.DOUBLE) {
			return Optional.of((mapper, reuse, object) -> mapper.getNodeFactory().numberNode((Double) object));
		} else if (simpleTypeInfo == Types.FLOAT) {
			return Optional.of((mapper, reuse, object) -> mapper.getNodeFactory().numberNode((Float) object));
		} else if (simpleTypeInfo == Types.SHORT) {
			return Optional.of((mapper, reuse, object) -> mapper.getNodeFactory().numberNode((Short) object));
		} else if (simpleTypeInfo == Types.BYTE) {
			return Optional.of((mapper, reuse, object) -> mapper.getNodeFactory().numberNode((Byte) object));
		} else if (simpleTypeInfo == Types.BIG_DEC) {
			return Optional.of(createBigDecimalConverter());
		} else if (simpleTypeInfo == Types.BIG_INT) {
			return Optional.of(createBigIntegerConverter());
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

	private SerializationRuntimeConverter createDateConverter() {
		return (mapper, reuse, object) -> {
			Date date = (Date) object;

			return mapper.getNodeFactory().textNode(ISO_LOCAL_DATE.format(date.toLocalDate()));
		};
	}

	private SerializationRuntimeConverter createTimestampConverter() {
		return (mapper, reuse, object) -> {
			Timestamp timestamp = (Timestamp) object;

			return mapper.getNodeFactory()
				.textNode(RFC3339_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
		};
	}

	private SerializationRuntimeConverter createTimeConverter() {
		return (mapper, reuse, object) -> {
			final Time time = (Time) object;

			JsonNodeFactory nodeFactory = mapper.getNodeFactory();
			return nodeFactory.textNode(RFC3339_TIME_FORMAT.format(time.toLocalTime()));
		};
	}

	private SerializationRuntimeConverter createBigDecimalConverter() {
		return (mapper, reuse, object) -> {
			// convert decimal if necessary
			JsonNodeFactory nodeFactory = mapper.getNodeFactory();
			if (object instanceof BigDecimal) {
				return nodeFactory.numberNode((BigDecimal) object);
			}
			return nodeFactory.numberNode(BigDecimal.valueOf(((Number) object).doubleValue()));
		};
	}

	private SerializationRuntimeConverter createBigIntegerConverter() {
		return (mapper, reuse, object) -> {
			// convert decimal if necessary
			JsonNodeFactory nodeFactory = mapper.getNodeFactory();
			if (object instanceof BigInteger) {
				return nodeFactory.numberNode((BigInteger) object);
			}
			return nodeFactory.numberNode(BigInteger.valueOf(((Number) object).longValue()));
		};
	}

	private SerializationRuntimeConverter assembleRowConverter(
			String[] fieldNames,
			List<SerializationRuntimeConverter> fieldConverters) {
		return (mapper, reuse, object) -> {
			ObjectNode node;

			if (reuse == null) {
				node = mapper.createObjectNode();
			} else {
				node = (ObjectNode) reuse;
			}

			Row row = (Row) object;

			for (int i = 0; i < fieldNames.length; i++) {
				String fieldName = fieldNames[i];
				node.set(fieldName,
					fieldConverters.get(i).convert(mapper, node.get(fieldNames[i]), row.getField(i)));
			}

			return node;
		};
	}

	private SerializationRuntimeConverter assembleArrayConverter(SerializationRuntimeConverter elementConverter) {
		return (mapper, reuse, object) -> {
			ArrayNode node;

			if (reuse == null) {
				node = mapper.createArrayNode();
			} else {
				node = (ArrayNode) reuse;
				node.removeAll();
			}

			Object[] array = (Object[]) object;

			for (Object element : array) {
				node.add(elementConverter.convert(mapper, null, element));
			}

			return node;
		};
	}
}
