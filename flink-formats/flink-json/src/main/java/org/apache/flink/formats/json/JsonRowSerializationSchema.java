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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ContainerNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Objects;

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
	private final TypeInformation<Row> typeInfo;

	/** Object mapper that is used to create output JSON objects. */
	private final ObjectMapper mapper = new ObjectMapper();

	/** Formatter for RFC 3339-compliant string representation of a time value (with UTC timezone, without milliseconds). */
	private SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss'Z'");

	/** Formatter for RFC 3339-compliant string representation of a time value (with UTC timezone). */
	private SimpleDateFormat timeFormatWithMillis = new SimpleDateFormat("HH:mm:ss.SSS'Z'");

	/** Formatter for RFC 3339-compliant string representation of a timestamp value (with UTC timezone). */
	private SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	/** Reusable object node. */
	private transient ObjectNode node;

	/**
	 * Creates a JSON serialization schema for the given type information.
	 *
	 * @param typeInfo The field names of {@link Row} are used to map to JSON properties.
	 */
	public JsonRowSerializationSchema(TypeInformation<Row> typeInfo) {
		Preconditions.checkNotNull(typeInfo, "Type information");
		this.typeInfo = typeInfo;
	}

	/**
	 * Creates a JSON serialization schema for the given JSON schema.
	 *
	 * @param jsonSchema JSON schema describing the result type
	 *
	 * @see <a href="http://json-schema.org/">http://json-schema.org/</a>
	 */
	public JsonRowSerializationSchema(String jsonSchema) {
		this(JsonRowSchemaConverter.convert(jsonSchema));
	}

	@Override
	public byte[] serialize(Row row) {
		if (node == null) {
			node = mapper.createObjectNode();
		}

		try {
			convertRow(node, (RowTypeInfo) typeInfo, row);
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

	// --------------------------------------------------------------------------------------------

	private ObjectNode convertRow(ObjectNode reuse, RowTypeInfo info, Row row) {
		if (reuse == null) {
			reuse = mapper.createObjectNode();
		}
		final String[] fieldNames = info.getFieldNames();
		final TypeInformation<?>[] fieldTypes = info.getFieldTypes();

		// validate the row
		if (row.getArity() != fieldNames.length) {
			throw new IllegalStateException(String.format(
				"Number of elements in the row '%s' is different from number of field names: %d", row, fieldNames.length));
		}

		for (int i = 0; i < fieldNames.length; i++) {
			final String name = fieldNames[i];

			final JsonNode fieldConverted = convert(reuse, reuse.get(name), fieldTypes[i], row.getField(i));
			reuse.set(name, fieldConverted);
		}

		return reuse;
	}

	private JsonNode convert(ContainerNode<?> container, JsonNode reuse, TypeInformation<?> info, Object object) {
		if (info == Types.VOID || object == null) {
			return container.nullNode();
		} else if (info == Types.BOOLEAN) {
			return container.booleanNode((Boolean) object);
		} else if (info == Types.STRING) {
			return container.textNode((String) object);
		} else if (info == Types.BIG_DEC) {
			// convert decimal if necessary
			if (object instanceof BigDecimal) {
				return container.numberNode((BigDecimal) object);
			}
			return container.numberNode(BigDecimal.valueOf(((Number) object).doubleValue()));
		} else if (info == Types.BIG_INT) {
			// convert integer if necessary
			if (object instanceof BigInteger) {
				return container.numberNode((BigInteger) object);
			}
			return container.numberNode(BigInteger.valueOf(((Number) object).longValue()));
		} else if (info == Types.SQL_DATE) {
			return container.textNode(object.toString());
		} else if (info == Types.SQL_TIME) {
			final Time time = (Time) object;
			// strip milliseconds if possible
			if (time.getTime() % 1000 > 0) {
				return container.textNode(timeFormatWithMillis.format(time));
			}
			return container.textNode(timeFormat.format(time));
		} else if (info == Types.SQL_TIMESTAMP) {
			return container.textNode(timestampFormat.format((Timestamp) object));
		} else if (info instanceof RowTypeInfo) {
			if (reuse != null && reuse instanceof ObjectNode) {
				return convertRow((ObjectNode) reuse, (RowTypeInfo) info, (Row) object);
			} else {
				return convertRow(null, (RowTypeInfo) info, (Row) object);
			}
		} else if (info instanceof ObjectArrayTypeInfo) {
			if (reuse != null && reuse instanceof ArrayNode) {
				return convertObjectArray((ArrayNode) reuse, ((ObjectArrayTypeInfo) info).getComponentInfo(), (Object[]) object);
			} else {
				return convertObjectArray(null, ((ObjectArrayTypeInfo) info).getComponentInfo(), (Object[]) object);
			}
		} else if (info instanceof BasicArrayTypeInfo) {
			if (reuse != null && reuse instanceof ArrayNode) {
				return convertObjectArray((ArrayNode) reuse, ((BasicArrayTypeInfo) info).getComponentInfo(), (Object[]) object);
			} else {
				return convertObjectArray(null, ((BasicArrayTypeInfo) info).getComponentInfo(), (Object[]) object);
			}
		} else if (info instanceof PrimitiveArrayTypeInfo && ((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
			return container.binaryNode((byte[]) object);
		} else {
			// for types that were specified without JSON schema
			// e.g. POJOs
			try {
				return mapper.valueToTree(object);
			} catch (IllegalArgumentException e) {
				throw new IllegalStateException("Unsupported type information '" + info + "' for object: " + object, e);
			}
		}
	}

	private ArrayNode convertObjectArray(ArrayNode reuse, TypeInformation<?> info, Object[] array) {
		if (reuse == null) {
			reuse = mapper.createArrayNode();
		} else {
			reuse.removeAll();
		}

		for (Object object : array) {
			reuse.add(convert(reuse, null, info, object));
		}
		return reuse;
	}
}
