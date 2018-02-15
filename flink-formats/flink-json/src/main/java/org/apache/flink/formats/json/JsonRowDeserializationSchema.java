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
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.lang.reflect.Array;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Deserialization schema from JSON to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a JSON object and reads
 * the specified fields.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 */
@PublicEvolving
public class JsonRowDeserializationSchema implements DeserializationSchema<Row> {

	private static final long serialVersionUID = -228294330688809195L;

	/** Type information describing the result type. */
	private final TypeInformation<Row> typeInfo;

	/** Object mapper for parsing the JSON. */
	private final ObjectMapper objectMapper = new ObjectMapper();

	/** Flag indicating whether to fail on a missing field. */
	private boolean failOnMissingField;

	/**
	 * Creates a JSON deserialization schema for the given type information.
	 *
	 * @param typeInfo Type information describing the result type. The field names of {@link Row}
	 *                 are used to parse the JSON properties.
	 */
	public JsonRowDeserializationSchema(TypeInformation<Row> typeInfo) {
		Preconditions.checkNotNull(typeInfo, "Type information");
		this.typeInfo = typeInfo;

		if (!(typeInfo instanceof RowTypeInfo)) {
			throw new IllegalArgumentException("Row type information expected.");
		}
	}

	/**
	 * Creates a JSON deserialization schema for the given JSON schema.
	 *
	 * @param jsonSchema JSON schema describing the result type
	 *
	 * @see <a href="http://json-schema.org/">http://json-schema.org/</a>
	 */
	public JsonRowDeserializationSchema(String jsonSchema) {
		this(JsonSchemaConverter.convert(jsonSchema));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Row deserialize(byte[] message) throws IOException {
		try {
			final JsonNode root = objectMapper.readTree(message);
			return convertRow(root, (RowTypeInfo) typeInfo);
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
	 * Configures the failure behaviour if a JSON field is missing.
	 *
	 * <p>By default, a missing field is ignored and the field is set to null.
	 *
	 * @param failOnMissingField Flag indicating whether to fail or not on a missing field.
	 */
	public void setFailOnMissingField(boolean failOnMissingField) {
		this.failOnMissingField = failOnMissingField;
	}

	// --------------------------------------------------------------------------------------------

	private Object convert(JsonNode node, TypeInformation<?> info) {
		if (info == Types.VOID || node.isNull()) {
			return null;
		} else if (info == Types.BOOLEAN) {
			return node.asBoolean();
		} else if (info == Types.STRING) {
			return node.asText();
		} else if (info == Types.BIG_DEC) {
			return node.decimalValue();
		} else if (info == Types.BIG_INT) {
			return node.bigIntegerValue();
		} else if (info == Types.SQL_DATE) {
			return Date.valueOf(node.asText());
		} else if (info == Types.SQL_TIME) {
			// according to RFC 3339 every full-time must have a timezone;
			// until we have full timezone support, we only support UTC;
			// users can parse their time as string as a workaround
			final String time = node.asText();
			if (time.indexOf('Z') < 0 || time.indexOf('.') >= 0) {
				throw new IllegalStateException(
					"Invalid time format. Only a time in UTC timezone without milliseconds is supported yet. " +
						"Format: HH:mm:ss'Z'");
			}
			return Time.valueOf(time.substring(0, time.length() - 1));
		} else if (info == Types.SQL_TIMESTAMP) {
			// according to RFC 3339 every date-time must have a timezone;
			// until we have full timezone support, we only support UTC;
			// users can parse their time as string as a workaround
			final String timestamp = node.asText();
			if (timestamp.indexOf('Z') < 0) {
				throw new IllegalStateException(
					"Invalid timestamp format. Only a timestamp in UTC timezone is supported yet. " +
						"Format: yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			}
			return Timestamp.valueOf(timestamp.substring(0, timestamp.length() - 1).replace('T', ' '));
		} else if (info instanceof RowTypeInfo) {
			return convertRow(node, (RowTypeInfo) info);
		} else if (info instanceof ObjectArrayTypeInfo) {
			return convertObjectArray(node, ((ObjectArrayTypeInfo) info).getComponentInfo());
		} else if (info instanceof BasicArrayTypeInfo) {
			return convertObjectArray(node, ((BasicArrayTypeInfo) info).getComponentInfo());
		} else if (info instanceof PrimitiveArrayTypeInfo &&
				((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
			return convertByteArray(node);
		} else {
			// for types that were specified without JSON schema
			// e.g. POJOs
			try {
				return objectMapper.treeToValue(node, info.getTypeClass());
			} catch (JsonProcessingException e) {
				throw new IllegalStateException("Unsupported type information '" + info + "' for node: " + node);
			}
		}
	}

	private Row convertRow(JsonNode node, RowTypeInfo info) {
		final String[] names = info.getFieldNames();
		final TypeInformation<?>[] types = info.getFieldTypes();

		final Row row = new Row(names.length);
		for (int i = 0; i < names.length; i++) {
			final String name = names[i];
			final JsonNode subNode = node.get(name);
			if (subNode == null) {
				if (failOnMissingField) {
					throw new IllegalStateException(
						"Could not find field with name '" + name + "'.");
				} else {
					row.setField(i, null);
				}
			} else {
				row.setField(i, convert(subNode, types[i]));
			}
		}

		return row;
	}

	private Object convertObjectArray(JsonNode node, TypeInformation<?> elementType) {
		final Object[] array = (Object[]) Array.newInstance(elementType.getTypeClass(), node.size());
		for (int i = 0; i < node.size(); i++) {
			array[i] = convert(node.get(i), elementType);
		}
		return array;
	}

	private Object convertByteArray(JsonNode node) {
		try {
			return node.binaryValue();
		} catch (IOException e) {
			throw new RuntimeException("Unable to deserialize byte array.", e);
		}
	}
}
