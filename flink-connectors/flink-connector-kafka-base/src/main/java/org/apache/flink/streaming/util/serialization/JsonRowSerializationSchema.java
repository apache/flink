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
package org.apache.flink.streaming.util.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.connectors.kafka.internals.TypeUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;


/**
 * Serialization schema that serializes an object into a JSON bytes.
 *
 * <p>Serializes the input {@link Row} object into a JSON string and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using
 * {@link JsonRowDeserializationSchema}.
 */
public class JsonRowSerializationSchema implements SerializationSchema<Row> {
	/** Fields names in the input Row object */
	private final String[] fieldNames;
	/** Object mapper that is used to create output JSON objects */
	private static ObjectMapper mapper = new ObjectMapper();
	private final TypeInformation<?>[] fieldTypes;

	/**
	 * Creates a JSON serialization schema for the given fields and types.
	 *
	 * @param fieldNames Names of JSON fields to parse.
	 */
	public JsonRowSerializationSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this.fieldNames = Preconditions.checkNotNull(fieldNames);
		this.fieldTypes = Preconditions.checkNotNull(fieldTypes);
	}

	public JsonRowSerializationSchema(String[] fieldNames, Class[] fieldTypes) {
		this(fieldNames, TypeUtil.toTypeInfo(fieldTypes));
	}

	@Override
	public byte[] serialize(Row row) {
		ObjectNode objectNode = serializeRow(row, fieldNames, fieldTypes);

		try {
			return mapper.writeValueAsBytes(objectNode);
		} catch (Exception e) {
			throw new RuntimeException("Failed to serialize row", e);
		}
	}

	private ObjectNode serializeRow(Row row, String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		verifyRowArity(row, fieldNames);
		ObjectNode objectNode = mapper.createObjectNode();

		for (int i = 0; i < row.getArity(); i++) {
			TypeInformation<?> fieldType = fieldTypes[i];
			Object rowField = row.getField(i);
			JsonNode node = serializeToNode(i, rowField, fieldType);
			objectNode.set(fieldNames[i], node);
		}
		return objectNode;
	}

	private void verifyRowArity(Row row, String[] fieldNames) {
		if (row.getArity() != fieldNames.length) {
			throw new IllegalStateException(String.format(
				"Number of elements in the row %s is different from number of field names: %d", row, fieldNames.length));
		}
	}

	private JsonNode serializeToNode(int pos, Object rowField, TypeInformation<?> fieldType) {
		if (fieldType instanceof RowTypeInfo) {
			return serializeRowToNode(pos, rowField, (RowTypeInfo) fieldType);
		}
		return mapper.valueToTree(rowField);
	}

	private JsonNode serializeRowToNode(int pos, Object rowField, RowTypeInfo rowTypeInfo) {
		JsonNode node;
		verifyRowField(pos, rowField);
		node = serializeRow((Row) rowField, rowTypeInfo.getFieldNames(), rowTypeInfo.getFieldTypes());
		return node;
	}

	private void verifyRowField(int i, Object rowField) {
		if (! (rowField instanceof Row)) {
			throw new IllegalStateException(
				String.format("Expected Row type in position %d. Instead found: %s", i, rowField));
		}
	}
}
