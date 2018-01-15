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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Serialization schema that serializes an object into a JSON bytes.
 *
 * <p>Serializes the input {@link Row} object into a JSON string and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using
 * {@link JsonRowDeserializationSchema}.
 */
@PublicEvolving
public class JsonRowSerializationSchema implements SerializationSchema<Row> {

	private static final long serialVersionUID = -2885556750743978636L;

	/** Fields names in the input Row object. */
	private final String[] fieldNames;
	/** Object mapper that is used to create output JSON objects. */
	private static ObjectMapper mapper = new ObjectMapper();

	/**
	 * Creates a JSON serialization schema for the given fields and types.
	 *
	 * @param rowSchema The schema of the rows to encode.
	 */
	public JsonRowSerializationSchema(RowTypeInfo rowSchema) {

		Preconditions.checkNotNull(rowSchema);
		String[] fieldNames = rowSchema.getFieldNames();
		TypeInformation[] fieldTypes = rowSchema.getFieldTypes();

		// check that no field is composite
		for (int i = 0; i < fieldTypes.length; i++) {
			if (fieldTypes[i] instanceof CompositeType) {
				throw new IllegalArgumentException("JsonRowSerializationSchema cannot encode rows with nested schema, " +
					"but field '" + fieldNames[i] + "' is nested: " + fieldTypes[i].toString());
			}
		}

		this.fieldNames = fieldNames;
	}

	@Override
	public byte[] serialize(Row row) {
		if (row.getArity() != fieldNames.length) {
			throw new IllegalStateException(String.format(
				"Number of elements in the row %s is different from number of field names: %d", row, fieldNames.length));
		}

		ObjectNode objectNode = mapper.createObjectNode();

		for (int i = 0; i < row.getArity(); i++) {
			JsonNode node = mapper.valueToTree(row.getField(i));
			objectNode.set(fieldNames[i], node);
		}

		try {
			return mapper.writeValueAsBytes(objectNode);
		} catch (Exception e) {
			throw new RuntimeException("Failed to serialize row", e);
		}
	}
}
