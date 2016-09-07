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
import org.apache.flink.api.table.Row;
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

	/**
	 * Creates a JSON serialization schema for the given fields and types.
	 *
	 * @param fieldNames Names of JSON fields to parse.
	 */
	public JsonRowSerializationSchema(String[] fieldNames) {
		this.fieldNames = Preconditions.checkNotNull(fieldNames);
	}

	@Override
	public byte[] serialize(Row row) {
		if (row.productArity() != fieldNames.length) {
			throw new IllegalStateException(String.format(
				"Number of elements in the row %s is different from number of field names: %d", row, fieldNames.length));
		}

		ObjectNode objectNode = mapper.createObjectNode();

		for (int i = 0; i < row.productArity(); i++) {
			JsonNode node = mapper.valueToTree(row.productElement(i));
			objectNode.set(fieldNames[i], node);
		}

		try {
			return mapper.writeValueAsBytes(objectNode);
		} catch (Exception e) {
			throw new RuntimeException("Failed to serialize row", e);
		}
	}
}
