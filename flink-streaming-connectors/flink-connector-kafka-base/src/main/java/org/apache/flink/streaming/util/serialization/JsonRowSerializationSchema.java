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


/**
 * Serialization schema that serializes an object into a JSON bytes.
 * <p>
 */
public class JsonRowSerializationSchema implements SerializationSchema<Row> {
	private final String[] fieldNames;
	private static ObjectMapper mapper = new ObjectMapper();

	public JsonRowSerializationSchema(String[] fieldNames) {
		this.fieldNames = fieldNames;
	}

	@Override
	public byte[] serialize(Row row) {

		ObjectNode objectNode = mapper.createObjectNode();
		for (int i = 0; i < row.getFieldNumber(); i++) {
			JsonNode node = mapper.valueToTree(row.getField(i));
			objectNode.set(fieldNames[i], node);
		}

		try {
			return mapper.writeValueAsBytes(objectNode);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
