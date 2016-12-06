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

package org.apache.flink.streaming.util.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.typeutils.RowTypeInfo;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Deserialization schema from JSON to {@link Row}.
 *
 * <p>Deserializes the <code>byte[]</code> messages as a JSON object and reads
 * the specified fields.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 */
public class JsonRowDeserializationSchema implements DeserializationSchema<Row> {

	/** Field names to parse. Indices match fieldTypes indices. */
	private final String[] fieldNames;

	/** Types to parse fields as. Indices match fieldNames indices. */
	private final TypeInformation<?>[] fieldTypes;

	/** Object mapper for parsing the JSON. */
	private final ObjectMapper objectMapper = new ObjectMapper();

	/** Flag indicating whether to fail on a missing field. */
	private boolean failOnMissingField;

	/**
	 * Creates a JSON deserialization schema for the given fields and type classes.
	 *
	 * @param fieldNames Names of JSON fields to parse.
	 * @param fieldTypes Type classes to parse JSON fields as.
	 */
	public JsonRowDeserializationSchema(String[] fieldNames, Class<?>[] fieldTypes) {
		this.fieldNames = Preconditions.checkNotNull(fieldNames, "Field names");

		this.fieldTypes = new TypeInformation[fieldTypes.length];
		for (int i = 0; i < fieldTypes.length; i++) {
			this.fieldTypes[i] = TypeExtractor.getForClass(fieldTypes[i]);
		}

		Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
				"Number of provided field names and types does not match.");
	}

	/**
	 * Creates a JSON deserialization schema for the given fields and types.
	 *
	 * @param fieldNames Names of JSON fields to parse.
	 * @param fieldTypes Types to parse JSON fields as.
	 */
	public JsonRowDeserializationSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this.fieldNames = Preconditions.checkNotNull(fieldNames, "Field names");
		this.fieldTypes = Preconditions.checkNotNull(fieldTypes, "Field types");

		Preconditions.checkArgument(fieldNames.length == fieldTypes.length,
				"Number of provided field names and types does not match.");
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		try {
			JsonNode root = objectMapper.readTree(message);

			Row row = new Row(fieldNames.length);
			for (int i = 0; i < fieldNames.length; i++) {
				JsonNode node = root.get(fieldNames[i]);

				if (node == null) {
					if (failOnMissingField) {
						throw new IllegalStateException("Failed to find field with name '"
								+ fieldNames[i] + "'.");
					} else {
						row.setField(i, null);
					}
				} else {
					// Read the value as specified type
					Object value = objectMapper.treeToValue(node, fieldTypes[i].getTypeClass());
					row.setField(i, value);
				}
			}

			return row;
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
		return new RowTypeInfo(fieldTypes);
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

}
