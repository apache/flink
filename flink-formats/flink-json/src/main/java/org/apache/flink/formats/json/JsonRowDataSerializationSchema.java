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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Objects;

/**
 * Serialization schema that serializes an object of Flink internal data structure into a JSON bytes.
 *
 * <p>Serializes the input Flink object into a JSON string and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link JsonRowDataDeserializationSchema}.
 */
@Internal
public class JsonRowDataSerializationSchema implements SerializationSchema<RowData> {
	private static final long serialVersionUID = 1L;

	/** RowType to generate the runtime converter. */
	private final RowType rowType;

	/** The converter that converts internal data formats to JsonNode. */
	private final RowDataToJsonConverters.RowDataToJsonConverter runtimeConverter;

	/** Object mapper that is used to create output JSON objects. */
	private final ObjectMapper mapper = new ObjectMapper();

	/** Reusable object node. */
	private transient ObjectNode node;

	/** Timestamp format specification which is used to parse timestamp. */
	private final TimestampFormat timestampFormat;

	public JsonRowDataSerializationSchema(RowType rowType, TimestampFormat timestampFormat) {
		this.rowType = rowType;
		this.timestampFormat = timestampFormat;
		this.runtimeConverter = new RowDataToJsonConverters(timestampFormat).createConverter(rowType);
	}

	@Override
	public byte[] serialize(RowData row) {
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
		JsonRowDataSerializationSchema that = (JsonRowDataSerializationSchema) o;
		return rowType.equals(that.rowType) && timestampFormat.equals(that.timestampFormat);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rowType, timestampFormat);
	}
}
