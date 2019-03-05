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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

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
	private final RowTypeInfo typeInfo;

	/** Object mapper that is used to create output JSON objects. */
	private final ObjectMapper mapper = new ObjectMapper();

	private final RuntimeSerializationConverterFactory.SerializationRuntimeConverter runtimeConverter;

	/** Reusable object node. */
	private transient ObjectNode node;

	/**
	 * Creates a JSON serialization schema for the given type information.
	 *
	 * @param typeInfo The field names of {@link Row} are used to map to JSON properties.
	 */
	public JsonRowSerializationSchema(TypeInformation<Row> typeInfo) {
		this(typeInfo, new DefaultRuntimeSerializationConverterFactory());
	}

	/**
	 * Creates a JSON serialization schema for the given JSON schema.
	 *
	 * @param jsonSchema JSON schema describing the result type
	 * @see <a href="http://json-schema.org/">http://json-schema.org/</a>
	 */
	public JsonRowSerializationSchema(String jsonSchema) {
		this(JsonRowSchemaConverter.convert(jsonSchema));
	}

	private JsonRowSerializationSchema(
			TypeInformation<Row> typeInfo,
			RuntimeSerializationConverterFactory converterFactory) {
		Preconditions.checkNotNull(typeInfo, "Type information");
		Preconditions.checkArgument(typeInfo instanceof RowTypeInfo, "Only RowTypeInfo is supported");
		Preconditions.checkNotNull(converterFactory, "Did not provide converter factory.");
		this.typeInfo = (RowTypeInfo) typeInfo;
		this.runtimeConverter = converterFactory.getSerializationRuntimeConverter(this.typeInfo);
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

}
