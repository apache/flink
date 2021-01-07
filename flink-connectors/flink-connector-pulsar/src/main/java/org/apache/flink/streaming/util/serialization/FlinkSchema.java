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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.IOException;
import java.io.Serializable;

/**
 * flink format for Pulsar schema.
 *
 * @param <T>
 */
@Internal
public class FlinkSchema<T> implements Schema<T>, Serializable {

	private final SchemaInfo schemaInfo;

	private final SerializationSchema<T> serializer;

	private final DeserializationSchema<T> deserializer;

	public FlinkSchema(
		SchemaInfo schemaInfo, SerializationSchema<T> serializer,
		DeserializationSchema<T> deserializer) {
		this.schemaInfo = schemaInfo;
		this.serializer = serializer;
		this.deserializer = deserializer;
	}

	@Override
	public void validate(byte[] message) {
	}

	@Override
	public byte[] encode(T t) {
		if (serializer == null) {
			throw new UnsupportedOperationException();
		}
		return serializer.serialize(t);
	}

	@Override
	public T decode(byte[] bytes) {
		if (deserializer == null) {
			throw new UnsupportedOperationException();
		}
		try {
			return deserializer.deserialize(bytes);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public SchemaInfo getSchemaInfo() {
		return schemaInfo;
	}

	@Override
	public Schema<T> clone() {
		return this;
	}
}
