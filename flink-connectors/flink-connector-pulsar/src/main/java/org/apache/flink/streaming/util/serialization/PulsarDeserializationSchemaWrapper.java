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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An interface for the deserialization of Pulsar messages.
 */
@PublicEvolving
public class PulsarDeserializationSchemaWrapper<T> implements PulsarDeserializationSchema<T>, PulsarContextAware<T> {

	private final DeserializationSchema<T> deSerializationSchema;

	private final DataType dataType;

	public PulsarDeserializationSchemaWrapper(
		DeserializationSchema<T> deSerializationSchema,
		DataType dataType) {
		this.deSerializationSchema = checkNotNull(deSerializationSchema);
		this.dataType = checkNotNull(dataType);
	}

	@Override
	public Optional<String> getTargetTopic(T element) {
		return Optional.empty();
	}

	@Override
	public Schema<T> getSchema() {
		SchemaInfo si = BytesSchema.of().getSchemaInfo();

		if (deSerializationSchema instanceof SimpleStringSchema) {
			si = (new SchemaInfo())
				.setName("String")
				.setType(SchemaType.STRING)
				.setSchema(new byte[0]);
		} else if (deSerializationSchema instanceof AvroDeserializationSchema) {
			final org.apache.avro.Schema schema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
			byte[] schemaBytes = schema.toString().getBytes(StandardCharsets.UTF_8);
			si.setName("Record");
			si.setSchema(schemaBytes);
			si.setType(SchemaType.AVRO);
		} else if (deSerializationSchema instanceof JsonNodeDeserializationSchema) {
			final org.apache.avro.Schema schema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
			byte[] schemaBytes = schema.toString().getBytes(StandardCharsets.UTF_8);
			si.setName("Record");
			si.setSchema(schemaBytes);
			si.setType(SchemaType.JSON);
		}
		return new FlinkSchema<>(si, null, deSerializationSchema);
	}

	@Override
	public TypeInformation<T> getProducedType() {
		return deSerializationSchema.getProducedType();
	}

	@Override
	public void open(DeserializationSchema.InitializationContext context) throws Exception {
		deSerializationSchema.open(context);
	}

	@Override
	public boolean isEndOfStream(T nextElement) {
		return false;
	}

	@Override
	public T deserialize(Message<T> message) throws IOException {
		return message.getValue();
	}
}
