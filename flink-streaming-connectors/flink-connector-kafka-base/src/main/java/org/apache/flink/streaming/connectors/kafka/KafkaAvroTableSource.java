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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.avro.Schema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.sources.StreamTableSource;
import org.apache.flink.streaming.connectors.kafka.internals.TypeUtil;
import org.apache.flink.streaming.util.serialization.AvroRowDeserializationSchema;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.GenericRecordToRowConverter;
import org.apache.flink.streaming.util.serialization.GenericRecordToRowConverterFactory;

import java.util.Properties;

/**
 * A version-agnostic Kafka Avro {@link StreamTableSource}.
 *
 * <p>The version-specific Kafka consumers need to extend this class and
 * override {@link #getKafkaConsumer(String, Properties, DeserializationSchema)}}.
 *
 * <p>The field names are used to parse the Avro file and so are the types.
 */
public abstract class KafkaAvroTableSource extends KafkaTableSource {

	/**
	 * Creates a generic Kafka Avro {@link StreamTableSource}.
	 *
	 * @param topic      Kafka topic to consume.
	 * @param properties Properties for the Kafka consumer.
	 * @param fieldNames Row field names.
	 * @param fieldTypes Row field types.
	 * @param schema     Avro schema.
	 * @param factory    Avro GenericRecord converter factory.
	 */
	KafkaAvroTableSource(
		String topic,
		Properties properties,
		String[] fieldNames,
		Class<?>[] fieldTypes,
		Schema schema,
		GenericRecordToRowConverterFactory factory) {

		super(topic, properties, createDeserializationSchema(fieldNames, fieldTypes, schema, factory),
			fieldNames, fieldTypes);
	}

	/**
	 * Creates a generic Kafka Avro {@link StreamTableSource}.
	 *
	 * @param topic      Kafka topic to consume.
	 * @param properties Properties for the Kafka consumer.
	 * @param fieldNames Row field names.
	 * @param fieldTypes Row field types.
	 * @param schema     Avro schema.
	 * @param factory    Avro GenericRecord converter factory.
	 */
	KafkaAvroTableSource(
		String topic,
		Properties properties,
		String[] fieldNames,
		TypeInformation<?>[] fieldTypes,
		Schema schema,
		GenericRecordToRowConverterFactory factory) {

		super(topic, properties, createDeserializationSchema(fieldNames, fieldTypes, schema, factory),
			fieldNames, fieldTypes);
	}

	private static AvroRowDeserializationSchema createDeserializationSchema(
		String[] fieldNames, TypeInformation<?>[] fieldTypes, Schema schema, GenericRecordToRowConverterFactory converterFactory) {

		GenericRecordToRowConverter converter = converterFactory.create(fieldNames, fieldTypes);
		return new AvroRowDeserializationSchema(converter, schema);
	}

	private static AvroRowDeserializationSchema createDeserializationSchema(
		String[] fieldNames, Class<?>[] fieldTypes, Schema schema, GenericRecordToRowConverterFactory converterFactory) {

		GenericRecordToRowConverter converter = converterFactory.create(fieldNames, TypeUtil.toTypeInfo(fieldTypes));
		return new AvroRowDeserializationSchema(converter, schema);
	}
}
