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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Kafka {@link StreamTableSource} for Kafka 0.8.
 */
@PublicEvolving
public abstract class Kafka08TableSource extends KafkaTableSource {

	// The deserialization schema for the Kafka records
	private final DeserializationSchema<Row> deserializationSchema;

	/**
	 * Creates a Kafka 0.8 {@link StreamTableSource}.
	 *
	 * @param topic                 Kafka topic to consume.
	 * @param properties            Properties for the Kafka consumer.
	 * @param deserializationSchema Deserialization schema to use for Kafka records.
	 * @param typeInfo              Type information describing the result type. The field names are used
	 *                              to parse the JSON file and so are the types.
	 */
	public Kafka08TableSource(
			String topic,
			Properties properties,
			DeserializationSchema<Row> deserializationSchema,
			TableSchema schema,
			TypeInformation<Row> typeInfo) {

		super(topic, properties, schema, typeInfo);

		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public DeserializationSchema<Row> getDeserializationSchema() {
		return this.deserializationSchema;
	}

	@Override
	protected FlinkKafkaConsumerBase<Row> createKafkaConsumer(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
		return new FlinkKafkaConsumer08<>(topic, deserializationSchema, properties);
	}
}
