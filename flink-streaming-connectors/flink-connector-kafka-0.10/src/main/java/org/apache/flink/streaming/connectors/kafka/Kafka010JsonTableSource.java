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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.sources.StreamTableSource;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.util.Properties;

/**
 * Kafka {@link StreamTableSource} for Kafka 0.10.
 */
public class Kafka010JsonTableSource extends Kafka09JsonTableSource {

	/**
	 * Creates a Kafka 0.10 JSON {@link StreamTableSource}.
	 *
	 * @param topic      Kafka topic to consume.
	 * @param properties Properties for the Kafka consumer.
	 * @param fieldNames Row field names.
	 * @param fieldTypes Row field types.
	 */
	public Kafka010JsonTableSource(
			String topic,
			Properties properties,
			String[] fieldNames,
			TypeInformation<?>[] fieldTypes) {

		super(topic, properties, fieldNames, fieldTypes);
	}

	/**
	 * Creates a Kafka 0.10 JSON {@link StreamTableSource}.
	 *
	 * @param topic      Kafka topic to consume.
	 * @param properties Properties for the Kafka consumer.
	 * @param fieldNames Row field names.
	 * @param fieldTypes Row field types.
	 */
	public Kafka010JsonTableSource(
			String topic,
			Properties properties,
			String[] fieldNames,
			Class<?>[] fieldTypes) {

		super(topic, properties, fieldNames, fieldTypes);
	}

	@Override
	FlinkKafkaConsumerBase<Row> getKafkaConsumer(String topic, Properties properties, DeserializationSchema<Row> deserializationSchema) {
		return new FlinkKafkaConsumer010<>(topic, deserializationSchema, properties);
	}
}
