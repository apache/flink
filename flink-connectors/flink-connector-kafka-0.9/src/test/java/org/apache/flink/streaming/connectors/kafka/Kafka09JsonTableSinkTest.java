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

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Tests for the {@link Kafka09JsonTableSink}.
 */
public class Kafka09JsonTableSinkTest extends KafkaTableSinkTestBase {

	@Override
	protected KafkaTableSink createTableSink(
			String topic,
			Properties properties,
			FlinkKafkaPartitioner<Row> partitioner,
			final FlinkKafkaProducerBase<Row> kafkaProducer) {

		return new Kafka09JsonTableSink(topic, properties, partitioner) {
			@Override
			protected FlinkKafkaProducerBase<Row> createKafkaProducer(
					String topic,
					Properties properties,
					SerializationSchema<Row> serializationSchema,
					FlinkKafkaPartitioner<Row> partitioner) {

				return kafkaProducer;
			}
		};
	}

	@Override
	@SuppressWarnings("unchecked")
	protected SerializationSchema<Row> getSerializationSchema() {
		return new JsonRowSerializationSchema(FIELD_NAMES);
	}
}

