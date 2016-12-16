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

import org.apache.flink.types.Row;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.util.Properties;

public class Kafka08JsonTableSinkTest extends KafkaTableSinkTestBase {

	@Override
	protected KafkaTableSink createTableSink(String topic, Properties properties, KafkaPartitioner<Row> partitioner,
			final FlinkKafkaProducerBase<Row> kafkaProducer) {

		return new Kafka08JsonTableSink(topic, properties, partitioner) {
			@Override
			protected FlinkKafkaProducerBase<Row> createKafkaProducer(String topic, Properties properties,
					SerializationSchema<Row> serializationSchema, KafkaPartitioner<Row> partitioner) {
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

