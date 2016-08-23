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

import org.apache.flink.api.table.Row;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.JsonRowSerializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.util.Properties;

/**
 * Base class for {@link KafkaTableSink} that serializes data in JSON format
 */
public abstract class KafkaJsonTableSink extends KafkaTableSink {

	/**
	 * Creates KafkaJsonTableSink
	 *
	 * @param topic topic in Kafka to which table is written
	 * @param properties properties to connect to Kafka
	 * @param partitioner Kafka partitioner
	 */
	public KafkaJsonTableSink(String topic, Properties properties, KafkaPartitioner<Row> partitioner) {
		super(topic, properties, partitioner);
	}

	@Override
	protected SerializationSchema<Row> createSerializationSchema(String[] fieldNames) {
		return new JsonRowSerializationSchema(fieldNames);
	}
}
