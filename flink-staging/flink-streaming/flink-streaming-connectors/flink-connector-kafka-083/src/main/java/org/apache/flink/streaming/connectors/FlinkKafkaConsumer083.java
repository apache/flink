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

package org.apache.flink.streaming.connectors;

import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import java.util.Properties;

/**
 * Creates a Kafka consumer compatible with reading from Kafka 0.8.3.x brokers.
 * The consumer will use the new Kafka consumer API (early Flink backport version),
 * and lets Kafka handle the offset committing internally.
 * 
 * @param <T> The type of elements produced by this consumer.
 */
public class FlinkKafkaConsumer083<T> extends FlinkKafkaConsumer<T> {

	private static final long serialVersionUID = 1126432820518992927L;

	/**
	 * Creates a new Kafka 0.8.3.x streaming source consumer.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects. 
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer083(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
		super(topic, valueDeserializer, props, OffsetStore.KAFKA, FetcherType.NEW_HIGH_LEVEL);
	}
}
