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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Creates a Kafka consumer compatible with reading from Kafka 0.8.2.x brokers.
 * The consumer will internally use the old low-level Kafka API, and manually commit offsets
 * partition offsets to ZooKeeper.
 *
 * Once Kafka released the new consumer with Kafka 0.8.3 Flink might use the 0.8.3 consumer API
 * also against Kafka 0.8.2 installations.
 *
 * @param <T> The type of elements produced by this consumer.
 */
public class FlinkKafkaConsumer082<T> extends FlinkKafkaConsumer<T> {

	private static final long serialVersionUID = -8450689820627198228L;

	/**
	 * Creates a new Kafka 0.8.2.x streaming source consumer.
	 * 
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects. 
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer082(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
		super(Collections.singletonList(topic), valueDeserializer, props, OffsetStore.FLINK_ZOOKEEPER, FetcherType.LEGACY_LOW_LEVEL);
	}


	//----- key-value deserializer constructor

	/**
	 * Creates a new Kafka 0.8.2.x streaming source consumer.
	 *
	 * This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs from Kafka.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param deserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer082(String topic, KeyedDeserializationSchema<T> deserializer, Properties props) {
		super(Collections.singletonList(topic), deserializer, props, OffsetStore.FLINK_ZOOKEEPER, FetcherType.LEGACY_LOW_LEVEL);
	}

	//----- topic list constructors


	public FlinkKafkaConsumer082(List<String> topics, DeserializationSchema<T> valueDeserializer, Properties props) {
		super(topics, valueDeserializer, props, OffsetStore.FLINK_ZOOKEEPER, FetcherType.LEGACY_LOW_LEVEL);
	}

	public FlinkKafkaConsumer082(List<String> topics, KeyedDeserializationSchema<T> deserializer, Properties props) {
		super(topics, deserializer, props, OffsetStore.FLINK_ZOOKEEPER, FetcherType.LEGACY_LOW_LEVEL);
	}
}
