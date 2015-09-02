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

package org.apache.flink.streaming.connectors.kafka.api.persistent;

import kafka.consumer.ConsumerConfig;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;


/**
 * Creates a Kafka consumer compatible with reading from Kafka 0.8.1+ consumers.
 *
 * This class is provided as a migration path from the old Flink kafka connectors to the new, updated implemntations.
 *
 * Please use FlinkKafkaConsumer081 and FlinkKafkaConsumer082.
 *
 * @param <T> The type of elements produced by this consumer.
 */
@Deprecated
public class PersistentKafkaSource<T> extends FlinkKafkaConsumer<T> {

	private static final long serialVersionUID = -8450689820627198228L;

	/**
	 * Creates a new Kafka 0.8.2.x streaming source consumer.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param consumerConfig
	 *           The consumer config used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public PersistentKafkaSource(String topic, DeserializationSchema<T> valueDeserializer, ConsumerConfig consumerConfig) {
		super(topic, valueDeserializer, consumerConfig.props().props(), OffsetStore.FLINK_ZOOKEEPER, FetcherType.LEGACY_LOW_LEVEL);
	}
}
