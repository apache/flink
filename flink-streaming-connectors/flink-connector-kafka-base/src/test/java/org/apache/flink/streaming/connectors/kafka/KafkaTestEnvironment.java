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

import kafka.server.KafkaServer;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Abstract class providing a Kafka test environment
 */
public abstract class KafkaTestEnvironment {

	protected static final String KAFKA_HOST = "localhost";

	public abstract void prepare(int numKafkaServers);

	public abstract void shutdown();

	public abstract void deleteTestTopic(String topic);

	public abstract void createTestTopic(String topic, int numberOfPartitions, int replicationFactor);


	public abstract Properties getStandardProperties();

	public abstract String getBrokerConnectionString();

	public abstract String getVersion();

	public abstract List<KafkaServer> getBrokers();

	// -- consumer / producer instances:
	public <T> FlinkKafkaConsumerBase<T> getConsumer(List<String> topics, DeserializationSchema<T> deserializationSchema, Properties props) {
		return getConsumer(topics, new KeyedDeserializationSchemaWrapper<T>(deserializationSchema), props);
	}

	public <T> FlinkKafkaConsumerBase<T> getConsumer(String topic, KeyedDeserializationSchema<T> readSchema, Properties props) {
		return getConsumer(Collections.singletonList(topic), readSchema, props);
	}

	public <T> FlinkKafkaConsumerBase<T> getConsumer(String topic, DeserializationSchema<T> deserializationSchema, Properties props) {
		return getConsumer(Collections.singletonList(topic), deserializationSchema, props);
	}

	public abstract <T> FlinkKafkaConsumerBase<T> getConsumer(List<String> topics, KeyedDeserializationSchema<T> readSchema, Properties props);

	public abstract <T> FlinkKafkaProducerBase<T> getProducer(String topic, KeyedSerializationSchema<T> serSchema, Properties props, KafkaPartitioner<T> partitioner);


	// -- leader failure simulation

	public abstract void restartBroker(int leaderId) throws Exception;

	public abstract int getLeaderToShutDown(String topic) throws Exception;

	public abstract int getBrokerId(KafkaServer server);
}
