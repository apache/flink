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

package org.apache.flink.streaming.connectors.kafka.api;

import java.util.Properties;

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.function.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.api.config.PartitionerWrapper;
import org.apache.flink.streaming.connectors.kafka.api.simple.KafkaTopicUtils;
import org.apache.flink.streaming.connectors.kafka.partitioner.SerializableKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.NetUtils;

import com.google.common.base.Preconditions;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;

/**
 * Sink that emits its inputs to a Kafka topic.
 *
 * @param <IN>
 * 		Type of the sink input
 */
public class KafkaSink<IN> extends RichSinkFunction<IN> {

	private static final long serialVersionUID = 1L;

	private Producer<IN, byte[]> producer;
	private Properties props;
	private String topicId;
	private String zookeeperAddress;
	private SerializationSchema<IN, byte[]> schema;
	private SerializableKafkaPartitioner partitioner;
	private Class<? extends SerializableKafkaPartitioner> partitionerClass = null;

	/**
	 * Creates a KafkaSink for a given topic. The partitioner distributes the
	 * messages between the partitions of the topics.
	 *
	 * @param zookeeperAddress
	 * 		Address of the Zookeeper host (with port number).
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param serializationSchema
	 * 		User defined serialization schema.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public KafkaSink(String zookeeperAddress, String topicId,
			SerializationSchema<IN, byte[]> serializationSchema) {
		this(zookeeperAddress, topicId, serializationSchema, (Class) null);
	}

	/**
	 * Creates a KafkaSink for a given topic. The sink produces its input into
	 * the topic.
	 *
	 * @param zookeeperAddress
	 * 		Address of the Zookeeper host (with port number).
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param serializationSchema
	 * 		User defined serialization schema.
	 * @param partitioner
	 * 		User defined partitioner.
	 */
	public KafkaSink(String zookeeperAddress, String topicId,
			SerializationSchema<IN, byte[]> serializationSchema, SerializableKafkaPartitioner partitioner) {
		NetUtils.ensureCorrectHostnamePort(zookeeperAddress);
		Preconditions.checkNotNull(topicId, "TopicID not set");
		ClosureCleaner.ensureSerializable(partitioner);

		this.zookeeperAddress = zookeeperAddress;
		this.topicId = topicId;
		this.schema = serializationSchema;
		this.partitioner = partitioner;
	}

	public KafkaSink(String zookeeperAddress, String topicId,
			SerializationSchema<IN, byte[]> serializationSchema, Class<? extends SerializableKafkaPartitioner> partitioner) {
		NetUtils.ensureCorrectHostnamePort(zookeeperAddress);
		Preconditions.checkNotNull(topicId, "TopicID not set");
		ClosureCleaner.ensureSerializable(partitioner);

		this.zookeeperAddress = zookeeperAddress;
		this.topicId = topicId;
		this.schema = serializationSchema;
		this.partitionerClass = partitioner;
	}

	/**
	 * Initializes the connection to Kafka.
	 */
	@Override
	public void open(Configuration configuration) {

		KafkaTopicUtils kafkaTopicUtils = new KafkaTopicUtils(zookeeperAddress);
		String brokerAddress = kafkaTopicUtils.getLeaderBrokerAddressForTopic(topicId);

		props = new Properties();

		props.put("metadata.broker.list", brokerAddress);
		props.put("request.required.acks", "1");

		props.put("serializer.class", DefaultEncoder.class.getCanonicalName());

		// this will not be used as the key will not be serialized
		props.put("key.serializer.class", DefaultEncoder.class.getCanonicalName());

		if (partitioner != null) {
			props.put("partitioner.class", PartitionerWrapper.class.getCanonicalName());
			// java serialization will do the rest.
			props.put(PartitionerWrapper.SERIALIZED_WRAPPER_NAME, partitioner);
		}
		if (partitionerClass != null) {
			props.put("partitioner.class", partitionerClass);
		}

		ProducerConfig config = new ProducerConfig(props);

		try {
			producer = new Producer<IN, byte[]>(config);
		} catch (NullPointerException e) {
			throw new RuntimeException("Cannot connect to Kafka broker " + brokerAddress, e);
		}
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Kafka.
	 *
	 * @param next
	 * 		The incoming data
	 */
	@Override
	public void invoke(IN next) {
		byte[] serialized = schema.serialize(next);

		// Sending message without serializable key.
		producer.send(new KeyedMessage<IN, byte[]>(topicId, null, next, serialized));
	}

	@Override
	public void close() {
		if (producer != null) {
			producer.close();
		}
	}

}
