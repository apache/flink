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

import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.api.config.PartitionerWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.SerializableKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);

	private Producer<IN, byte[]> producer;
	private Properties userDefinedProperties;
	private String topicId;
	private String brokerList;
	private SerializationSchema<IN, byte[]> schema;
	private SerializableKafkaPartitioner partitioner;
	private Class<? extends SerializableKafkaPartitioner> partitionerClass = null;

	/**
	 * Creates a KafkaSink for a given topic. The sink produces its input to
	 * the topic.
	 *
	 * @param brokerList
	 *			Addresses of the brokers
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param serializationSchema
	 * 		User defined serialization schema.
	 */
	public KafkaSink(String brokerList, String topicId,
			SerializationSchema<IN, byte[]> serializationSchema) {
		this(brokerList, topicId, new Properties(), serializationSchema);
	}

	/**
	 * Creates a KafkaSink for a given topic with custom Producer configuration.
	 * If you use this constructor, the broker should be set with the "metadata.broker.list"
	 * configuration.
	 *
	 * @param brokerList
	 * 		Addresses of the brokers
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param producerConfig
	 * 		Configurations of the Kafka producer
	 * @param serializationSchema
	 * 		User defined serialization schema.
	 */
	public KafkaSink(String brokerList, String topicId, Properties producerConfig,
			SerializationSchema<IN, byte[]> serializationSchema) {
		String[] elements = brokerList.split(",");
		for(String broker: elements) {
			NetUtils.ensureCorrectHostnamePort(broker);
		}
		Preconditions.checkNotNull(topicId, "TopicID not set");

		this.brokerList = brokerList;
		this.topicId = topicId;
		this.schema = serializationSchema;
		this.partitionerClass = null;
		this.userDefinedProperties = producerConfig;
	}

	/**
	 * Creates a KafkaSink for a given topic. The sink produces its input to
	 * the topic.
	 *
	 * @param brokerList
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param serializationSchema
	 * 		User defined serialization schema.
	 * @param partitioner
	 * 		User defined partitioner.
	 */
	public KafkaSink(String brokerList, String topicId,
			SerializationSchema<IN, byte[]> serializationSchema, SerializableKafkaPartitioner partitioner) {
		this(brokerList, topicId, serializationSchema);
		ClosureCleaner.ensureSerializable(partitioner);
		this.partitioner = partitioner;
	}

	public KafkaSink(String brokerList,
			String topicId,
			SerializationSchema<IN, byte[]> serializationSchema,
			Class<? extends SerializableKafkaPartitioner> partitioner) {
		this(brokerList, topicId, serializationSchema);
		this.partitionerClass = partitioner;
	}

	/**
	 * Initializes the connection to Kafka.
	 */
	@Override
	public void open(Configuration configuration) {

		Properties properties = new Properties();

		properties.put("metadata.broker.list", brokerList);
		properties.put("request.required.acks", "-1");
		properties.put("message.send.max.retries", "10");

		properties.put("serializer.class", DefaultEncoder.class.getCanonicalName());

		// this will not be used as the key will not be serialized
		properties.put("key.serializer.class", DefaultEncoder.class.getCanonicalName());

		for (Map.Entry<Object, Object> propertiesEntry : userDefinedProperties.entrySet()) {
			properties.put(propertiesEntry.getKey(), propertiesEntry.getValue());
		}

		if (partitioner != null) {
			properties.put("partitioner.class", PartitionerWrapper.class.getCanonicalName());
			// java serialization will do the rest.
			properties.put(PartitionerWrapper.SERIALIZED_WRAPPER_NAME, partitioner);
		}
		if (partitionerClass != null) {
			properties.put("partitioner.class", partitionerClass);
		}

		ProducerConfig config = new ProducerConfig(properties);

		try {
			producer = new Producer<IN, byte[]>(config);
		} catch (NullPointerException e) {
			throw new RuntimeException("Cannot connect to Kafka broker " + brokerList, e);
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
