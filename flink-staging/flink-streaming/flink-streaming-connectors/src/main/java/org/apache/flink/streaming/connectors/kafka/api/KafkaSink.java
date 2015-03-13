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

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;

import org.apache.flink.streaming.api.function.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.config.EncoderWrapper;
import org.apache.flink.streaming.connectors.kafka.config.PartitionerWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaDistributePartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.connectors.util.SerializationSchema;

/**
 * Sink that emits its inputs to a Kafka topic.
 * 
 * @param <IN>
 *            Type of the sink input
 */
public class KafkaSink<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private Producer<IN, byte[]> producer;
	private Properties props;
	private String topicId;
	private String brokerAddr;
	private boolean initDone = false;
	private SerializationSchema<IN, byte[]> scheme;
	private KafkaPartitioner<IN> partitioner;

	/**
	 * Creates a KafkaSink for a given topic. The partitioner distributes the
	 * messages between the partitions of the topics.
	 * 
	 * @param topicId
	 *            ID of the Kafka topic.
	 * @param brokerAddr
	 *            Address of the Kafka broker (with port number).
	 * @param serializationSchema
	 *            User defined serialization schema.
	 */
	public KafkaSink(String topicId, String brokerAddr,
			SerializationSchema<IN, byte[]> serializationSchema) {
		this(topicId, brokerAddr, serializationSchema, new KafkaDistributePartitioner<IN>());
	}

	/**
	 * Creates a KafkaSink for a given topic. The sink produces its input into
	 * the topic.
	 * 
	 * @param topicId
	 *            ID of the Kafka topic.
	 * @param brokerAddr
	 *            Address of the Kafka broker (with port number).
	 * @param serializationSchema
	 *            User defined serialization schema.
	 * @param partitioner
	 *            User defined partitioner.
	 */
	public KafkaSink(String topicId, String brokerAddr,
			SerializationSchema<IN, byte[]> serializationSchema, KafkaPartitioner<IN> partitioner) {
		this.topicId = topicId;
		this.brokerAddr = brokerAddr;
		this.scheme = serializationSchema;
		this.partitioner = partitioner;
	}

	/**
	 * Initializes the connection to Kafka.
	 */
	public void initialize() {

		props = new Properties();

		props.put("metadata.broker.list", brokerAddr);
		props.put("request.required.acks", "1");

		props.put("serializer.class", DefaultEncoder.class.getCanonicalName());
		props.put("key.serializer.class", EncoderWrapper.class.getCanonicalName());
		props.put("partitioner.class", PartitionerWrapper.class.getCanonicalName());

		EncoderWrapper<IN> encoderWrapper = new EncoderWrapper<IN>(scheme);
		encoderWrapper.write(props);

		PartitionerWrapper<IN> partitionerWrapper = new PartitionerWrapper<IN>(partitioner);
		partitionerWrapper.write(props);

		ProducerConfig config = new ProducerConfig(props);

		producer = new Producer<IN, byte[]>(config);
		initDone = true;
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Kafka.
	 * 
	 * @param next
	 *            The incoming data
	 */
	@Override
	public void invoke(IN next) {
		if (!initDone) {
			initialize();
		}

		byte[] serialized = scheme.serialize(next);
		producer.send(new KeyedMessage<IN, byte[]>(topicId, next, serialized));
	}

	@Override
	public void close() {
		if (producer != null) {
			producer.close();
		}
	}

	@Override
	public void cancel() {
		close();
	}

}
