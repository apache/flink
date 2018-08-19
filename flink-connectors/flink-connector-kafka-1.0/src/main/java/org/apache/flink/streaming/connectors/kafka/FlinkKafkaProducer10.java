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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafka10Producer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

/**
 * Flink Sink to produce data into a Kafka topic. This producer is compatible with Kafka 1.0.x. By default producer
 * will use {@link FlinkKafkaProducer10.Semantic#AT_LEAST_ONCE} semantic.
 * Before using {@link FlinkKafkaProducer10.Semantic#EXACTLY_ONCE} please refer to Flink's
 * Kafka connector documentation.
 */
public class FlinkKafkaProducer10<IN> extends FlinkKafkaProducer011<IN> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducer10.class);

	/**
	 * Creates a FlinkKafka10Producer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param brokerList          Comma separated addresses of the brokers
	 * @param topicId             ID of the Kafka topic.
	 * @param serializationSchema
	 */
	public FlinkKafkaProducer10(String brokerList, String topicId, SerializationSchema<IN> serializationSchema) {
		super(brokerList, topicId, serializationSchema);
	}

	/**
	 * Creates a FlinkKafka10Producer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default
	 * {@link org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer10(String, SerializationSchema, Properties, Optional)} instead.
	 *
	 * @param topicId             ID of the Kafka topic.
	 * @param serializationSchema User defined key-less serialization schema.
	 * @param producerConfig
	 */
	public FlinkKafkaProducer10(String topicId, SerializationSchema<IN> serializationSchema, Properties producerConfig) {
		super(topicId, serializationSchema, producerConfig);
	}

	/**
	 * Creates a FlinkKafka10Producer for a given topic. The sink produces its input to
	 * the topic. It accepts a key-less {@link SerializationSchema} and possibly a custom {@link FlinkKafkaPartitioner}.
	 *
	 * <p>Since a key-less {@link SerializationSchema} is used, all records sent to Kafka will not have an
	 * attached key. Therefore, if a partitioner is also not provided, records will be distributed to Kafka
	 * partitions in a round-robin fashion.
	 *
	 * @param topicId             The topic to write data to
	 * @param serializationSchema A key-less serializable serialization schema for turning user objects into a kafka-consumable byte[]
	 * @param producerConfig      Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner   A serializable partitioner for assigning messages to Kafka partitions.
	 *                            If a partitioner is not provided, records will be distributed to Kafka partitions
	 */
	public FlinkKafkaProducer10(
		String topicId,
		SerializationSchema<IN> serializationSchema,
		Properties producerConfig,
		Optional<FlinkKafkaPartitioner<IN>> customPartitioner) {
		super(topicId, serializationSchema, producerConfig, customPartitioner);
	}

	/**
	 * Creates a FlinkKafka10Producer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default
	 * {@link org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer10(String, KeyedSerializationSchema, Properties, Optional)} instead.
	 *
	 * @param brokerList          Comma separated addresses of the brokers
	 * @param topicId             ID of the Kafka topic.
	 * @param serializationSchema
	 */
	public FlinkKafkaProducer10(
		String brokerList,
		String topicId,
		KeyedSerializationSchema<IN> serializationSchema) {
		super(brokerList, topicId, serializationSchema);
	}

	/**
	 * Creates a FlinkKafka10Producer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default
	 * {@link org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer10(String, KeyedSerializationSchema, Properties, Optional)} instead.
	 *
	 * @param topicId             ID of the Kafka topic.
	 * @param serializationSchema User defined serialization schema supporting key/value messages
	 * @param producerConfig
	 */
	public FlinkKafkaProducer10(
		String topicId,
		KeyedSerializationSchema<IN> serializationSchema,
		Properties producerConfig) {
		super(topicId, serializationSchema, producerConfig);
	}

	/**
	 * Creates a FlinkKafka10Producer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default
	 * {@link org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer10(String, KeyedSerializationSchema, Properties, Optional, Semantic, int)} instead.
	 *
	 * @param topicId             ID of the Kafka topic.
	 * @param serializationSchema User defined serialization schema supporting key/value messages
	 * @param producerConfig      Properties with the producer configuration.
	 * @param semantic            Defines semantic that will be used by this producer (see {@link Semantic}).
	 */
	public FlinkKafkaProducer10(
		String topicId,
		KeyedSerializationSchema<IN> serializationSchema,
		Properties producerConfig,
		Semantic semantic) {
		super(topicId, serializationSchema, producerConfig, semantic);
	}

	/**
	 * Creates a FlinkKafka10Producer for a given topic. The sink produces its input to
	 * the topic. It accepts a keyed {@link KeyedSerializationSchema} and possibly a custom {@link FlinkKafkaPartitioner}.
	 *
	 * <p>If a partitioner is not provided, written records will be partitioned by the attached key of each
	 * record (as determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If written records do not
	 * have a key (i.e., {@link KeyedSerializationSchema#serializeKey(Object)} returns {@code null}), they
	 * will be distributed to Kafka partitions in a round-robin fashion.
	 *
	 * @param defaultTopicId      The default topic to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 * @param producerConfig      Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner   A serializable partitioner for assigning messages to Kafka partitions.
	 *                            If a partitioner is not provided, records will be partitioned by the key of each record
	 *                            (determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If the keys
	 *                            are {@code null}, then records will be distributed to Kafka partitions in a
	 */
	public FlinkKafkaProducer10(
		String defaultTopicId,
		KeyedSerializationSchema<IN> serializationSchema,
		Properties producerConfig,
		Optional<FlinkKafkaPartitioner<IN>> customPartitioner) {
		super(defaultTopicId, serializationSchema, producerConfig, customPartitioner);
	}

	/**
	 * Creates a FlinkKafka10Producer for a given topic. The sink produces its input to
	 * the topic. It accepts a keyed {@link KeyedSerializationSchema} and possibly a custom {@link FlinkKafkaPartitioner}.
	 *
	 * <p>If a partitioner is not provided, written records will be partitioned by the attached key of each
	 * record (as determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If written records do not
	 * have a key (i.e., {@link KeyedSerializationSchema#serializeKey(Object)} returns {@code null}), they
	 * will be distributed to Kafka partitions in a round-robin fashion.
	 *
	 * @param defaultTopicId         The default topic to write data to
	 * @param serializationSchema    A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 * @param producerConfig         Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner      A serializable partitioner for assigning messages to Kafka partitions.
	 *                               If a partitioner is not provided, records will be partitioned by the key of each record
	 *                               (determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If the keys
	 *                               are {@code null}, then records will be distributed to Kafka partitions in a
	 *                               round-robin fashion.
	 * @param semantic               Defines semantic that will be used by this producer (see {@link Semantic}).
	 * @param kafkaProducersPoolSize Overwrite default KafkaProducers pool size (see {@link Semantic#EXACTLY_ONCE}).
	 */
	public FlinkKafkaProducer10(
		String defaultTopicId,
		KeyedSerializationSchema<IN> serializationSchema,
		Properties producerConfig,
		Optional<FlinkKafkaPartitioner<IN>> customPartitioner,
		Semantic semantic,
		int kafkaProducersPoolSize) {
		super(defaultTopicId, serializationSchema, producerConfig, customPartitioner, semantic, kafkaProducersPoolSize);
	}

	@Override
	protected FlinkKafka10Producer createProducer() {
		return new FlinkKafka10Producer(this.producerConfig);
	}

}
