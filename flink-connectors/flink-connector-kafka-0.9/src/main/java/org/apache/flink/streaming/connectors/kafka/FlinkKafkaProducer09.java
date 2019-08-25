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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaDelegatePartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import javax.annotation.Nullable;

import java.util.Properties;

/**
 * Flink Sink to produce data into a Kafka topic. This producer is compatible with Kafka 0.9.
 *
 * <p>Please note that this producer does not have any reliability guarantees.
 *
 * @param <IN> Type of the messages to write into Kafka.
 */
@PublicEvolving
public class FlinkKafkaProducer09<IN> extends FlinkKafkaProducerBase<IN> {

	private static final long serialVersionUID = 1L;

	// ------------------- Key-less serialization schema constructors ----------------------

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer09(String, SerializationSchema, Properties, FlinkKafkaPartitioner)} instead.
	 *
	 * @param brokerList
	 *			Comma separated addresses of the brokers
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined key-less serialization schema.
	 */
	public FlinkKafkaProducer09(String brokerList, String topicId, SerializationSchema<IN> serializationSchema) {
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), getPropertiesFromBrokerList(brokerList), new FlinkFixedPartitioner<IN>());
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer09(String, SerializationSchema, Properties, FlinkKafkaPartitioner)} instead.
	 *
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined key-less serialization schema.
	 * @param producerConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkKafkaProducer09(String topicId, SerializationSchema<IN> serializationSchema, Properties producerConfig) {
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, new FlinkFixedPartitioner<IN>());
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
	 * the topic. It accepts a key-less {@link SerializationSchema} and possibly a custom {@link FlinkKafkaPartitioner}.
	 *
	 * <p>Since a key-less {@link SerializationSchema} is used, all records sent to Kafka will not have an
	 * attached key. Therefore, if a partitioner is also not provided, records will be distributed to Kafka
	 * partitions in a round-robin fashion.
	 *
	 * @param topicId The topic to write data to
	 * @param serializationSchema A key-less serializable serialization schema for turning user objects into a kafka-consumable byte[]
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions.
	 *                          If set to {@code null}, records will be distributed to Kafka partitions
	 *                          in a round-robin fashion.
	 */
	public FlinkKafkaProducer09(
			String topicId,
			SerializationSchema<IN> serializationSchema,
			Properties producerConfig,
			@Nullable FlinkKafkaPartitioner<IN> customPartitioner) {

		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, customPartitioner);

	}

	// ------------------- Key/Value serialization schema constructors ----------------------

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer09(String, KeyedSerializationSchema, Properties, FlinkKafkaPartitioner)} instead.
	 *
	 * @param brokerList
	 *			Comma separated addresses of the brokers
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined serialization schema supporting key/value messages
	 */
	public FlinkKafkaProducer09(String brokerList, String topicId, KeyedSerializationSchema<IN> serializationSchema) {
		this(topicId, serializationSchema, getPropertiesFromBrokerList(brokerList), new FlinkFixedPartitioner<IN>());
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>Using this constructor, the default {@link FlinkFixedPartitioner} will be used as
	 * the partitioner. This default partitioner maps each sink subtask to a single Kafka
	 * partition (i.e. all records received by a sink subtask will end up in the same
	 * Kafka partition).
	 *
	 * <p>To use a custom partitioner, please use
	 * {@link #FlinkKafkaProducer09(String, KeyedSerializationSchema, Properties, FlinkKafkaPartitioner)} instead.
	 *
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined serialization schema supporting key/value messages
	 * @param producerConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkKafkaProducer09(String topicId, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig) {
		this(topicId, serializationSchema, producerConfig, new FlinkFixedPartitioner<IN>());
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
	 * the topic. It accepts a keyed {@link KeyedSerializationSchema} and possibly a custom {@link FlinkKafkaPartitioner}.
	 *
	 * <p>If a partitioner is not provided, written records will be partitioned by the attached key of each
	 * record (as determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If written records do not
	 * have a key (i.e., {@link KeyedSerializationSchema#serializeKey(Object)} returns {@code null}), they
	 * will be distributed to Kafka partitions in a round-robin fashion.
	 *
	 * @param topicId The topic to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions.
	 *                          If set to {@code null}, records will be partitioned by the key of each record
	 *                          (determined by {@link KeyedSerializationSchema#serializeKey(Object)}). If the keys
	 *                          are {@code null}, then records will be distributed to Kafka partitions in a
	 *                          round-robin fashion.
	 */
	public FlinkKafkaProducer09(
			String topicId,
			KeyedSerializationSchema<IN> serializationSchema,
			Properties producerConfig,
			@Nullable FlinkKafkaPartitioner<IN> customPartitioner) {

		super(topicId, serializationSchema, producerConfig, customPartitioner);
	}

	// ------------------- Deprecated constructors ----------------------

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * @param topicId The topic to write data to
	 * @param serializationSchema A (keyless) serializable serialization schema for turning user objects into a kafka-consumable byte[]
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions (when passing null, we'll use Kafka's partitioner)
	 *
	 * @deprecated This is a deprecated constructor that does not correctly handle partitioning when
	 *             producing to multiple topics. Use
	 *             {@link #FlinkKafkaProducer09(String, SerializationSchema, Properties, FlinkKafkaPartitioner)} instead.
	 */
	@Deprecated
	public FlinkKafkaProducer09(String topicId, SerializationSchema<IN> serializationSchema, Properties producerConfig, KafkaPartitioner<IN> customPartitioner) {
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, customPartitioner);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param topicId The topic to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions.
	 *
	 * @deprecated This is a deprecated constructor that does not correctly handle partitioning when
	 *             producing to multiple topics. Use
	 *             {@link #FlinkKafkaProducer09(String, KeyedSerializationSchema, Properties, FlinkKafkaPartitioner)} instead.
	 */
	@Deprecated
	public FlinkKafkaProducer09(String topicId, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig, KafkaPartitioner<IN> customPartitioner) {
		super(topicId, serializationSchema, producerConfig, new FlinkKafkaDelegatePartitioner<>(customPartitioner));
	}

	// ------------------------------------------------------------------

	@Override
	protected void flush() {
		if (this.producer != null) {
			producer.flush();
		}
	}
}
