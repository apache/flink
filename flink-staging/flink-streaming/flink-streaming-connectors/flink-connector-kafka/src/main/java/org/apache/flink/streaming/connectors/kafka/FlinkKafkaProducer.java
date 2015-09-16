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

import com.google.common.base.Preconditions;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.NetUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.List;


/**
 * Flink Sink to produce data into a Kafka topic.
 *
 * Please note that this producer does not have any reliability guarantees.
 *
 * @param <IN> Type of the messages to write into Kafka.
 */
public class FlinkKafkaProducer<IN> extends RichSinkFunction<IN>  {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducer.class);

	private static final long serialVersionUID = 1L;

	/**
	 * Array with the partition ids of the given topicId
	 * The size of this array is the number of partitions
	 */
	private final int[] partitions;

	/**
	 * User defined properties for the Producer
	 */
	private final Properties producerConfig;

	/**
	 * The name of the topic this producer is writing data to
	 */
	private String topicId;

	/**
	 * (Serializable) SerializationSchema for turning objects used with Flink into
	 * byte[] for Kafka.
	 */
	private SerializationSchema<IN, byte[]> schema;

	/**
	 * User-provided partitioner for assigning an object to a Kafka partition.
	 */
	private KafkaPartitioner partitioner;

	// -------------------------------- Runtime fields ------------------------------------------

	/**
	 * KafkaProducer instance.
	 */
	private transient KafkaProducer<byte[], byte[]> producer;


	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
	 * the topic.
	 *
	 * @param brokerList
	 *			Comma separated addresses of the brokers
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined serialization schema.
	 */
	public FlinkKafkaProducer(String brokerList, String topicId, SerializationSchema<IN, byte[]> serializationSchema) {
		this(topicId, serializationSchema, getPropertiesFromBrokerList(brokerList), null);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces its input to
	 * the topic.
	 *
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined serialization schema.
	 * @param producerConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkKafkaProducer(String topicId, SerializationSchema<IN, byte[]> serializationSchema, Properties producerConfig) {
		this(topicId, serializationSchema, producerConfig, null);
	}

	/**
	 * The main constructor for creating a FlinkKafkaProducer.
	 *
	 * @param topicId The topic to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[]
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assining messages to Kafka partitions.
	 */
	public FlinkKafkaProducer(String topicId, SerializationSchema<IN, byte[]> serializationSchema, Properties producerConfig, KafkaPartitioner customPartitioner) {
		Preconditions.checkNotNull(topicId, "TopicID not set");
		Preconditions.checkNotNull(serializationSchema, "serializationSchema not set");
		Preconditions.checkNotNull(producerConfig, "producerConfig not set");
		ClosureCleaner.ensureSerializable(customPartitioner);
		ClosureCleaner.ensureSerializable(serializationSchema);

		this.topicId = topicId;
		this.schema = serializationSchema;
		this.producerConfig = producerConfig;

		// set the producer configuration properties.

		if(!producerConfig.contains(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
		}

		if(!producerConfig.contains(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
		}


		// create a local KafkaProducer to get the list of partitions.
		// this will also ensure locally that all required ProducerConfig values are set.
		{
			KafkaProducer<Void, IN> getPartitionsProd = new KafkaProducer<>(this.producerConfig);
			List<PartitionInfo> partitionsList = getPartitionsProd.partitionsFor(topicId);

			this.partitions = new int[partitionsList.size()];
			for (int i = 0; i < partitions.length; i++) {
				partitions[i] = partitionsList.get(i).partition();
			}
			getPartitionsProd.close();
		}

		if(customPartitioner == null) {
			this.partitioner = new FixedPartitioner();
		} else {
			this.partitioner = customPartitioner;
		}
	}


	/**
	 * Initializes the connection to Kafka.
	 */
	@Override
	public void open(Configuration configuration) {
		producer = new org.apache.kafka.clients.producer.KafkaProducer<>(this.producerConfig);

		partitioner.open(getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks(), partitions);

		LOG.info("Starting FlinkKafkaProducer ({}/{}) to produce into topic {}", getRuntimeContext().getIndexOfThisSubtask(), getRuntimeContext().getNumberOfParallelSubtasks(), topicId);
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

		producer.send(new ProducerRecord<byte[], byte[]>(topicId,
			partitioner.partition(next, partitions.length),
			null,
			serialized),
			new ErrorLoggingCallback(topicId, null, serialized, false));
	}


	@Override
	public void close() {
		if (producer != null) {
			producer.close();
		}
	}


	// ----------------------------------- Utilities --------------------------

	public static Properties getPropertiesFromBrokerList(String brokerList) {
		String[] elements = brokerList.split(",");
		for(String broker: elements) {
			NetUtils.getCorrectHostnamePort(broker);
		}
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		return props;
	}
}
