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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaDelegatePartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Flink Sink to produce data into a Kafka topic. This producer is compatible with Kafka 0.10.x
 */
public class FlinkKafkaProducer010<T> extends FlinkKafkaProducer09<T> {

	/**
	 * Flag controlling whether we are writing the Flink record's timestamp into Kafka.
	 */
	private boolean writeTimestampToKafka = false;

	// ---------------------- "Constructors" for timestamp writing ------------------

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>This constructor allows writing timestamps to Kafka, it follow approach (b) (see above)
	 *
	 * @param inStream The stream to write to Kafka
	 * @param topicId ID of the Kafka topic.
	 * @param serializationSchema User defined serialization schema supporting key/value messages
	 * @param producerConfig Properties with the producer configuration.
	 *
	 * @deprecated Use {@link #FlinkKafkaProducer010(String, KeyedSerializationSchema, Properties)}
	 * and call {@link #setWriteTimestampToKafka(boolean)}.
	 */
	@Deprecated
	public static <T> FlinkKafkaProducer010Configuration<T> writeToKafkaWithTimestamps(DataStream<T> inStream,
																					String topicId,
																					KeyedSerializationSchema<T> serializationSchema,
																					Properties producerConfig) {
		return writeToKafkaWithTimestamps(inStream, topicId, serializationSchema, producerConfig, new FlinkFixedPartitioner<T>());
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * <p>This constructor allows writing timestamps to Kafka, it follow approach (b) (see above)
	 *
	 * @param inStream The stream to write to Kafka
	 * @param topicId ID of the Kafka topic.
	 * @param serializationSchema User defined (keyless) serialization schema.
	 * @param producerConfig Properties with the producer configuration.
	 *
	 * @deprecated Use {@link #FlinkKafkaProducer010(String, KeyedSerializationSchema, Properties)}
	 * and call {@link #setWriteTimestampToKafka(boolean)}.
	 */
	@Deprecated
	public static <T> FlinkKafkaProducer010Configuration<T> writeToKafkaWithTimestamps(DataStream<T> inStream,
																					String topicId,
																					SerializationSchema<T> serializationSchema,
																					Properties producerConfig) {
		return writeToKafkaWithTimestamps(inStream, topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, new FlinkFixedPartitioner<T>());
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>This constructor allows writing timestamps to Kafka, it follow approach (b) (see above)
	 *
	 *  @param inStream The stream to write to Kafka
	 *  @param topicId The name of the target topic
	 *  @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 *  @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 *  @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions.
	 *
	 * @deprecated Use {@link #FlinkKafkaProducer010(String, KeyedSerializationSchema, Properties, FlinkKafkaPartitioner)}
	 * and call {@link #setWriteTimestampToKafka(boolean)}.
	 */
	@Deprecated
	public static <T> FlinkKafkaProducer010Configuration<T> writeToKafkaWithTimestamps(DataStream<T> inStream,
																					String topicId,
																					KeyedSerializationSchema<T> serializationSchema,
																					Properties producerConfig,
																					FlinkKafkaPartitioner<T> customPartitioner) {

		FlinkKafkaProducer010<T> kafkaProducer = new FlinkKafkaProducer010<>(topicId, serializationSchema, producerConfig, customPartitioner);
		DataStreamSink<T> streamSink = inStream.addSink(kafkaProducer);
		return new FlinkKafkaProducer010Configuration<>(streamSink, inStream, kafkaProducer);

	}

	// ---------------------- Regular constructors------------------

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param brokerList
	 *			Comma separated addresses of the brokers
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined (keyless) serialization schema.
	 */
	public FlinkKafkaProducer010(String brokerList, String topicId, SerializationSchema<T> serializationSchema) {
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), getPropertiesFromBrokerList(brokerList), new FlinkFixedPartitioner<T>());
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined (keyless) serialization schema.
	 * @param producerConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkKafkaProducer010(String topicId, SerializationSchema<T> serializationSchema, Properties producerConfig) {
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, new FlinkFixedPartitioner<T>());
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * @param topicId The topic to write data to
	 * @param serializationSchema A (keyless) serializable serialization schema for turning user objects into a kafka-consumable byte[]
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions (when passing null, we'll use Kafka's partitioner)
	 */
	public FlinkKafkaProducer010(String topicId, SerializationSchema<T> serializationSchema, Properties producerConfig, FlinkKafkaPartitioner<T> customPartitioner) {
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, customPartitioner);
	}

	// ------------------- Key/Value serialization schema constructors ----------------------

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param brokerList
	 *			Comma separated addresses of the brokers
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined serialization schema supporting key/value messages
	 */
	public FlinkKafkaProducer010(String brokerList, String topicId, KeyedSerializationSchema<T> serializationSchema) {
		this(topicId, serializationSchema, getPropertiesFromBrokerList(brokerList), new FlinkFixedPartitioner<T>());
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * @param topicId
	 * 			ID of the Kafka topic.
	 * @param serializationSchema
	 * 			User defined serialization schema supporting key/value messages
	 * @param producerConfig
	 * 			Properties with the producer configuration.
	 */
	public FlinkKafkaProducer010(String topicId, KeyedSerializationSchema<T> serializationSchema, Properties producerConfig) {
		this(topicId, serializationSchema, producerConfig, new FlinkFixedPartitioner<T>());
	}

	/**
	 * Create Kafka producer.
	 *
	 * <p>This constructor does not allow writing timestamps to Kafka, it follow approach (a) (see above)
	 */
	public FlinkKafkaProducer010(String topicId, KeyedSerializationSchema<T> serializationSchema, Properties producerConfig, FlinkKafkaPartitioner<T> customPartitioner) {
		super(topicId, serializationSchema, producerConfig, customPartitioner);
	}

	// ----------------------------- Deprecated constructors / factory methods  ---------------------------

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * <p>This constructor allows writing timestamps to Kafka, it follow approach (b) (see above)
	 *
	 *  @param inStream The stream to write to Kafka
	 *  @param topicId The name of the target topic
	 *  @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 *  @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 *  @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions.
	 *
	 *  @deprecated This is a deprecated since it does not correctly handle partitioning when
	 *              producing to multiple topics. Use
	 *              {@link FlinkKafkaProducer010#FlinkKafkaProducer010(String, SerializationSchema, Properties, FlinkKafkaPartitioner)} instead.
	 */
	@Deprecated
	public static <T> FlinkKafkaProducer010Configuration<T> writeToKafkaWithTimestamps(DataStream<T> inStream,
																					String topicId,
																					KeyedSerializationSchema<T> serializationSchema,
																					Properties producerConfig,
																					KafkaPartitioner<T> customPartitioner) {

		FlinkKafkaProducer010<T> kafkaProducer =
				new FlinkKafkaProducer010<>(topicId, serializationSchema, producerConfig, new FlinkKafkaDelegatePartitioner<>(customPartitioner));
		DataStreamSink<T> streamSink = inStream.addSink(kafkaProducer);
		return new FlinkKafkaProducer010Configuration<T>(streamSink, inStream, kafkaProducer);
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * @param topicId The topic to write data to
	 * @param serializationSchema A (keyless) serializable serialization schema for turning user objects into a kafka-consumable byte[]
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions (when passing null, we'll use Kafka's partitioner)
	 *
	 * @deprecated This is a deprecated since it does not correctly handle partitioning when
	 *             producing to multiple topics. Use
	 *             {@link FlinkKafkaProducer010#FlinkKafkaProducer010(String, SerializationSchema, Properties, FlinkKafkaPartitioner)} instead.
	 */
	@Deprecated
	public FlinkKafkaProducer010(String topicId, SerializationSchema<T> serializationSchema, Properties producerConfig, KafkaPartitioner<T> customPartitioner) {
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, customPartitioner);
	}

	/**
	 * Create Kafka producer.
	 *
	 * <p>This constructor does not allow writing timestamps to Kafka, it follow approach (a) (see above)
	 *
	 * @deprecated This is a deprecated constructor that does not correctly handle partitioning when
	 *             producing to multiple topics. Use
	 *             {@link FlinkKafkaProducer010#FlinkKafkaProducer010(String, SerializationSchema, Properties, FlinkKafkaPartitioner)} instead.
	 */
	@Deprecated
	public FlinkKafkaProducer010(String topicId, KeyedSerializationSchema<T> serializationSchema, Properties producerConfig, KafkaPartitioner<T> customPartitioner) {
		// We create a Kafka 09 producer instance here and only "override" (by intercepting) the
		// invoke call.
		super(topicId, serializationSchema, producerConfig, customPartitioner);
	}

	/**
	 * If set to true, Flink will write the (event time) timestamp attached to each record into Kafka.
	 * Timestamps must be positive for Kafka to accept them.
	 *
	 * @param writeTimestampToKafka Flag indicating if Flink's internal timestamps are written to Kafka.
	 */
	public void setWriteTimestampToKafka(boolean writeTimestampToKafka) {
		this.writeTimestampToKafka = writeTimestampToKafka;
	}


	// ----------------------------- Generic element processing  ---------------------------

	@Override
	public void invoke(T value, Context context) throws Exception {

		checkErroneous();

		byte[] serializedKey = schema.serializeKey(value);
		byte[] serializedValue = schema.serializeValue(value);
		String targetTopic = schema.getTargetTopic(value);
		if (targetTopic == null) {
			targetTopic = defaultTopicId;
		}

		Long timestamp = null;
		if (this.writeTimestampToKafka) {
			timestamp = context.timestamp();
		}

		ProducerRecord<byte[], byte[]> record;
		int[] partitions = topicPartitionsMap.get(targetTopic);
		if (null == partitions) {
			partitions = getPartitionsByTopic(targetTopic, producer);
			topicPartitionsMap.put(targetTopic, partitions);
		}
		if (flinkKafkaPartitioner == null) {
			record = new ProducerRecord<>(targetTopic, null, timestamp, serializedKey, serializedValue);
		} else {
			record = new ProducerRecord<>(targetTopic, flinkKafkaPartitioner.partition(value, serializedKey, serializedValue, targetTopic, partitions), timestamp, serializedKey, serializedValue);
		}
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords++;
			}
		}
		producer.send(record, callback);
	}

	/**
	 * Configuration object returned by the writeToKafkaWithTimestamps() call.
	 *
	 * <p>This is only kept because it's part of the public API. It is not necessary anymore, now
	 * that the {@link SinkFunction} interface provides timestamps.</p>
	 *
	 * <p>To enable the settings, this fake sink must override all the public methods
	 * in {@link DataStreamSink}.</p>
	 */
	public static class FlinkKafkaProducer010Configuration<T> extends DataStreamSink<T> {

		private final FlinkKafkaProducer010 producer;
		private final SinkTransformation<T> transformation;

		private FlinkKafkaProducer010Configuration(
				DataStreamSink<T> originalSink,
				DataStream<T> inputStream,
				FlinkKafkaProducer010<T> producer) {
			//noinspection unchecked
			super(inputStream, originalSink.getTransformation().getOperator());
			this.transformation = originalSink.getTransformation();
			this.producer = producer;
		}

		/**
		 * Defines whether the producer should fail on errors, or only log them.
		 * If this is set to true, then exceptions will be only logged, if set to false,
		 * exceptions will be eventually thrown and cause the streaming program to
		 * fail (and enter recovery).
		 *
		 * @param logFailuresOnly The flag to indicate logging-only on exceptions.
		 */
		public void setLogFailuresOnly(boolean logFailuresOnly) {
			producer.setLogFailuresOnly(logFailuresOnly);
		}

		/**
		 * If set to true, the Flink producer will wait for all outstanding messages in the Kafka buffers
		 * to be acknowledged by the Kafka producer on a checkpoint.
		 * This way, the producer can guarantee that messages in the Kafka buffers are part of the checkpoint.
		 *
		 * @param flush Flag indicating the flushing mode (true = flush on checkpoint)
		 */
		public void setFlushOnCheckpoint(boolean flush) {
			producer.setFlushOnCheckpoint(flush);
		}

		/**
		 * If set to true, Flink will write the (event time) timestamp attached to each record into Kafka.
		 * Timestamps must be positive for Kafka to accept them.
		 *
		 * @param writeTimestampToKafka Flag indicating if Flink's internal timestamps are written to Kafka.
		 */
		public void setWriteTimestampToKafka(boolean writeTimestampToKafka) {
			producer.writeTimestampToKafka = writeTimestampToKafka;
		}

		// *************************************************************************
		//  Override methods to use the transformation in this class.
		// *************************************************************************

		@Override
		public SinkTransformation<T> getTransformation() {
			return transformation;
		}

		@Override
		public DataStreamSink<T> name(String name) {
			transformation.setName(name);
			return this;
		}

		@Override
		public DataStreamSink<T> uid(String uid) {
			transformation.setUid(uid);
			return this;
		}

		@Override
		public DataStreamSink<T> setUidHash(String uidHash) {
			transformation.setUidHash(uidHash);
			return this;
		}

		@Override
		public DataStreamSink<T> setParallelism(int parallelism) {
			transformation.setParallelism(parallelism);
			return this;
		}

		@Override
		public DataStreamSink<T> disableChaining() {
			this.transformation.setChainingStrategy(ChainingStrategy.NEVER);
			return this;
		}

		@Override
		public DataStreamSink<T> slotSharingGroup(String slotSharingGroup) {
			transformation.setSlotSharingGroup(slotSharingGroup);
			return this;
		}
	}
}
