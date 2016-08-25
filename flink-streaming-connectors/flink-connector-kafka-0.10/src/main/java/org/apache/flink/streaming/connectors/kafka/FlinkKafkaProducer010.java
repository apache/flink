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

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase.getPropertiesFromBrokerList;


/**
 * Flink Sink to produce data into a Kafka topic. This producer is compatible with Kafka 0.10.x
 *
 * Implementation note: This producer is a hybrid between a regular regular sink function (a)
 * and a custom operator (b).
 *
 * For (a), the class implements the SinkFunction and RichFunction interfaces.
 * For (b), it extends the StreamTask class.
 *
 * Details about approach (a):
 *
 *  Pre Kafka 0.10 producers only follow approach (a), allowing users to use the producer using the
 *  DataStream.addSink() method.
 *  Since the APIs exposed in that variant do not allow accessing the the timestamp attached to the record
 *  the Kafka 0.10 producer has a section invocation option, approach (b).
 *
 * Details about approach (b):
 *  Kafka 0.10 supports writing the timestamp attached to a record to Kafka. When adding the
 *  FlinkKafkaProducer010 using the FlinkKafkaProducer010.writeToKafka() method, the Kafka producer
 *  can access the internal record timestamp of the record and write it to Kafka.
 *
 * All methods and constructors in this class are marked with the approach they are needed for.
 */
public class FlinkKafkaProducer010<T> extends StreamSink<T> implements SinkFunction<T>, RichFunction {

	/**
	 * Flag controlling whether we are writing the Flink record's timestamp into Kafka.
	 */
	private boolean writeTimestampToKafka = false;

	// ---------------------- "Constructors" for timestamp writing ------------------

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * This constructor allows writing timestamps to Kafka, it follow approach (b) (see above)
	 *
	 * @param inStream The stream to write to Kafka
	 * @param topicId ID of the Kafka topic.
	 * @param serializationSchema User defined serialization schema supporting key/value messages
	 * @param producerConfig Properties with the producer configuration.
	 */
	public static <T> FlinkKafkaProducer010Configuration writeToKafka(DataStream<T> inStream,
										String topicId,
										KeyedSerializationSchema<T> serializationSchema,
										Properties producerConfig) {
		return writeToKafka(inStream, topicId, serializationSchema, producerConfig, new FixedPartitioner<T>());
	}


	/**
	 * Creates a FlinkKafkaProducer for a given topic. the sink produces a DataStream to
	 * the topic.
	 *
	 * This constructor allows writing timestamps to Kafka, it follow approach (b) (see above)
	 *
	 * @param inStream The stream to write to Kafka
	 * @param topicId ID of the Kafka topic.
	 * @param serializationSchema User defined (keyless) serialization schema.
	 * @param producerConfig Properties with the producer configuration.
	 */
	public static <T> FlinkKafkaProducer010Configuration writeToKafka(DataStream<T> inStream,
										String topicId,
										SerializationSchema<T> serializationSchema,
										Properties producerConfig) {
		return writeToKafka(inStream, topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, new FixedPartitioner<T>());
	}

	/**
	 * Creates a FlinkKafkaProducer for a given topic. The sink produces a DataStream to
	 * the topic.
	 *
	 * This constructor allows writing timestamps to Kafka, it follow approach (b) (see above)
	 *
	 *  @param inStream The stream to write to Kafka
	 *  @param topicId The name of the target topic
	 *  @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 *  @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 *  @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions.
	 */
	public static <T> FlinkKafkaProducer010Configuration<T> writeToKafka(DataStream<T> inStream,
																String topicId,
																KeyedSerializationSchema<T> serializationSchema,
																Properties producerConfig,
																KafkaPartitioner<T> customPartitioner) {

		GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
		FlinkKafkaProducer010<T> kafkaProducer = new FlinkKafkaProducer010<>(topicId, serializationSchema, producerConfig, customPartitioner);
		SingleOutputStreamOperator<Object> transformation = inStream.transform("FlinKafkaProducer 0.10.x", objectTypeInfo, kafkaProducer);
		return new FlinkKafkaProducer010Configuration<>(transformation, kafkaProducer);
	}

	// ---------------------- Regular constructors w/o timestamp support  ------------------

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
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), getPropertiesFromBrokerList(brokerList), new FixedPartitioner<T>());
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
		this(topicId, new KeyedSerializationSchemaWrapper<>(serializationSchema), producerConfig, new FixedPartitioner<T>());
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
	public FlinkKafkaProducer010(String topicId, SerializationSchema<T> serializationSchema, Properties producerConfig, KafkaPartitioner<T> customPartitioner) {
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
		this(topicId, serializationSchema, getPropertiesFromBrokerList(brokerList), new FixedPartitioner<T>());
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
		this(topicId, serializationSchema, producerConfig, new FixedPartitioner<T>());
	}

	/**
	 * Create Kafka producer
	 *
	 * This constructor does not allow writing timestamps to Kafka, it follow approach (a) (see above)
	 */
	public FlinkKafkaProducer010(String topicId, KeyedSerializationSchema<T> serializationSchema, Properties producerConfig, KafkaPartitioner<T> customPartitioner) {
		// We create a Kafka 09 producer instance here and only "override" (by intercepting) the
		// invoke call.
		super(new FlinkKafkaProducer09<>(topicId, serializationSchema, producerConfig, customPartitioner));
	}


	// ----------------------------- Generic element processing  ---------------------------

	private void invokeInternal(T next, long elementTimestamp) throws Exception {

		final FlinkKafkaProducerBase<T> internalProducer = (FlinkKafkaProducerBase<T>) userFunction;

		internalProducer.checkErroneous();

		byte[] serializedKey = internalProducer.schema.serializeKey(next);
		byte[] serializedValue = internalProducer.schema.serializeValue(next);
		String targetTopic = internalProducer.schema.getTargetTopic(next);
		if (targetTopic == null) {
			targetTopic = internalProducer.defaultTopicId;
		}

		Long timestamp = null;
		if(this.writeTimestampToKafka) {
			timestamp = elementTimestamp;
		}

		ProducerRecord<byte[], byte[]> record;
		if (internalProducer.partitioner == null) {
			record = new ProducerRecord<>(targetTopic, null, timestamp, serializedKey, serializedValue);
		} else {
			record = new ProducerRecord<>(targetTopic, internalProducer.partitioner.partition(next, serializedKey, serializedValue, internalProducer.partitions.length), timestamp, serializedKey, serializedValue);
		}
		if (internalProducer.flushOnCheckpoint) {
			synchronized (internalProducer.pendingRecordsLock) {
				internalProducer.pendingRecords++;
			}
		}
		internalProducer.producer.send(record, internalProducer.callback);
	}


	// ----------------- Helper methods implementing methods from SinkFunction and RichFunction (Approach (a)) ----


	// ---- Configuration setters

	/**
	 * Defines whether the producer should fail on errors, or only log them.
	 * If this is set to true, then exceptions will be only logged, if set to false,
	 * exceptions will be eventually thrown and cause the streaming program to
	 * fail (and enter recovery).
	 *
	 * Method is only accessible for approach (a) (see above)
	 *
	 * @param logFailuresOnly The flag to indicate logging-only on exceptions.
	 */
	public void setLogFailuresOnly(boolean logFailuresOnly) {
		final FlinkKafkaProducerBase<T> internalProducer = (FlinkKafkaProducerBase<T>) userFunction;
		internalProducer.setLogFailuresOnly(logFailuresOnly);
	}

	/**
	 * If set to true, the Flink producer will wait for all outstanding messages in the Kafka buffers
	 * to be acknowledged by the Kafka producer on a checkpoint.
	 * This way, the producer can guarantee that messages in the Kafka buffers are part of the checkpoint.
	 *
	 * Method is only accessible for approach (a) (see above)
	 *
	 * @param flush Flag indicating the flushing mode (true = flush on checkpoint)
	 */
	public void setFlushOnCheckpoint(boolean flush) {
		final FlinkKafkaProducerBase<T> internalProducer = (FlinkKafkaProducerBase<T>) userFunction;
		internalProducer.setFlushOnCheckpoint(flush);
	}

	/**
	 * This method is used for approach (a) (see above)
	 *
	 */
	@Override
	public void open(Configuration parameters) throws Exception {
		final FlinkKafkaProducerBase<T> internalProducer = (FlinkKafkaProducerBase<T>) userFunction;
		internalProducer.open(parameters);
	}

	/**
	 * This method is used for approach (a) (see above)
	 */
	@Override
	public IterationRuntimeContext getIterationRuntimeContext() {
		final FlinkKafkaProducerBase<T> internalProducer = (FlinkKafkaProducerBase<T>) userFunction;
		return internalProducer.getIterationRuntimeContext();
	}

	/**
	 * This method is used for approach (a) (see above)
	 */
	@Override
	public void setRuntimeContext(RuntimeContext t) {
		final FlinkKafkaProducerBase<T> internalProducer = (FlinkKafkaProducerBase<T>) userFunction;
		internalProducer.setRuntimeContext(t);
	}

	/**
	 * Invoke method for using the Sink as DataStream.addSink() sink.
	 *
	 * This method is used for approach (a) (see above)
	 *
	 * @param value The input record.
	 */
	@Override
	public void invoke(T value) throws Exception {
		invokeInternal(value, Long.MAX_VALUE);
	}


	// ----------------- Helper methods and classes implementing methods from StreamSink (Approach (b)) ----


	/**
	 * Process method for using the sink with timestamp support.
	 *
	 * This method is used for approach (b) (see above)
	 */
	@Override
	public void processElement(StreamRecord<T> element) throws Exception {
		invokeInternal(element.getValue(), element.getTimestamp());
	}

	/**
	 * Configuration object returned by the writeToKafka() call.
	 */
	public static class FlinkKafkaProducer010Configuration<T> extends DataStreamSink<T> {

		private final FlinkKafkaProducerBase wrappedProducerBase;
		private final FlinkKafkaProducer010 producer;

		private FlinkKafkaProducer010Configuration(DataStream stream, FlinkKafkaProducer010<T> producer) {
			//noinspection unchecked
			super(stream, producer);
			this.producer = producer;
			this.wrappedProducerBase = (FlinkKafkaProducerBase) producer.userFunction;
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
			this.wrappedProducerBase.setLogFailuresOnly(logFailuresOnly);
		}

		/**
		 * If set to true, the Flink producer will wait for all outstanding messages in the Kafka buffers
		 * to be acknowledged by the Kafka producer on a checkpoint.
		 * This way, the producer can guarantee that messages in the Kafka buffers are part of the checkpoint.
		 *
		 * @param flush Flag indicating the flushing mode (true = flush on checkpoint)
		 */
		public void setFlushOnCheckpoint(boolean flush) {
			this.wrappedProducerBase.setFlushOnCheckpoint(flush);
		}

		/**
		 * If set to true, Flink will write the (event time) timestamp attached to each record into Kafka.
		 * Timestamps must be positive for Kafka to accept them.
		 *
		 * @param writeTimestampToKafka Flag indicating if Flink's internal timestamps are written to Kafka.
		 */
		public void setWriteTimestampToKafka(boolean writeTimestampToKafka) {
			this.producer.writeTimestampToKafka = writeTimestampToKafka;
		}
	}


}
