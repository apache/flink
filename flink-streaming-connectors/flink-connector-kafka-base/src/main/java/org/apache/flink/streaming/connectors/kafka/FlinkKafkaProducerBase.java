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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.DefaultKafkaMetricAccumulator;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.NetUtils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Flink Sink to produce data into a Kafka topic.
 *
 * Please note that this producer does not have any reliability guarantees.
 *
 * @param <IN> Type of the messages to write into Kafka.
 */
public abstract class FlinkKafkaProducerBase<IN> extends RichSinkFunction<IN>  {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducerBase.class);

	private static final long serialVersionUID = 1L;

	/**
	 * Configuration key for disabling the metrics reporting
	 */
	public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

	/**
	 * Array with the partition ids of the given topicId
	 * The size of this array is the number of partitions
	 */
	protected final int[] partitions;

	/**
	 * User defined properties for the Producer
	 */
	protected final Properties producerConfig;

	/**
	 * The name of the topic this producer is writing data to
	 */
	protected final String topicId;

	/**
	 * (Serializable) SerializationSchema for turning objects used with Flink into
	 * byte[] for Kafka.
	 */
	protected final KeyedSerializationSchema<IN> schema;

	/**
	 * User-provided partitioner for assigning an object to a Kafka partition.
	 */
	protected final KafkaPartitioner<IN> partitioner;

	/**
	 * Flag indicating whether to accept failures (and log them), or to fail on failures
	 */
	protected boolean logFailuresOnly;
	
	// -------------------------------- Runtime fields ------------------------------------------

	/** KafkaProducer instance */
	protected transient KafkaProducer<byte[], byte[]> producer;

	/** The callback than handles error propagation or logging callbacks */
	protected transient Callback callback;
	
	/** Errors encountered in the async producer are stored here */
	protected transient volatile Exception asyncException;


	/**
	 * The main constructor for creating a FlinkKafkaProducer.
	 *
	 * @param topicId The topic to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions. Passing null will use Kafka's partitioner
	 */
	public FlinkKafkaProducerBase(String topicId, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig, KafkaPartitioner<IN> customPartitioner) {
		Preconditions.checkNotNull(topicId, "TopicID not set");
		Preconditions.checkNotNull(serializationSchema, "serializationSchema not set");
		Preconditions.checkNotNull(producerConfig, "producerConfig not set");
		ClosureCleaner.ensureSerializable(customPartitioner);
		ClosureCleaner.ensureSerializable(serializationSchema);

		this.topicId = topicId;
		this.schema = serializationSchema;
		this.producerConfig = producerConfig;

		// set the producer configuration properties.

		if (!producerConfig.contains(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
		}

		if (!producerConfig.contains(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
		}


		// create a local KafkaProducer to get the list of partitions.
		// this will also ensure locally that all required ProducerConfig values are set.
		try (KafkaProducer<Void, IN> getPartitionsProd = new KafkaProducer<>(this.producerConfig)) {
			List<PartitionInfo> partitionsList = getPartitionsProd.partitionsFor(topicId);

			this.partitions = new int[partitionsList.size()];
			for (int i = 0; i < partitions.length; i++) {
				partitions[i] = partitionsList.get(i).partition();
			}
			getPartitionsProd.close();
		}

		this.partitioner = customPartitioner;
	}

	// ---------------------------------- Properties --------------------------

	/**
	 * Defines whether the producer should fail on errors, or only log them.
	 * If this is set to true, then exceptions will be only logged, if set to false,
	 * exceptions will be eventually thrown and cause the streaming program to 
	 * fail (and enter recovery).
	 * 
	 * @param logFailuresOnly The flag to indicate logging-only on exceptions.
	 */
	public void setLogFailuresOnly(boolean logFailuresOnly) {
		this.logFailuresOnly = logFailuresOnly;
	}

	// ----------------------------------- Utilities --------------------------
	
	/**
	 * Initializes the connection to Kafka.
	 */
	@Override
	public void open(Configuration configuration) {
		producer = new org.apache.kafka.clients.producer.KafkaProducer<>(this.producerConfig);

		RuntimeContext ctx = getRuntimeContext();
		if(partitioner != null) {
			partitioner.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks(), partitions);
		}

		LOG.info("Starting FlinkKafkaProducer ({}/{}) to produce into topic {}", 
				ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks(), topicId);

		// register Kafka metrics to Flink accumulators
		if(!Boolean.getBoolean(producerConfig.getProperty(KEY_DISABLE_METRICS, "false"))) {
			Map<MetricName, ? extends Metric> metrics = this.producer.metrics();

			if(metrics == null) {
				// MapR's Kafka implementation returns null here.
				LOG.info("Producer implementation does not support metrics");
			} else {
				for(Map.Entry<MetricName, ? extends Metric> metric: metrics.entrySet()) {
					String name = "producer-" + metric.getKey().name();
					DefaultKafkaMetricAccumulator kafkaAccumulator = DefaultKafkaMetricAccumulator.createFor(metric.getValue());
					// best effort: we only add the accumulator if available.
					if(kafkaAccumulator != null) {
						getRuntimeContext().addAccumulator(name, kafkaAccumulator);
					}
				}
			}
		}

		if (logFailuresOnly) {
			callback = new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null) {
						LOG.error("Error while sending record to Kafka: " + e.getMessage(), e);
					}
				}
			};
		}
		else {
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null && asyncException == null) {
						asyncException = exception;
					}
				}
			};
		}
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Kafka.
	 *
	 * @param next
	 * 		The incoming data
	 */
	@Override
	public void invoke(IN next) throws Exception {
		// propagate asynchronous errors
		checkErroneous();

		byte[] serializedKey = schema.serializeKey(next);
		byte[] serializedValue = schema.serializeValue(next);
		ProducerRecord<byte[], byte[]> record;
		if(partitioner == null) {
			record = new ProducerRecord<>(topicId, serializedKey, serializedValue);
		} else {
			record = new ProducerRecord<>(topicId, partitioner.partition(next, serializedKey, serializedValue, partitions.length), serializedKey, serializedValue);
		}

		producer.send(record, callback);
	}


	@Override
	public void close() throws Exception {
		if (producer != null) {
			producer.close();
		}
		
		// make sure we propagate pending errors
		checkErroneous();
	}


	// ----------------------------------- Utilities --------------------------

	protected void checkErroneous() throws Exception {
		Exception e = asyncException;
		if (e != null) {
			// prevent double throwing
			asyncException = null;
			throw new Exception("Failed to send data to Kafka: " + e.getMessage(), e);
		}
	}
	
	public static Properties getPropertiesFromBrokerList(String brokerList) {
		String[] elements = brokerList.split(",");
		
		// validate the broker addresses
		for (String broker: elements) {
			NetUtils.getCorrectHostnamePort(broker);
		}
		
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		return props;
	}
}
