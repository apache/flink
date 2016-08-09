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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.SerializableObject;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.NetUtils;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;


/**
 * Flink Sink to produce data into a Kafka topic.
 *
 * Please note that this producer provides at-least-once reliability guarantees when
 * checkpoints are enabled and setFlushOnCheckpoint(true) is set.
 * Otherwise, the producer doesn't provide any reliability guarantees.
 *
 * @param <IN> Type of the messages to write into Kafka.
 */
public abstract class FlinkKafkaProducerBase<IN> extends RichSinkFunction<IN> implements Checkpointed<Serializable> {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducerBase.class);

	private static final long serialVersionUID = 1L;

	/**
	 * Configuration key for disabling the metrics reporting
	 */
	public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

	/**
	 * Array with the partition ids of the given defaultTopicId
	 * The size of this array is the number of partitions
	 */
	protected final int[] partitions;

	/**
	 * User defined properties for the Producer
	 */
	protected final Properties producerConfig;

	/**
	 * The name of the default topic this producer is writing data to
	 */
	protected final String defaultTopicId;

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

	/**
	 * If true, the producer will wait until all outstanding records have been send to the broker.
	 */
	protected boolean flushOnCheckpoint;
	
	// -------------------------------- Runtime fields ------------------------------------------

	/** KafkaProducer instance */
	protected transient KafkaProducer<byte[], byte[]> producer;

	/** The callback than handles error propagation or logging callbacks */
	protected transient Callback callback;

	/** Errors encountered in the async producer are stored here */
	protected transient volatile Exception asyncException;

	/** Lock for accessing the pending records */
	protected final SerializableObject pendingRecordsLock = new SerializableObject();

	/** Number of unacknowledged records. */
	protected long pendingRecords;


	/**
	 * The main constructor for creating a FlinkKafkaProducer.
	 *
	 * @param defaultTopicId The default topic to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions. Passing null will use Kafka's partitioner
	 */
	public FlinkKafkaProducerBase(String defaultTopicId, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig, KafkaPartitioner<IN> customPartitioner) {
		requireNonNull(defaultTopicId, "TopicID not set");
		requireNonNull(serializationSchema, "serializationSchema not set");
		requireNonNull(producerConfig, "producerConfig not set");
		ClosureCleaner.clean(customPartitioner, true);
		ClosureCleaner.ensureSerializable(serializationSchema);

		this.defaultTopicId = defaultTopicId;
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
		try (Producer<Void, IN> getPartitionsProd = getKafkaProducer(this.producerConfig)) {
			List<PartitionInfo> partitionsList = getPartitionsProd.partitionsFor(defaultTopicId);

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

	/**
	 * If set to true, the Flink producer will wait for all outstanding messages in the Kafka buffers
	 * to be acknowledged by the Kafka producer on a checkpoint.
	 * This way, the producer can guarantee that messages in the Kafka buffers are part of the checkpoint.
	 *
	 * @param flush Flag indicating the flushing mode (true = flush on checkpoint)
	 */
	public void setFlushOnCheckpoint(boolean flush) {
		this.flushOnCheckpoint = flush;
	}

	/**
	 * Used for testing only
	 */
	protected <K,V> KafkaProducer<K,V> getKafkaProducer(Properties props) {
		return new KafkaProducer<>(props);
	}

	// ----------------------------------- Utilities --------------------------
	
	/**
	 * Initializes the connection to Kafka.
	 */
	@Override
	public void open(Configuration configuration) {
		producer = getKafkaProducer(this.producerConfig);

		RuntimeContext ctx = getRuntimeContext();
		if (partitioner != null) {
			partitioner.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks(), partitions);
		}

		LOG.info("Starting FlinkKafkaProducer ({}/{}) to produce into topic {}", 
				ctx.getIndexOfThisSubtask() + 1, ctx.getNumberOfParallelSubtasks(), defaultTopicId);

		// register Kafka metrics to Flink accumulators
		if (!Boolean.parseBoolean(producerConfig.getProperty(KEY_DISABLE_METRICS, "false"))) {
			Map<MetricName, ? extends Metric> metrics = this.producer.metrics();

			if (metrics == null) {
				// MapR's Kafka implementation returns null here.
				LOG.info("Producer implementation does not support metrics");
			} else {
				final MetricGroup kafkaMetricGroup = getRuntimeContext().getMetricGroup().addGroup("KafkaProducer");
				for (Map.Entry<MetricName, ? extends Metric> metric: metrics.entrySet()) {
					kafkaMetricGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));
				}
			}
		}

		if (flushOnCheckpoint && !((StreamingRuntimeContext)this.getRuntimeContext()).isCheckpointingEnabled()) {
			LOG.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
			flushOnCheckpoint = false;
		}

		if (logFailuresOnly) {
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null) {
						LOG.error("Error while sending record to Kafka: " + e.getMessage(), e);
					}
					acknowledgeMessage();
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
					acknowledgeMessage();
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
		String targetTopic = schema.getTargetTopic(next);
		if (targetTopic == null) {
			targetTopic = defaultTopicId;
		}

		ProducerRecord<byte[], byte[]> record;
		if (partitioner == null) {
			record = new ProducerRecord<>(targetTopic, serializedKey, serializedValue);
		} else {
			record = new ProducerRecord<>(targetTopic, partitioner.partition(next, serializedKey, serializedValue, partitions.length), serializedKey, serializedValue);
		}
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords++;
			}
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

	// ------------------- Logic for handling checkpoint flushing -------------------------- //

	private void acknowledgeMessage() {
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords--;
				if (pendingRecords == 0) {
					pendingRecordsLock.notifyAll();
				}
			}
		}
	}

	/**
	 * Flush pending records.
	 */
	protected abstract void flush();

	@Override
	public Serializable snapshotState(long checkpointId, long checkpointTimestamp) {
		if (flushOnCheckpoint) {
			// flushing is activated: We need to wait until pendingRecords is 0
			flush();
			synchronized (pendingRecordsLock) {
				if (pendingRecords != 0) {
					throw new IllegalStateException("Pending record count must be zero at this point: " + pendingRecords);
				}
				// pending records count is 0. We can now confirm the checkpoint
			}
		}
		// return empty state
		return null;
	}

	@Override
	public void restoreState(Serializable state) {
		// nothing to do here
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
