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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KafkaSerializationSchema;
import org.apache.flink.streaming.util.serialization.KafkaSerializationSchemaAdapterBase;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.SerializableObject;

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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * Flink Sink to produce data into a Kafka topic.
 *
 * <p>Please note that this producer provides at-least-once reliability guarantees when
 * checkpoints are enabled and setFlushOnCheckpoint(true) is set.
 * Otherwise, the producer doesn't provide any reliability guarantees.
 *
 * @param <IN> Type of the messages to write into Kafka.
 */
@Internal
public abstract class FlinkKafkaProducerBase<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducerBase.class);

	private static final long serialVersionUID = 1L;

	/**
	 * Configuration key for disabling the metrics reporting.
	 */
	public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

	/**
	 * User defined properties for the Producer.
	 */
	protected final Properties producerConfig;

	/**
	 * (Serializable) SerializationSchema for turning objects used with Flink into.
	 * byte[] for Kafka.
	 */
	protected final KafkaSerializationSchema<IN> schema;


	protected transient PartitionInfoBase schemaPartitionInfo;

	/**
	 * Flag indicating whether to accept failures (and log them), or to fail on failures.
	 */
	protected boolean logFailuresOnly;

	/**
	 * If true, the producer will wait until all outstanding records have been send to the broker.
	 */
	protected boolean flushOnCheckpoint = true;

	// -------------------------------- Runtime fields ------------------------------------------

	/** KafkaProducer instance. */
	protected transient KafkaProducer<byte[], byte[]> producer;

	/** The callback than handles error propagation or logging callbacks. */
	protected transient Callback callback;

	/** Errors encountered in the async producer are stored here. */
	protected transient volatile Exception asyncException;

	/** Lock for accessing the pending records. */
	protected final SerializableObject pendingRecordsLock = new SerializableObject();

	/** Number of unacknowledged records. */
	protected long pendingRecords;

	/**
	 * The constructor for creating a FlinkKafkaProducer using given KeyedSerializationSchema and custom partitioner.
	 *
	 * @param defaultTopicId The default topic to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions. Passing null will use Kafka's partitioner.
	 */
	public FlinkKafkaProducerBase(String defaultTopicId, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig, FlinkKafkaPartitioner<IN> customPartitioner) {
		this(
			new KafkaSerializationSchemaAdapterBase<>(
				defaultTopicId,
				serializationSchema,
				customPartitioner
			),
			producerConfig);
	}

	/**
	 * The main constructor for creating a FlinkKafkaProducer.
	 * @param serializationSchema A serializable serialization schema for turning user objects into a ProducerRecords
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 */
	public FlinkKafkaProducerBase(KafkaSerializationSchema<IN> serializationSchema, Properties producerConfig) {
		requireNonNull(serializationSchema, "serializationSchema not set");
		requireNonNull(producerConfig, "producerConfig not set");
		ClosureCleaner.ensureSerializable(serializationSchema);

		this.schema = serializationSchema;
		this.producerConfig = producerConfig;

		// set the producer configuration properties for kafka record key value serializers.
		if (!producerConfig.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
		}

		if (!producerConfig.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
		}

		// eagerly ensure that bootstrap servers are set.
		if (!this.producerConfig.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
			throw new IllegalArgumentException(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + " must be supplied in the producer config properties.");
		}
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
	 * Used for testing only.
	 */
	@VisibleForTesting
	protected <K, V> KafkaProducer<K, V> getKafkaProducer(Properties props) {
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

		schemaPartitionInfo = new PartitionInfoBase(ctx);
		if (schema instanceof KafkaSerializationSchemaAdapterBase) {
			((KafkaSerializationSchemaAdapterBase<IN>) schema).open(ctx, schemaPartitionInfo);
		}

		LOG.info("Starting FlinkKafkaProducer ({}/{})",
				ctx.getIndexOfThisSubtask() + 1, ctx.getNumberOfParallelSubtasks());

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

		if (flushOnCheckpoint && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
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
	public void invoke(IN next, Context context) throws Exception {
		// propagate asynchronous errors
		checkErroneous();

		final ProducerRecord<byte[], byte[]> record = schema.serialize(next, null, schemaPartitionInfo);
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
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do
	}

	@Override
	public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
		// check for asynchronous errors and fail the checkpoint if necessary
		checkErroneous();

		if (flushOnCheckpoint) {
			// flushing is activated: We need to wait until pendingRecords is 0
			flush();
			synchronized (pendingRecordsLock) {
				if (pendingRecords != 0) {
					throw new IllegalStateException("Pending record count must be zero at this point: " + pendingRecords);
				}

				// if the flushed requests has errors, we should propagate it also and fail the checkpoint
				checkErroneous();
			}
		}
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

	@VisibleForTesting
	protected long numPendingRecords() {
		synchronized (pendingRecordsLock) {
			return pendingRecords;
		}
	}

	private class PartitionInfoBase implements KafkaSerializationSchema.PartitionInfo, Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * Partitions of each topic.
		 */
		private final Map<String, int[]> topicPartitionsMap = new HashMap<>();
		private final int subtaskIndex;
		private final int maxNumberOfParallelSubtasks;

		PartitionInfoBase(RuntimeContext ctx) {
			this.subtaskIndex = ctx.getIndexOfThisSubtask();
			this.maxNumberOfParallelSubtasks = ctx.getMaxNumberOfParallelSubtasks();
		}

		@Override
		public int[] partitionsFor(String topicName) {
			return topicPartitionsMap.computeIfAbsent(topicName, this::fetch);
		}

		@Override
		public int getIndexOfThisSubtask() {
			return subtaskIndex;
		}

		@Override
		public int getMaxNumberOfParallelSubtasks() {
			return maxNumberOfParallelSubtasks;
		}

		private int[] fetch(String topic) {
			// the fetched list is immutable, so we're creating a mutable copy in order to sort it
			List<PartitionInfo> partitionsList = new ArrayList<>(producer.partitionsFor(topic));

			// sort the partitions by partition id to make sure the fetched partition list is the same across subtasks
			Collections.sort(partitionsList, new Comparator<PartitionInfo>() {
				@Override
				public int compare(PartitionInfo o1, PartitionInfo o2) {
					return Integer.compare(o1.partition(), o2.partition());
				}
			});

			int[] partitions = new int[partitionsList.size()];
			for (int i = 0; i < partitions.length; i++) {
				partitions[i] = partitionsList.get(i).partition();
			}

			return partitions;
		}
	}
}
