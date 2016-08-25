/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.ExceptionProxy;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A fetcher that fetches data from Kafka brokers via the Kafka 0.9 consumer API.
 * 
 * @param <T> The type of elements produced by the fetcher.
 */
public class Kafka09Fetcher<T> extends AbstractFetcher<T, TopicPartition> implements Runnable {

	private static final Logger LOG = LoggerFactory.getLogger(Kafka09Fetcher.class);

	// ------------------------------------------------------------------------

	/** The schema to convert between Kafka's byte messages, and Flink's objects */
	private final KeyedDeserializationSchema<T> deserializer;

	/** The subtask's runtime context */
	private final RuntimeContext runtimeContext;

	/** The configuration for the Kafka consumer */
	private final Properties kafkaProperties;

	/** The maximum number of milliseconds to wait for a fetch batch */
	private final long pollTimeout;

	/** Mutex to guard against concurrent access to the non-threadsafe Kafka consumer */
	private final Object consumerLock = new Object();

	/** Reference to the Kafka consumer, once it is created */
	private volatile KafkaConsumer<byte[], byte[]> consumer;

	/** Reference to the proxy, forwarding exceptions from the fetch thread to the main thread */
	private volatile ExceptionProxy errorHandler;

	/** Flag to mark the main work loop as alive */
	private volatile boolean running = true;

	// ------------------------------------------------------------------------

	public Kafka09Fetcher(
			SourceContext<T> sourceContext,
			List<KafkaTopicPartition> assignedPartitions,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext,
			KeyedDeserializationSchema<T> deserializer,
			Properties kafkaProperties,
			long pollTimeout,
			boolean useMetrics) throws Exception
	{
		super(sourceContext, assignedPartitions, watermarksPeriodic, watermarksPunctuated, runtimeContext, useMetrics);

		this.deserializer = deserializer;
		this.runtimeContext = runtimeContext;
		this.kafkaProperties = kafkaProperties;
		this.pollTimeout = pollTimeout;

		// if checkpointing is enabled, we are not automatically committing to Kafka.
		kafkaProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
				Boolean.toString(!runtimeContext.isCheckpointingEnabled()));
	}

	// ------------------------------------------------------------------------
	//  Fetcher work methods
	// ------------------------------------------------------------------------

	@Override
	public void runFetchLoop() throws Exception {
		this.errorHandler = new ExceptionProxy(Thread.currentThread());

		// rather than running the main fetch loop directly here, we spawn a dedicated thread
		// this makes sure that no interrupt() call upon canceling reaches the Kafka consumer code
		Thread runner = new Thread(this, getFetcherName() + " for " + runtimeContext.getTaskNameWithSubtasks());
		runner.setDaemon(true);
		runner.start();

		try {
			runner.join();
		} catch (InterruptedException e) {
			// may be the result of a wake-up after an exception. we ignore this here and only
			// restore the interruption state
			Thread.currentThread().interrupt();
		}

		// make sure we propagate any exception that occurred in the concurrent fetch thread,
		// before leaving this method
		this.errorHandler.checkAndThrowException();
	}

	@Override
	public void cancel() {
		// flag the main thread to exit
		running = false;

		// NOTE:
		//   - We cannot interrupt the runner thread, because the Kafka consumer may
		//     deadlock when the thread is interrupted while in certain methods
		//   - We cannot call close() on the consumer, because it will actually throw
		//     an exception if a concurrent call is in progress

		// make sure the consumer finds out faster that we are shutting down 
		if (consumer != null) {
			consumer.wakeup();
		}
	}

	@Override
	public void run() {
		// This method initializes the KafkaConsumer and guarantees it is torn down properly.
		// This is important, because the consumer has multi-threading issues,
		// including concurrent 'close()' calls.

		final KafkaConsumer<byte[], byte[]> consumer;
		try {
			consumer = new KafkaConsumer<>(kafkaProperties);
		}
		catch (Throwable t) {
			running = false;
			errorHandler.reportError(t);
			return;
		}

		// from here on, the consumer will be closed properly
		try {
			assignPartitionsToConsumer(consumer, convertKafkaPartitions(subscribedPartitions()));


			if (useMetrics) {
				final MetricGroup kafkaMetricGroup = runtimeContext.getMetricGroup().addGroup("KafkaConsumer");
				addOffsetStateGauge(kafkaMetricGroup);
				// register Kafka metrics to Flink
				Map<MetricName, ? extends Metric> metrics = consumer.metrics();
				if (metrics == null) {
					// MapR's Kafka implementation returns null here.
					LOG.info("Consumer implementation does not support metrics");
				} else {
					// we have Kafka metrics, register them
					for (Map.Entry<MetricName, ? extends Metric> metric: metrics.entrySet()) {
						kafkaMetricGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));
					}
				}
			}

			// seek the consumer to the initial offsets
			for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitions()) {
				if (partition.isOffsetDefined()) {
					consumer.seek(partition.getKafkaPartitionHandle(), partition.getOffset() + 1);
				}
			}

			// from now on, external operations may call the consumer
			this.consumer = consumer;

			// main fetch loop
			while (running) {
				// get the next batch of records
				final ConsumerRecords<byte[], byte[]> records;
				synchronized (consumerLock) {
					try {
						records = consumer.poll(pollTimeout);
					}
					catch (WakeupException we) {
						if (running) {
							throw we;
						} else {
							continue;
						}
					}
				}

				// get the records for each topic partition
				for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitions()) {
					
					List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(partition.getKafkaPartitionHandle());

					for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
						T value = deserializer.deserialize(
								record.key(), record.value(),
								record.topic(), record.partition(), record.offset());

						if (deserializer.isEndOfStream(value)) {
							// end of stream signaled
							running = false;
							break;
						}

						// emit the actual record. this also update offset state atomically
						// and deals with timestamps and watermark generation
						emitRecord(value, partition, record.offset(), record);
					}
				}
			}
			// end main fetch loop
		}
		catch (Throwable t) {
			if (running) {
				running = false;
				errorHandler.reportError(t);
			} else {
				LOG.debug("Stopped ConsumerThread threw exception", t);
			}
		}
		finally {
			try {
				synchronized (consumerLock) {
					consumer.close();
				}
			} catch (Throwable t) {
				LOG.warn("Error while closing Kafka 0.9 consumer", t);
			}
		}
	}

	// Kafka09Fetcher ignores the timestamp.
	protected void emitRecord(T record, KafkaTopicPartitionState<TopicPartition> partition, long offset, ConsumerRecord consumerRecord) throws Exception {
		emitRecord(record, partition, offset, Long.MAX_VALUE);
	}
	/**
	 * Protected method to make the partition assignment pluggable, for different Kafka versions.
	 */
	protected void assignPartitionsToConsumer(KafkaConsumer<byte[], byte[]> consumer, List<TopicPartition> topicPartitions) {
		consumer.assign(topicPartitions);
	}

	protected String getFetcherName() {
		return "Kafka 0.9 Fetcher";
	}

	// ------------------------------------------------------------------------
	//  Kafka 0.9 specific fetcher behavior
	// ------------------------------------------------------------------------

	@Override
	public TopicPartition createKafkaPartitionHandle(KafkaTopicPartition partition) {
		return new TopicPartition(partition.getTopic(), partition.getPartition());
	}

	@Override
	public void commitSpecificOffsetsToKafka(Map<KafkaTopicPartition, Long> offsets) throws Exception {
		KafkaTopicPartitionState<TopicPartition>[] partitions = subscribedPartitions();
		Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>(partitions.length);

		for (KafkaTopicPartitionState<TopicPartition> partition : partitions) {
			Long offset = offsets.get(partition.getKafkaTopicPartition());
			if (offset != null) {
				offsetsToCommit.put(partition.getKafkaPartitionHandle(), new OffsetAndMetadata(offset));
				partition.setCommittedOffset(offset);
			}
		}

		if (this.consumer != null) {
			synchronized (consumerLock) {
				this.consumer.commitSync(offsetsToCommit);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	public static List<TopicPartition> convertKafkaPartitions(KafkaTopicPartitionState<TopicPartition>[] partitions) {
		ArrayList<TopicPartition> result = new ArrayList<>(partitions.length);
		for (KafkaTopicPartitionState<TopicPartition> p : partitions) {
			result.add(p.getKafkaPartitionHandle());
		}
		return result;
	}
}
