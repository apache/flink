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

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A fetcher that fetches data from Kafka brokers via the Kafka 0.9 consumer API.
 * 
 * @param <T> The type of elements produced by the fetcher.
 */
public class Kafka09Fetcher<T> extends AbstractFetcher<T, TopicPartition> {

	private static final Logger LOG = LoggerFactory.getLogger(Kafka09Fetcher.class);

	// ------------------------------------------------------------------------

	/** The schema to convert between Kafka's byte messages, and Flink's objects */
	private final KeyedDeserializationSchema<T> deserializer;

	/** The handover of data and exceptions between the consumer thread and the task thread */
	private final Handover handover;

	/** The thread that runs the actual KafkaConsumer and hand the record batches to this fetcher */
	private final KafkaConsumerThread consumerThread;

	/** Flag to mark the main work loop as alive */
	private volatile boolean running = true;

	// ------------------------------------------------------------------------

	public Kafka09Fetcher(
			SourceContext<T> sourceContext,
			List<KafkaTopicPartition> assignedPartitions,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval,
			ClassLoader userCodeClassLoader,
			boolean enableCheckpointing,
			String taskNameWithSubtasks,
			MetricGroup metricGroup,
			KeyedDeserializationSchema<T> deserializer,
			Properties kafkaProperties,
			long pollTimeout,
			boolean useMetrics) throws Exception
	{
		super(
				sourceContext,
				assignedPartitions,
				watermarksPeriodic,
				watermarksPunctuated,
				processingTimeProvider,
				autoWatermarkInterval,
				userCodeClassLoader,
				useMetrics);

		this.deserializer = deserializer;
		this.handover = new Handover();

		final MetricGroup kafkaMetricGroup = metricGroup.addGroup("KafkaConsumer");
		addOffsetStateGauge(kafkaMetricGroup);

		// if checkpointing is enabled, we are not automatically committing to Kafka.
		kafkaProperties.setProperty(
				ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
				Boolean.toString(!enableCheckpointing));
		
		this.consumerThread = new KafkaConsumerThread(
				LOG,
				handover,
				kafkaProperties,
				subscribedPartitions(),
				kafkaMetricGroup,
				createCallBridge(),
				getFetcherName() + " for " + taskNameWithSubtasks,
				pollTimeout,
				useMetrics);
	}

	// ------------------------------------------------------------------------
	//  Fetcher work methods
	// ------------------------------------------------------------------------

	@Override
	public void runFetchLoop() throws Exception {
		try {
			final Handover handover = this.handover;

			// kick off the actual Kafka consumer
			consumerThread.start();

			while (running) {
				// this blocks until we get the next records
				// it automatically re-throws exceptions encountered in the fetcher thread
				final ConsumerRecords<byte[], byte[]> records = handover.pollNext();

				// get the records for each topic partition
				for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitions()) {

					List<ConsumerRecord<byte[], byte[]>> partitionRecords =
							records.records(partition.getKafkaPartitionHandle());

					for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {

						final T value = deserializer.deserialize(
								record.key(), record.value(),
								record.topic(), record.partition(), record.offset());

						if (deserializer.isEndOfStream(value)) {
							// end of stream signaled
							running = false;
							break;
						}

						// emit the actual record. this also updates offset state atomically
						// and deals with timestamps and watermark generation
						emitRecord(value, partition, record.offset(), record);
					}
				}
			}
		}
		finally {
			// this signals the consumer thread that no more work is to be done
			consumerThread.shutdown();
		}

		// on a clean exit, wait for the runner thread
		try {
			consumerThread.join();
		}
		catch (InterruptedException e) {
			// may be the result of a wake-up interruption after an exception.
			// we ignore this here and only restore the interruption state
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void cancel() {
		// flag the main thread to exit. A thread interrupt will come anyways.
		running = false;
		handover.close();
		consumerThread.shutdown();
	}

	// ------------------------------------------------------------------------
	//  The below methods are overridden in the 0.10 fetcher, which otherwise
	//   reuses most of the 0.9 fetcher behavior
	// ------------------------------------------------------------------------

	protected void emitRecord(
			T record,
			KafkaTopicPartitionState<TopicPartition> partition,
			long offset,
			@SuppressWarnings("UnusedParameters") ConsumerRecord<?, ?> consumerRecord) throws Exception {

		// the 0.9 Fetcher does not try to extract a timestamp
		emitRecord(record, partition, offset);
	}

	/**
	 * Gets the name of this fetcher, for thread naming and logging purposes.
	 */
	protected String getFetcherName() {
		return "Kafka 0.9 Fetcher";
	}

	protected KafkaConsumerCallBridge createCallBridge() {
		return new KafkaConsumerCallBridge();
	}

	// ------------------------------------------------------------------------
	//  Implement Methods of the AbstractFetcher
	// ------------------------------------------------------------------------

	@Override
	public TopicPartition createKafkaPartitionHandle(KafkaTopicPartition partition) {
		return new TopicPartition(partition.getTopic(), partition.getPartition());
	}

	@Override
	public void commitInternalOffsetsToKafka(Map<KafkaTopicPartition, Long> offsets) throws Exception {
		KafkaTopicPartitionState<TopicPartition>[] partitions = subscribedPartitions();
		Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>(partitions.length);

		for (KafkaTopicPartitionState<TopicPartition> partition : partitions) {
			Long lastProcessedOffset = offsets.get(partition.getKafkaTopicPartition());
			if (lastProcessedOffset != null) {
				// committed offsets through the KafkaConsumer need to be 1 more than the last processed offset.
				// This does not affect Flink's checkpoints/saved state.
				long offsetToCommit = lastProcessedOffset + 1;

				offsetsToCommit.put(partition.getKafkaPartitionHandle(), new OffsetAndMetadata(offsetToCommit));
				partition.setCommittedOffset(offsetToCommit);
			}
		}

		// record the work to be committed by the main consumer thread and make sure the consumer notices that
		consumerThread.setOffsetsToCommit(offsetsToCommit);
	}
}
