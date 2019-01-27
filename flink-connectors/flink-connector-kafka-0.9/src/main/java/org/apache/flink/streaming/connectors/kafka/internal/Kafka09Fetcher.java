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

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaCommitCallback;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A fetcher that fetches data from Kafka brokers via the Kafka 0.9 consumer API.
 *
 * @param <T> The type of elements produced by the fetcher.
 */
@Internal
public class Kafka09Fetcher<T> extends AbstractFetcher<T, TopicPartition> {

	private static final Logger LOG = LoggerFactory.getLogger(Kafka09Fetcher.class);

	// ------------------------------------------------------------------------

	/** The schema to convert between Kafka's byte messages, and Flink's objects. */
	private final KeyedDeserializationSchema<T> deserializer;

	/** The handover of data and exceptions between the consumer thread and the task thread. */
	private final Handover handover;

	/** The thread that runs the actual KafkaConsumer and hand the record batches to this fetcher. */
	private final KafkaConsumerThread consumerThread;

	/** Flag to mark the main work loop as alive. */
	private volatile boolean running = true;

	// ------------------------------------------------------------------------

	public Kafka09Fetcher(
			SourceContext<T> sourceContext,
			Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
			Map<KafkaTopicPartition, Long> assignedPartitionsToEndOffsets,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			ProcessingTimeService processingTimeProvider,
			long autoWatermarkInterval,
			ClassLoader userCodeClassLoader,
			String taskNameWithSubtasks,
			KeyedDeserializationSchema<T> deserializer,
			Properties kafkaProperties,
			long pollTimeout,
			MetricGroup subtaskMetricGroup,
			MetricGroup consumerMetricGroup,
			boolean useMetrics) throws Exception {
		super(
				sourceContext,
				assignedPartitionsWithInitialOffsets,
				assignedPartitionsToEndOffsets,
				watermarksPeriodic,
				watermarksPunctuated,
				processingTimeProvider,
				autoWatermarkInterval,
				userCodeClassLoader,
				consumerMetricGroup,
				useMetrics);

		this.deserializer = deserializer;
		this.handover = new Handover();

		this.consumerThread = new KafkaConsumerThread(
				LOG,
				handover,
				kafkaProperties,
				unassignedPartitionsQueue,
				createCallBridge(),
				getFetcherName() + " for " + taskNameWithSubtasks,
				pollTimeout,
				useMetrics,
				consumerMetricGroup,
				subtaskMetricGroup);
	}

	// ------------------------------------------------------------------------
	//  Fetcher work methods
	// ------------------------------------------------------------------------

	public void requestAndSetLatestEndOffsetsFromKafka(
			KafkaConsumer<byte[], byte[]> consumer,
			KafkaConsumerCallBridge consumerCallBridge,
			List<KafkaTopicPartitionState<TopicPartition>> partitions) throws Exception {

		List<TopicPartition> topicPartitions = new ArrayList<>(partitions.size());

		Map<TopicPartition, KafkaTopicPartitionState<TopicPartition>>
			topicPartitionToKafkaTopicPartitionState = new HashMap<>();
		for (KafkaTopicPartitionState<TopicPartition> partitionState : partitions) {
			if (partitionState.getEndOffset() == KafkaTopicPartitionStateSentinel.LATEST_OFFSET) {
				topicPartitions.add(partitionState.getKafkaPartitionHandle());
				topicPartitionToKafkaTopicPartitionState.put(
						partitionState.getKafkaPartitionHandle(), partitionState);
			}
		}

		consumerCallBridge.assignPartitions(consumer, topicPartitions);
		for (TopicPartition topicPartition : topicPartitions) {
			consumerCallBridge.seekPartitionToEnd(consumer, topicPartition);
			topicPartitionToKafkaTopicPartitionState.get(topicPartition)
				.setEndOffset(consumer.position(topicPartition));
		}
	}

	@Override
	public void runFetchLoop(boolean dynamicDiscoverEnabled) throws Exception {
		try {
			final Handover handover = this.handover;
			if (subscribedPartitionStates().size() == 0 && !dynamicDiscoverEnabled) {
				return;
			}

			KafkaConsumer<byte[], byte[]> consumer = consumerThread.getConsumer();
			try {
				requestAndSetLatestEndOffsetsFromKafka(consumer, createCallBridge(), subscribedPartitionStates());
			} finally {
				consumer.close();
			}
			// kick off the actual Kafka consumer
			consumerThread.setDynamicDiscoverEnabled(dynamicDiscoverEnabled);
			consumerThread.start();

			while (running) {
				// this blocks until we get the next records
				// it automatically re-throws exceptions encountered in the consumer thread
				Handover.ConsumerRecordsAndPositions records;
				try {
					records = handover.pollNext();
				} catch (Handover.ClosedException e) {
					break;
				}

				// get the records for each topic partition
				int finished = 0;
				for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitionStates()) {

					if (partition.isFinished()) {
						finished++;
						continue;
					}

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

						if (record.offset() + 1 >= partition.getEndOffset()) {
							partition.setFinished();
							break;
						}
					}

					// When transaction is enabled, the offset of the last record in a partition may not
					// be the log end offset due to in-band transaction markers. So we explicitly check
					// the consumer's position to see if the consumption should stop or not.
					Long position = records.position(partition.getKafkaPartitionHandle());
					if (position != null && position == partition.getEndOffset() && !partition.isFinished()) {
						partition.setFinished();
					}
				}
				if (!dynamicDiscoverEnabled && finished == subscribedPartitionStates().size()) {
					break;
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
	protected void doCommitInternalOffsetsToKafka(
			Map<KafkaTopicPartition, Long> offsets,
			@Nonnull KafkaCommitCallback commitCallback) throws Exception {

		@SuppressWarnings("unchecked")
		List<KafkaTopicPartitionState<TopicPartition>> partitions = subscribedPartitionStates();

		Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>(partitions.size());

		for (KafkaTopicPartitionState<TopicPartition> partition : partitions) {
			Long lastProcessedOffset = offsets.get(partition.getKafkaTopicPartition());
			if (lastProcessedOffset != null) {
				checkState(lastProcessedOffset >= 0, "Illegal offset value to commit");

				// committed offsets through the KafkaConsumer need to be 1 more than the last processed offset.
				// This does not affect Flink's checkpoints/saved state.
				long offsetToCommit = lastProcessedOffset + 1;

				offsetsToCommit.put(partition.getKafkaPartitionHandle(), new OffsetAndMetadata(offsetToCommit));
				partition.setCommittedOffset(offsetToCommit);
			}
		}

		// record the work to be committed by the main consumer thread and make sure the consumer notices that
		consumerThread.setOffsetsToCommit(offsetsToCommit, commitCallback);
	}
}
