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
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaCommitCallback;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.SerializedValue;

import javafx.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A fetcher that fetches data from Kafka brokers via the Kafka consumer API.
 *
 * @param <T> The type of elements produced by the fetcher.
 */
@Internal
public class KafkaFetcher<T> extends AbstractFetcher<T, TopicPartition> {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaFetcher.class);

	// ------------------------------------------------------------------------

	/** The schema to convert between Kafka's byte messages, and Flink's objects. */
	private final KafkaDeserializationSchema<T> deserializer;

	/** The handover of data and exceptions between the consumer thread and the task thread. */
	private final Handover handover;

	/** The thread that runs the actual KafkaConsumer and hand the record batches to this fetcher. */
	private final KafkaConsumerThread consumerThread;

	/** Global aggregate manager will help us maintain a global timestamp for event-time-alignment. */
	private final GlobalAggregateManager globalAggregateManager;

	private final int subtaskIndex;

	/** The thread that runs the event time alignment logic. */
	private Thread eventTimeAlignmentThread = null;

	/** The handover for the Fetcher to pass partitions that needs to be paused in KafkaConsumer for event-time alignment. */
	private EventTimeAlignmentHandover eventTimeAlignmentHandover = null;

	/** Maps each partition to it's latest timestamp value, this will be used for event-time alignment. */
	private Map<TopicPartition, Long> partitionTimestampMap = null;

	/** Aggregator for global timestamp tracking. */
	private AlignmentTimestampAggregateFunction alignmentTsAggregator;

	/** Flag to mark the main work loop as alive. */
	private volatile boolean running = true;

	// ------------------------------------------------------------------------

	public KafkaFetcher(
		SourceFunction.SourceContext<T> sourceContext,
		Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
		SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
		SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
		ProcessingTimeService processingTimeProvider,
		long autoWatermarkInterval,
		ClassLoader userCodeClassLoader,
		String taskNameWithSubtasks,
		KafkaDeserializationSchema<T> deserializer,
		Properties kafkaProperties,
		long pollTimeout,
		MetricGroup subtaskMetricGroup,
		MetricGroup consumerMetricGroup,
		boolean useMetrics,
		GlobalAggregateManager globalAggregateManager,
		int subtaskIndex,
		long eventTimeAlignmentIntervalMillis,
		long eventTimeAlignmentThresholdMillis) throws Exception {
		super(
			sourceContext,
			assignedPartitionsWithInitialOffsets,
			watermarksPeriodic,
			watermarksPunctuated,
			processingTimeProvider,
			autoWatermarkInterval,
			userCodeClassLoader,
			consumerMetricGroup,
			useMetrics);

		this.deserializer = deserializer;
		this.handover = new Handover();

		this.globalAggregateManager = globalAggregateManager;
		this.subtaskIndex = subtaskIndex;

		// Initialize event timer aligner
		// current support is only for PERIODIC WATERMARKS
		if (watermarksPeriodic != null && eventTimeAlignmentIntervalMillis >= 0) {
			this.eventTimeAlignmentHandover = new EventTimeAlignmentHandover();
			this.eventTimeAlignmentThread = initializeEventTimeAligner(
				eventTimeAlignmentIntervalMillis, eventTimeAlignmentThresholdMillis, subtaskIndex);
			this.partitionTimestampMap = new ConcurrentHashMap<>();
			this.alignmentTsAggregator = new AlignmentTimestampAggregateFunction();
		}

		this.consumerThread = new KafkaConsumerThread(
			LOG,
			handover,
			kafkaProperties,
			unassignedPartitionsQueue,
			eventTimeAlignmentHandover,
			getFetcherName() + " for " + taskNameWithSubtasks,
			pollTimeout,
			useMetrics,
			consumerMetricGroup,
			subtaskMetricGroup);
	}

	private Thread initializeEventTimeAligner(long eventTimeAlignmentIntervalMillis,
												long eventTimeAlignmentThresholdMillis,
												int subtaskIndex) {

		return new Thread(() -> {

			LOG.info("Initializing event time aligner with interval={} threshold={}",
				eventTimeAlignmentIntervalMillis, eventTimeAlignmentThresholdMillis);

			while (running) {

				try {
					Thread.sleep(eventTimeAlignmentIntervalMillis);
				} catch (InterruptedException e) {
					LOG.warn("InterruptedException on event time aligner");
					break;
				}

				try {
					if (!this.partitionTimestampMap.isEmpty()) {

						// Min of timestamp from all partitions in this task
						long taskMin = this.partitionTimestampMap.values().stream()
								.reduce(Long.MAX_VALUE, (a, b) -> Math.min(a, b));

						// Global minimum for the job
						Long globalMin = this.globalAggregateManager.updateGlobalAggregate("event-time-alignment-ts",
							new Pair<>(this.subtaskIndex, taskMin), this.alignmentTsAggregator);

						LOG.debug("Wake-up event time alignment subTask={}, taskMin={}, globalMin={}",
							this.subtaskIndex, taskMin, globalMin);

						// Identify the partitions which are mis-aligned,
						// ie. have a timestamp > globalMin + threshold
						List<TopicPartition> partitionsToPause = new LinkedList<>();
						this.partitionTimestampMap.forEach((partition, timestamp) -> {
							if (timestamp > globalMin + eventTimeAlignmentThresholdMillis) {
								partitionsToPause.add(partition);
							}
						});

						if (partitionsToPause.size() > 0) {
							this.eventTimeAlignmentHandover.activate(partitionsToPause);
						} else {
							this.eventTimeAlignmentHandover.deactivate();
						}
					}
				} catch (IOException e) {
					LOG.error("GlobalAggregateManager exception", e);
				}
			}
		});
	}

	protected void onEmitWithPeriodicWatermark (
			KafkaTopicPartitionState<TopicPartition> partitionState, long timestamp) {
		// Update the map with the latest partition timestamp
		if (this.partitionTimestampMap != null) {
			this.partitionTimestampMap.put(partitionState.getKafkaPartitionHandle(), timestamp);
		}
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

			// kick off the event time aligner
			if (eventTimeAlignmentThread != null) {
				eventTimeAlignmentThread.start();
			}

			while (running) {
				// this blocks until we get the next records
				// it automatically re-throws exceptions encountered in the consumer thread
				final ConsumerRecords<byte[], byte[]> records = handover.pollNext();

				// get the records for each topic partition
				for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitionStates()) {

					List<ConsumerRecord<byte[], byte[]>> partitionRecords =
						records.records(partition.getKafkaPartitionHandle());

					for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
						final T value = deserializer.deserialize(record);

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
		if (eventTimeAlignmentThread != null) {
			eventTimeAlignmentThread.interrupt();
		}
	}

	protected void emitRecord(
		T record,
		KafkaTopicPartitionState<TopicPartition> partition,
		long offset,
		ConsumerRecord<?, ?> consumerRecord) throws Exception {

		emitRecordWithTimestamp(record, partition, offset, consumerRecord.timestamp());
	}

	/**
	 * Gets the name of this fetcher, for thread naming and logging purposes.
	 */
	protected String getFetcherName() {
		return "Kafka Fetcher";
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
