/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The thread the runs the {@link KafkaConsumer}, connecting to the brokers and polling records.
 * The thread pushes the data into a {@link Handover} to be picked up by the fetcher that will
 * deserialize and emit the records.
 * 
 * <p><b>IMPORTANT:</b> This thread must not be interrupted when attempting to shut it down.
 * The Kafka consumer code was found to not always handle interrupts well, and to even
 * deadlock in certain situations.
 * 
 * <p>Implementation Note: This code is written to be reusable in later versions of the KafkaConsumer.
 * Because Kafka is not maintaining binary compatibility, we use a "call bridge" as an indirection
 * to the KafkaConsumer calls that change signature.
 */
public class KafkaConsumerThread extends Thread {

	/** Logger for this consumer */
	private final Logger log;

	/** The handover of data and exceptions between the consumer thread and the task thread */
	private final Handover handover;

	/** The next offsets that the main thread should commit */
	private final AtomicReference<Map<TopicPartition, OffsetAndMetadata>> nextOffsetsToCommit;

	/** The configuration for the Kafka consumer */
	private final Properties kafkaProperties;

	/** The partitions that this consumer reads from */ 
	private final KafkaTopicPartitionState<TopicPartition>[] subscribedPartitions;

	/** We get this from the outside to publish metrics. **/
	private final MetricGroup kafkaMetricGroup;

	/** The indirections on KafkaConsumer methods, for cases where KafkaConsumer compatibility is broken */
	private final KafkaConsumerCallBridge consumerCallBridge;

	/** The maximum number of milliseconds to wait for a fetch batch */
	private final long pollTimeout;

	/** Flag whether to add Kafka's metrics to the Flink metrics */
	private final boolean useMetrics;

	/** Reference to the Kafka consumer, once it is created */
	private volatile KafkaConsumer<byte[], byte[]> consumer;

	/** Flag to mark the main work loop as alive */
	private volatile boolean running;

	/** Flag tracking whether the latest commit request has completed */
	private volatile boolean commitInProgress;


	public KafkaConsumerThread(
			Logger log,
			Handover handover,
			Properties kafkaProperties,
			KafkaTopicPartitionState<TopicPartition>[] subscribedPartitions,
			MetricGroup kafkaMetricGroup,
			KafkaConsumerCallBridge consumerCallBridge,
			String threadName,
			long pollTimeout,
			boolean useMetrics) {

		super(threadName);
		setDaemon(true);

		this.log = checkNotNull(log);
		this.handover = checkNotNull(handover);
		this.kafkaProperties = checkNotNull(kafkaProperties);
		this.subscribedPartitions = checkNotNull(subscribedPartitions);
		this.kafkaMetricGroup = checkNotNull(kafkaMetricGroup);
		this.consumerCallBridge = checkNotNull(consumerCallBridge);
		this.pollTimeout = pollTimeout;
		this.useMetrics = useMetrics;

		this.nextOffsetsToCommit = new AtomicReference<>();
		this.running = true;
	}

	// ------------------------------------------------------------------------

	@Override
	public void run() {
		// early exit check
		if (!running) {
			return;
		}

		// this is the means to talk to FlinkKafkaConsumer's main thread
		final Handover handover = this.handover;

		// This method initializes the KafkaConsumer and guarantees it is torn down properly.
		// This is important, because the consumer has multi-threading issues,
		// including concurrent 'close()' calls.
		final KafkaConsumer<byte[], byte[]> consumer;
		try {
			consumer = new KafkaConsumer<>(kafkaProperties);
		}
		catch (Throwable t) {
			handover.reportError(t);
			return;
		}

		// from here on, the consumer is guaranteed to be closed properly
		try {
			// The callback invoked by Kafka once an offset commit is complete
			final OffsetCommitCallback offsetCommitCallback = new CommitCallback();

			// tell the consumer which partitions to work with
			consumerCallBridge.assignPartitions(consumer, convertKafkaPartitions(subscribedPartitions));

			// register Kafka's very own metrics in Flink's metric reporters
			if (useMetrics) {
				// register Kafka metrics to Flink
				Map<MetricName, ? extends Metric> metrics = consumer.metrics();
				if (metrics == null) {
					// MapR's Kafka implementation returns null here.
					log.info("Consumer implementation does not support metrics");
				} else {
					// we have Kafka metrics, register them
					for (Map.Entry<MetricName, ? extends Metric> metric: metrics.entrySet()) {
						kafkaMetricGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));
					}
				}
			}

			// early exit check
			if (!running) {
				return;
			}

			// seek the consumer to the initial offsets
			for (KafkaTopicPartitionState<TopicPartition> partition : subscribedPartitions) {
				if (partition.isOffsetDefined()) {
					log.info("Partition {} has restored initial offsets {} from checkpoint / savepoint; " +
							"seeking the consumer to position {}",
							partition.getKafkaPartitionHandle(), partition.getOffset(), partition.getOffset() + 1);

					consumer.seek(partition.getKafkaPartitionHandle(), partition.getOffset() + 1);
				}
				else {
					// for partitions that do not have offsets restored from a checkpoint/savepoint,
					// we need to define our internal offset state for them using the initial offsets retrieved from Kafka
					// by the KafkaConsumer, so that they are correctly checkpointed and committed on the next checkpoint

					long fetchedOffset = consumer.position(partition.getKafkaPartitionHandle());

					log.info("Partition {} has no initial offset; the consumer has position {}, " +
							"so the initial offset will be set to {}",
							partition.getKafkaPartitionHandle(), fetchedOffset, fetchedOffset - 1);

					// the fetched offset represents the next record to process, so we need to subtract it by 1
					partition.setOffset(fetchedOffset - 1);
				}
			}

			// from now on, external operations may call the consumer
			this.consumer = consumer;

			// the latest bulk of records. may carry across the loop if the thread is woken up
			// from blocking on the handover
			ConsumerRecords<byte[], byte[]> records = null;

			// main fetch loop
			while (running) {

				// check if there is something to commit
				if (!commitInProgress) {
					// get and reset the work-to-be committed, so we don't repeatedly commit the same
					final Map<TopicPartition, OffsetAndMetadata> toCommit = nextOffsetsToCommit.getAndSet(null);

					if (toCommit != null) {
						log.debug("Sending async offset commit request to Kafka broker");

						// also record that a commit is already in progress
						// the order here matters! first set the flag, then send the commit command.
						commitInProgress = true;
						consumer.commitAsync(toCommit, offsetCommitCallback);
					}
				}

				// get the next batch of records, unless we did not manage to hand the old batch over
				if (records == null) {
					try {
						records = consumer.poll(pollTimeout);
					}
					catch (WakeupException we) {
						continue;
					}
				}

				try {
					handover.produce(records);
					records = null;
				}
				catch (Handover.WakeupException e) {
					// fall through the loop
				}
			}
			// end main fetch loop
		}
		catch (Throwable t) {
			// let the main thread know and exit
			// it may be that this exception comes because the main thread closed the handover, in
			// which case the below reporting is irrelevant, but does not hurt either
			handover.reportError(t);
		}
		finally {
			// make sure the handover is closed if it is not already closed or has an error
			handover.close();

			// make sure the KafkaConsumer is closed
			try {
				consumer.close();
			}
			catch (Throwable t) {
				log.warn("Error while closing Kafka consumer", t);
			}
		}
	}

	/**
	 * Shuts this thread down, waking up the thread gracefully if blocked (without Thread.interrupt() calls).
	 */
	public void shutdown() {
		running = false;

		// We cannot call close() on the KafkaConsumer, because it will actually throw
		// an exception if a concurrent call is in progress

		// this wakes up the consumer if it is blocked handing over records
		handover.wakeupProducer();

		// this wakes up the consumer if it is blocked in a kafka poll 
		if (consumer != null) {
			consumer.wakeup();
		}
	}

	/**
	 * Tells this thread to commit a set of offsets. This method does not block, the committing
	 * operation will happen asynchronously.
	 * 
	 * <p>Only one commit operation may be pending at any time. If the committing takes longer than
	 * the frequency with which this method is called, then some commits may be skipped due to being
	 * superseded  by newer ones.
	 * 
	 * @param offsetsToCommit The offsets to commit
	 */
	public void setOffsetsToCommit(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
		// record the work to be committed by the main consumer thread and make sure the consumer notices that
		if (nextOffsetsToCommit.getAndSet(offsetsToCommit) != null) {
			log.warn("Committing offsets to Kafka takes longer than the checkpoint interval. " +
					"Skipping commit of previous offsets because newer complete checkpoint offsets are available. " +
					"This does not compromise Flink's checkpoint integrity.");
		}

		// if the consumer is blocked in a poll() or handover operation, wake it up to commit soon
		handover.wakeupProducer();
		if (consumer != null) {
			consumer.wakeup();
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static List<TopicPartition> convertKafkaPartitions(KafkaTopicPartitionState<TopicPartition>[] partitions) {
		ArrayList<TopicPartition> result = new ArrayList<>(partitions.length);
		for (KafkaTopicPartitionState<TopicPartition> p : partitions) {
			result.add(p.getKafkaPartitionHandle());
		}
		return result;
	}

	// ------------------------------------------------------------------------

	private class CommitCallback implements OffsetCommitCallback {

		@Override
		public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception ex) {
			commitInProgress = false;

			if (ex != null) {
				log.warn("Committing offsets to Kafka failed. This does not compromise Flink's checkpoints.", ex);
			}
		}
	}
}
